package httpresilience

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/paper-scout/internal/circuitbreaker"
	"github.com/paper-scout/internal/logger"
	"github.com/rs/zerolog"
)

type Config struct {
	MaxRetries       int
	BaseBackoff      time.Duration
	MaxBackoff       time.Duration
	FailureThreshold int
	OpenTimeout      time.Duration
}

type Event struct {
	Operation string
	Kind      string
	Attempt   int
	Status    int
	Duration  time.Duration
	Err       error
}

type Observer interface {
	Observe(Event)
}

// Policy applies retry, throttling, and circuit-breaking to outbound HTTP requests.
type Policy struct {
	config      Config
	limiter     *TokenBucket
	breaker     *circuitbreaker.CircuitBreaker
	observer    Observer
	serviceName string
	log         zerolog.Logger
}

// New constructs a request policy with lifecycle logs owned by ctx.
func New(ctx context.Context, serviceName string, cfg Config, requestsPerSecond float64, burst int, observer Observer) *Policy {
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = 0
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = 500 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 5 * time.Second
	}
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.OpenTimeout <= 0 {
		cfg.OpenTimeout = 30 * time.Second
	}
	policy := &Policy{
		config:      cfg,
		limiter:     NewTokenBucket(requestsPerSecond, burst),
		serviceName: serviceName,
		observer:    observer,
		log:         *logger.From(ctx),
	}
	policy.breaker = circuitbreaker.New(serviceName, circuitbreaker.Config{
		FailureThreshold: cfg.FailureThreshold,
		SuccessThreshold: 2,
		OpenTimeout:      cfg.OpenTimeout,
		OnStateChange: func(name string, from, to circuitbreaker.State) {
			policy.log.Warn().Str("service", name).Str("from", from.String()).Str("to", to.String()).Msg("Circuit breaker state changed")
		},
	})
	return policy
}

func (p *Policy) Do(ctx context.Context, operation string, request func(context.Context) (*http.Response, error)) (*http.Response, error) {
	var response *http.Response
	err := p.breaker.Execute(ctx, func(ctx context.Context) error {
		var err error
		for attempt := 0; attempt <= p.config.MaxRetries; attempt++ {
			waitStarted := time.Now()
			if err := p.limiter.Wait(ctx); err != nil {
				return err
			}
			if waited := time.Since(waitStarted); waited >= time.Millisecond {
				p.observe(ctx, Event{Operation: operation, Kind: "throttle", Attempt: attempt + 1, Duration: waited})
			}
			started := time.Now()
			response, err = request(ctx)
			p.observe(ctx, Event{Operation: operation, Kind: "request", Attempt: attempt + 1, Status: responseStatus(response), Duration: time.Since(started), Err: err})

			if err == nil && response != nil && response.StatusCode >= 200 && response.StatusCode < 300 {
				return nil
			}
			if !retryable(ctx, err, response) || attempt == p.config.MaxRetries {
				if err != nil {
					return err
				}
				if response == nil {
					return errors.New("HTTP request returned no response")
				}
				return &HTTPError{StatusCode: response.StatusCode}
			}

			delay := p.backoff(attempt)
			if retryAfter := retryAfter(response); retryAfter > delay {
				delay = retryAfter
			}
			if response != nil {
				_, _ = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
				response = nil
			}
			p.observe(ctx, Event{Operation: operation, Kind: "retry", Attempt: attempt + 1, Duration: delay, Err: err})
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}
		return err
	})
	if errors.Is(err, circuitbreaker.ErrCircuitOpen) {
		p.observe(ctx, Event{Operation: operation, Kind: "circuit_rejected", Err: err})
	}
	return response, err
}

func (p *Policy) backoff(attempt int) time.Duration {
	delay := p.config.BaseBackoff * time.Duration(1<<uint(attempt))
	if delay > p.config.MaxBackoff {
		delay = p.config.MaxBackoff
	}
	if delay <= 0 {
		return 0
	}
	return delay + time.Duration(rand.Int64N(int64(delay/2)+1))
}

func (p *Policy) observe(ctx context.Context, event Event) {
	if p.observer != nil {
		p.observer.Observe(event)
	}
	entry := logger.From(ctx).Debug().Str("operation", event.Operation).Str("event", event.Kind).Int("attempt", event.Attempt).Int("status", event.Status).Dur("duration", event.Duration)
	if event.Err != nil {
		entry = entry.Err(event.Err)
	}
	entry.Msg("HTTP resilience event")
	if event.Kind == "retry" {
		logger.From(ctx).Warn().Str("service", p.serviceName).Str("operation", event.Operation).Int("attempt", event.Attempt).Dur("delay", event.Duration).Err(event.Err).Msg("HTTP request retrying")
	}
}

type HTTPError struct {
	StatusCode int
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("HTTP request failed with status %d", e.StatusCode)
}

func responseStatus(response *http.Response) int {
	if response == nil {
		return 0
	}
	return response.StatusCode
}

func retryable(ctx context.Context, err error, response *http.Response) bool {
	if ctx.Err() != nil {
		return false
	}
	if response != nil {
		switch response.StatusCode {
		case http.StatusRequestTimeout, http.StatusTooEarly, http.StatusTooManyRequests,
			http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			return true
		default:
			return false
		}
	}
	if err == nil {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout() || netErr.Temporary()
	}
	var urlErr *url.Error
	return errors.As(err, &urlErr)
}

func retryAfter(response *http.Response) time.Duration {
	if response == nil {
		return 0
	}
	value := strings.TrimSpace(response.Header.Get("Retry-After"))
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if when, err := http.ParseTime(value); err == nil {
		if delay := time.Until(when); delay > 0 {
			return delay
		}
	}
	return 0
}

type TokenBucket struct {
	mu         sync.Mutex
	tokens     float64
	maxTokens  float64
	refillRate float64
	lastRefill time.Time
}

func NewTokenBucket(requestsPerSecond float64, burst int) *TokenBucket {
	if requestsPerSecond <= 0 || burst <= 0 {
		return &TokenBucket{}
	}
	return &TokenBucket{tokens: float64(burst), maxTokens: float64(burst), refillRate: requestsPerSecond, lastRefill: time.Now()}
}

func (b *TokenBucket) Wait(ctx context.Context) error {
	if b.refillRate <= 0 || b.maxTokens <= 0 {
		return nil
	}
	for {
		b.mu.Lock()
		b.refill()
		if b.tokens >= 1 {
			b.tokens--
			b.mu.Unlock()
			return nil
		}
		wait := time.Duration((1 - b.tokens) / b.refillRate * float64(time.Second))
		b.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
}

func (b *TokenBucket) refill() {
	now := time.Now()
	b.tokens += now.Sub(b.lastRefill).Seconds() * b.refillRate
	if b.tokens > b.maxTokens {
		b.tokens = b.maxTokens
	}
	b.lastRefill = now
}
