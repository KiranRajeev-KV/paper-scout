package llm

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"time"

	"github.com/paper-scout/internal/logger"
	"google.golang.org/genai"
)

type RetryPolicy struct {
	maxRetries  int
	baseBackoff time.Duration
	maxBackoff  time.Duration
}

func NewRetryPolicy(maxRetries int, baseBackoff, maxBackoff time.Duration) *RetryPolicy {
	return &RetryPolicy{
		maxRetries:  maxRetries,
		baseBackoff: baseBackoff,
		maxBackoff:  maxBackoff,
	}
}

func (r *RetryPolicy) Execute(ctx context.Context, fn func(attempt int) error) error {
	var lastErr error

	for attempt := 0; attempt <= r.maxRetries; attempt++ {
		err := fn(attempt + 1)
		if err == nil {
			return nil
		}

		lastErr = err

		if !isRetryable(err) {
			return fmt.Errorf("non-retryable error: %w", err)
		}

		if attempt == r.maxRetries {
			break
		}

		backoff := r.calculateBackoff(attempt)
		logger.From(ctx).Warn().
			Err(err).
			Int("attempt", attempt+1).
			Int("max_retries", r.maxRetries).
			Dur("backoff", backoff).
			Msg("LLM call failed, retrying")

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
	}

	return fmt.Errorf("max retries exceeded: %w", lastErr)
}

func (r *RetryPolicy) calculateBackoff(attempt int) time.Duration {
	backoff := r.baseBackoff * time.Duration(1<<uint(attempt))
	if backoff > r.maxBackoff {
		backoff = r.maxBackoff
	}
	if backoff <= 0 {
		return 0
	}
	jitterRange := backoff / 2
	var jitter time.Duration
	if jitterRange > 0 {
		jitter = time.Duration(rand.Int63n(int64(jitterRange)))
	}
	return backoff + jitter
}

func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var apiErr genai.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.Code {
		case 408, 429, 500, 502, 503, 504:
			return true
		default:
			return false
		}
	}
	var networkErr net.Error
	return errors.As(err, &networkErr) && (networkErr.Timeout() || networkErr.Temporary())
}
