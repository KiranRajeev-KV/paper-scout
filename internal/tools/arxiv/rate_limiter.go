package arxiv

import (
	"context"
	"sync"
	"time"
)

type RateLimiter struct {
	tokens     float64
	maxTokens  float64
	refillRate float64
	lastRefill time.Time
	mu         sync.Mutex
}

func NewRateLimiter(requestsPerSecond float64, burst int) *RateLimiter {
	return &RateLimiter{
		tokens:     float64(burst),
		maxTokens:  float64(burst),
		refillRate: requestsPerSecond,
		lastRefill: time.Now(),
	}
}

func (r *RateLimiter) Wait(ctx context.Context) error {
	r.mu.Lock()
	r.refill()

	if r.tokens >= 1 {
		r.tokens--
		r.mu.Unlock()
		return nil
	}

	waitTime := time.Duration((1-r.tokens)/r.refillRate) * time.Second
	r.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(waitTime):
	}

	r.mu.Lock()
	r.refill()
	if r.tokens >= 1 {
		r.tokens--
	}
	r.mu.Unlock()

	return nil
}

func (r *RateLimiter) refill() {
	now := time.Now()
	elapsed := now.Sub(r.lastRefill).Seconds()
	r.lastRefill = now

	r.tokens += elapsed * r.refillRate
	if r.tokens > r.maxTokens {
		r.tokens = r.maxTokens
	}
}
