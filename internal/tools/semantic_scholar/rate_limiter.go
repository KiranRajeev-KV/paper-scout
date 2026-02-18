package semantic_scholar

import (
	"context"
	"sync"
	"time"
)

type RateLimiter struct {
	tokens     float64
	maxTokens  float64
	refillRate float64
	mu         sync.Mutex
}

func NewRateLimiter(requestsPerSecond float64, burst int) *RateLimiter {
	return &RateLimiter{
		tokens:     float64(burst),
		maxTokens:  float64(burst),
		refillRate: requestsPerSecond,
	}
}

func (r *RateLimiter) Wait(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.tokens >= 1 {
		r.tokens--
		return nil
	}

	waitTime := time.Duration((1-r.tokens)/r.refillRate) * time.Second
	if waitTime > 0 {
		r.mu.Unlock()
		select {
		case <-ctx.Done():
			r.mu.Lock()
			return ctx.Err()
		case <-time.After(waitTime):
			r.mu.Lock()
		}
	}

	r.tokens--
	if r.tokens < 0 {
		r.tokens = 0
	}
	return nil
}

func (r *RateLimiter) refill() {
	r.tokens += r.refillRate / 100
	if r.tokens > r.maxTokens {
		r.tokens = r.maxTokens
	}
}
