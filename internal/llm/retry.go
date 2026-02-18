package llm

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/research-agent/internal/logger"
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

func (r *RetryPolicy) Execute(ctx context.Context, fn func() error) error {
	var lastErr error

	for attempt := 0; attempt <= r.maxRetries; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}

		lastErr = err

		if !r.isRetryable(err) {
			return fmt.Errorf("non-retryable error: %w", err)
		}

		if attempt == r.maxRetries {
			break
		}

		backoff := r.calculateBackoff(attempt)
		logger.Warn().
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
	jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
	return backoff + jitter
}

func (r *RetryPolicy) isRetryable(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()

	if contains(errStr, "429") ||
		contains(errStr, "rate limit") ||
		contains(errStr, "quota") ||
		contains(errStr, "timeout") ||
		contains(errStr, "temporary") ||
		contains(errStr, "unavailable") ||
		contains(errStr, "deadline exceeded") ||
		contains(errStr, "resource exhausted") {
		return true
	}

	return false
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
