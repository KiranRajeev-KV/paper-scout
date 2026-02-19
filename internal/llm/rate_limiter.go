package llm

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type LLMRateLimiter struct {
	rpm         int
	rpd         int
	minuteMu    sync.Mutex
	dayMu       sync.Mutex
	minuteCount int
	dayCount    int
	minuteStart time.Time
	dayStart    time.Time
}

func NewLLMRateLimiter(rpm, rpd int) *LLMRateLimiter {
	now := time.Now()
	return &LLMRateLimiter{
		rpm:         rpm,
		rpd:         rpd,
		minuteStart: now,
		dayStart:    now,
	}
}

func (r *LLMRateLimiter) Wait(ctx context.Context) error {
	if err := r.checkMinuteLimit(ctx); err != nil {
		return err
	}
	if err := r.checkDayLimit(ctx); err != nil {
		return err
	}
	return nil
}

func (r *LLMRateLimiter) checkMinuteLimit(ctx context.Context) error {
	r.minuteMu.Lock()
	defer r.minuteMu.Unlock()

	now := time.Now()
	if now.Sub(r.minuteStart) >= time.Minute {
		r.minuteCount = 0
		r.minuteStart = now
	}

	if r.minuteCount >= r.rpm {
		waitTime := time.Minute - now.Sub(r.minuteStart)
		if waitTime > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(waitTime):
			}
			r.minuteCount = 0
			r.minuteStart = time.Now()
		}
	}

	r.minuteCount++
	return nil
}

func (r *LLMRateLimiter) checkDayLimit(ctx context.Context) error {
	r.dayMu.Lock()
	defer r.dayMu.Unlock()

	now := time.Now()
	if now.Sub(r.dayStart) >= 24*time.Hour {
		r.dayCount = 0
		r.dayStart = now
	}

	if r.dayCount >= r.rpd {
		return fmt.Errorf("daily request limit exceeded (%d/%d), skipping", r.dayCount, r.rpd)
	}

	r.dayCount++
	return nil
}

func (r *LLMRateLimiter) ResetMinute() {
	r.minuteMu.Lock()
	defer r.minuteMu.Unlock()
	r.minuteCount = 0
	r.minuteStart = time.Now()
}

func (r *LLMRateLimiter) GetStats() (minuteCount, dayCount int) {
	r.minuteMu.Lock()
	minuteCount = r.minuteCount
	r.minuteMu.Unlock()

	r.dayMu.Lock()
	dayCount = r.dayCount
	r.dayMu.Unlock()

	return
}
