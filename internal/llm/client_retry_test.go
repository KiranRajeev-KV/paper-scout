package llm

import (
	"context"
	"testing"
	"time"

	"github.com/paper-scout/internal/circuitbreaker"
	"github.com/paper-scout/internal/config"
	"google.golang.org/genai"
)

type countingQuota struct{ calls int }

func (q *countingQuota) Wait(context.Context) error { q.calls++; return nil }

// Protects gemini timeout and quota apply per attempt.
func TestGeminiTimeoutAndQuotaApplyPerAttempt(t *testing.T) {
	quota := &countingQuota{}
	client := testGeminiClient(quota)
	calls := 0
	client.generateContent = func(ctx context.Context, _ string, _ []*genai.Content, _ *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
		calls++
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if _, err := client.Generate(context.Background(), "prompt"); err == nil {
		t.Fatal("Generate returned nil error after timed-out attempts")
	}
	if calls != 2 || quota.calls != 2 {
		t.Fatalf("provider/quota calls = %d/%d, want one of each per attempt", calls, quota.calls)
	}
}

// Protects gemini permanent error is not retried.
func TestGeminiPermanentErrorIsNotRetried(t *testing.T) {
	quota := &countingQuota{}
	client := testGeminiClient(quota)
	calls := 0
	client.generateContent = func(context.Context, string, []*genai.Content, *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error) {
		calls++
		return nil, genai.APIError{Code: 400, Status: "INVALID_ARGUMENT", Message: "bad schema"}
	}
	if _, err := client.Generate(context.Background(), "prompt"); err == nil {
		t.Fatal("Generate returned nil error for permanent provider failure")
	}
	if calls != 1 || quota.calls != 1 {
		t.Fatalf("provider/quota calls = %d/%d, want no retry", calls, quota.calls)
	}
}

func testGeminiClient(quota quotaWaiter) *Client {
	return &Client{
		config: config.GeminiConfig{Timeout: 5 * time.Millisecond}, model: "gemini-test",
		retry: NewRetryPolicy(1, time.Millisecond, time.Millisecond), rateLimiter: quota,
		circuitBreaker: circuitbreaker.New("test", circuitbreaker.Config{FailureThreshold: 10, SuccessThreshold: 1, OpenTimeout: time.Second}),
	}
}
