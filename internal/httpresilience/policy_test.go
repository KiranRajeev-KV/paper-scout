package httpresilience

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

type recordingObserver struct {
	mu     sync.Mutex
	events []Event
}

func (o *recordingObserver) Observe(event Event) {
	o.mu.Lock()
	o.events = append(o.events, event)
	o.mu.Unlock()
}

func (o *recordingObserver) count(kind string) int {
	o.mu.Lock()
	defer o.mu.Unlock()
	count := 0
	for _, event := range o.events {
		if event.Kind == kind {
			count++
		}
	}
	return count
}

func testPolicy(observer Observer) *Policy {
	return New("test", Config{
		MaxRetries:       2,
		BaseBackoff:      time.Millisecond,
		MaxBackoff:       2 * time.Millisecond,
		FailureThreshold: 10,
		OpenTimeout:      time.Second,
	}, 0, 0, observer)
}

func TestPolicyRetriesTransientHTTPStatus(t *testing.T) {
	attempts := 0
	observer := &recordingObserver{}
	policy := testPolicy(observer)
	resp, err := policy.Do(context.Background(), "test", func(ctx context.Context) (*http.Response, error) {
		attempts++
		status := http.StatusOK
		if attempts < 3 {
			status = http.StatusServiceUnavailable
		}
		return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader("response"))}, nil
	})
	if err != nil {
		t.Fatalf("policy returned error: %v", err)
	}
	defer resp.Body.Close()
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
	if observer.count("retry") != 2 {
		t.Fatalf("retry events = %d, want 2", observer.count("retry"))
	}
}

func TestPolicyDoesNotRetryPermanentHTTPStatus(t *testing.T) {
	attempts := 0
	resp, err := testPolicy(nil).Do(context.Background(), "test", func(ctx context.Context) (*http.Response, error) {
		attempts++
		return &http.Response{StatusCode: http.StatusBadRequest, Body: io.NopCloser(strings.NewReader("bad request"))}, nil
	})
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
	var statusErr *HTTPError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusBadRequest {
		t.Fatalf("error = %v, want HTTPError 400", err)
	}
	if resp == nil {
		t.Fatal("policy dropped final response")
	}
	resp.Body.Close()
}

func TestRetryAfterParsesSecondsAndHTTPDate(t *testing.T) {
	seconds := &http.Response{Header: http.Header{"Retry-After": []string{"2"}}}
	if got := retryAfter(seconds); got != 2*time.Second {
		t.Fatalf("seconds retry-after = %s, want 2s", got)
	}
	future := time.Now().Add(2 * time.Second).UTC().Format(http.TimeFormat)
	date := &http.Response{Header: http.Header{"Retry-After": []string{future}}}
	if got := retryAfter(date); got <= 0 {
		t.Fatalf("date retry-after = %s, want positive duration", got)
	}
}

func TestPolicyCancellationStopsBackoff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	policy := New("test", Config{MaxRetries: 2, BaseBackoff: time.Second, MaxBackoff: time.Second}, 0, 0, nil)
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		close(started)
		_, _ = policy.Do(ctx, "test", func(context.Context) (*http.Response, error) {
			return nil, errors.New("temporary failure")
		})
	}()
	<-started
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case <-ctx.Done():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("context did not cancel")
	}
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("policy did not stop after context cancellation")
	}
}
