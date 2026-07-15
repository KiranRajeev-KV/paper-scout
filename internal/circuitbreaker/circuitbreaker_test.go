package circuitbreaker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// Protects half-open recovery from launching concurrent probe requests.
func TestHalfOpenAllowsOnlyOneProbe(t *testing.T) {
	breaker := New("service", Config{FailureThreshold: 1, SuccessThreshold: 1, OpenTimeout: time.Millisecond})
	_ = breaker.Execute(context.Background(), func(context.Context) error { return errors.New("failed") })
	time.Sleep(2 * time.Millisecond)
	entered := make(chan struct{})
	release := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = breaker.Execute(context.Background(), func(context.Context) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	err := breaker.Execute(context.Background(), func(context.Context) error { return nil })
	if !errors.Is(err, ErrHalfOpenBusy) {
		t.Fatalf("second probe error = %v, want ErrHalfOpenBusy", err)
	}
	close(release)
	wg.Wait()
}
