package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/paper-scout/internal/logger"
	redispkg "github.com/paper-scout/internal/storage/redis"
	"github.com/paper-scout/internal/worker"
)

type shutdownTestQueue struct{}

func (*shutdownTestQueue) EnsureGroup(context.Context) error           { return nil }
func (*shutdownTestQueue) Enqueue(context.Context, redispkg.Job) error { return nil }
func (*shutdownTestQueue) Dequeue(ctx context.Context, _ string, _ time.Duration) (*redispkg.Job, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (*shutdownTestQueue) Complete(context.Context, string) error { return nil }
func (*shutdownTestQueue) Fail(context.Context, redispkg.Job, string) (redispkg.FailResult, error) {
	return redispkg.FailResult{Terminal: true}, nil
}
func (*shutdownTestQueue) QueueDepth(context.Context) (int64, error) { return 0, nil }

type blockingRunner struct{ stopped chan struct{} }

func (r blockingRunner) Run(ctx context.Context, _ *Pipeline) { <-ctx.Done(); close(r.stopped) }

// Protects shutdown cancels active pipeline.
func TestShutdownCancelsActivePipeline(t *testing.T) {
	stopped := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	pool, err := worker.NewRedisPool(ctx, 1, &shutdownTestQueue{})
	if err != nil {
		t.Fatalf("NewRedisPool() error = %v", err)
	}
	manager, err := NewRunManager(ctx, cancel, blockingRunner{stopped: stopped}, &logger.Manager{}, pool)
	if err != nil {
		t.Fatalf("NewRunManager returned error: %v", err)
	}
	manager.Launch(&Pipeline{TopicID: "topic-1"})
	done := make(chan struct{})
	go func() { manager.Shutdown(); close(done) }()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("pipeline did not observe shutdown cancellation")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not wait for active pipeline")
	}
}

// Protects the shutdown Redis snapshot from inheriting pipeline cancellation.
func TestShutdownStateContextSurvivesPipelineCancellation(t *testing.T) {
	ctx, cancelPipeline := context.WithCancel(context.Background())
	cancelPipeline()
	stateCtx, cancelStateSave := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer cancelStateSave()
	if err := stateCtx.Err(); err != nil {
		t.Fatalf("state save context canceled: %v", err)
	}
}
