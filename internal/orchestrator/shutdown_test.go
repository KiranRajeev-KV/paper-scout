package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/paper-scout/internal/worker"
)

func TestShutdownCancelsActivePipeline(t *testing.T) {
	appCtx, appCancel := context.WithCancel(context.Background())
	o := &Orchestrator{
		appCtx:     appCtx,
		appCancel:  appCancel,
		workerPool: worker.NewPool(1, 1),
	}

	stopped := make(chan struct{})
	o.runFn = func(ctx context.Context, _ *Pipeline) {
		<-ctx.Done()
		close(stopped)
	}

	o.launchPipeline(&Pipeline{TopicID: "topic-1"})

	shutdownDone := make(chan struct{})
	go func() {
		o.Shutdown()
		close(shutdownDone)
	}()

	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("pipeline did not observe shutdown cancellation")
	}

	select {
	case <-shutdownDone:
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown did not wait for active pipeline")
	}
}
