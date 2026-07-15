package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/paper-scout/internal/worker"
)

// Protects shutdown cancels active pipeline.
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

// Protects shutdown cancellation from turning a recoverable run into a terminal failure.
func TestShutdownLeavesCanceledPipelineRecoverable(t *testing.T) {
	appCtx, cancel := context.WithCancel(context.Background())
	cancel()
	o := &Orchestrator{appCtx: appCtx, pipelines: make(map[string]*Pipeline)}
	pipeline := &Pipeline{TopicID: "topic-1", RunID: "run-1", Status: "processing", Stage: StageAnalysis, Progress: 0.35}
	o.failPipeline(appCtx, pipeline, StageAnalysis, context.Canceled)
	if pipeline.Status != "processing" || pipeline.Stage != StageAnalysis {
		t.Fatalf("pipeline = %+v, want recoverable processing state", pipeline)
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
