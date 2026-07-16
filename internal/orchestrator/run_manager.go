package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/paper-scout/internal/logger"
	"github.com/paper-scout/internal/worker"
)

// RunManager owns pipeline goroutine lifetimes, cancellation, and worker shutdown.
type RunManager struct {
	appCtx  context.Context
	cancel  context.CancelFunc
	runner  pipelineExecutor
	logs    *logger.Manager
	workers *worker.Pool
	runs    sync.WaitGroup
}

type pipelineExecutor interface {
	Run(context.Context, *Pipeline)
}

// NewRunManager constructs a lifecycle manager for pipelines and their shared workers.
func NewRunManager(appCtx context.Context, cancel context.CancelFunc, runner pipelineExecutor, logs *logger.Manager, workers *worker.Pool) (*RunManager, error) {
	if appCtx == nil || cancel == nil || runner == nil || logs == nil || workers == nil {
		return nil, fmt.Errorf("run manager requires application context, runner, logs, and worker pool")
	}
	return &RunManager{appCtx: appCtx, cancel: cancel, runner: runner, logs: logs, workers: workers}, nil
}

// Launch starts one tracked pipeline goroutine.
func (m *RunManager) Launch(pipeline *Pipeline) {
	ctx, cancel := context.WithCancel(m.appCtx)
	ctx = m.logs.ContextForTopic(ctx, pipeline.TopicID)
	m.runs.Add(1)
	go func() {
		defer m.runs.Done()
		defer cancel()
		m.runner.Run(ctx, pipeline)
	}()
}

// Shutdown cancels pipelines, waits for them, then stops shared workers.
func (m *RunManager) Shutdown() {
	const timeout = 30 * time.Second
	deadline := time.Now().Add(timeout)
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(m.appCtx), timeout)
	defer cancel()
	m.cancel()
	done := make(chan struct{})
	go func() { m.runs.Wait(); close(done) }()
	select {
	case <-done:
	case <-shutdownCtx.Done():
		logger.From(m.appCtx).Warn().Msg("Timed out waiting for pipelines to stop")
	}
	remaining := time.Until(deadline)
	if remaining < 0 {
		remaining = 0
	}
	m.workers.StopAndWait(remaining)
}
