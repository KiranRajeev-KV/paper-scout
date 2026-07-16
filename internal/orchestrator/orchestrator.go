package orchestrator

import (
	"context"
	"fmt"

	"github.com/paper-scout/internal/agent"
)

// Orchestrator is the API-facing façade for research runs and their lifecycle.
type Orchestrator struct {
	coordinator *RunCoordinator
	state       *PipelineStateService
	reports     *ReportService
	runs        *RunManager
	sse         *SSEManager
}

// NewOrchestrator joins focused run, state, report, and SSE services into the API façade.
func NewOrchestrator(coordinator *RunCoordinator, state *PipelineStateService, reports *ReportService, runs *RunManager, sse *SSEManager) (*Orchestrator, error) {
	if coordinator == nil || state == nil || reports == nil || runs == nil || sse == nil {
		return nil, fmt.Errorf("orchestrator requires coordinator, state, reports, run manager, and SSE")
	}
	return &Orchestrator{coordinator: coordinator, state: state, reports: reports, runs: runs, sse: sse}, nil
}

// StartResearch creates and launches a new research pipeline.
func (o *Orchestrator) StartResearch(ctx context.Context, topic string) (*Pipeline, error) {
	return o.coordinator.Start(ctx, topic)
}

// GetPipeline returns the current pipeline state with durable fallback.
func (o *Orchestrator) GetPipeline(ctx context.Context, topicID string) (*Pipeline, error) {
	return o.state.Get(ctx, topicID)
}

// GetReport returns a completed report, generating and caching it when necessary.
func (o *Orchestrator) GetReport(ctx context.Context, topicID string) (*agent.Report, error) {
	return o.reports.Get(ctx, topicID)
}

// GetSSEManager returns the application's SSE publisher.
func (o *Orchestrator) GetSSEManager() *SSEManager { return o.sse }

// Shutdown stops active runs and worker processing.
func (o *Orchestrator) Shutdown() { o.runs.Shutdown() }
