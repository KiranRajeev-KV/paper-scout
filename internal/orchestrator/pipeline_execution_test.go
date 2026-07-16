package orchestrator

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/paper-scout/internal/agent"
	"github.com/paper-scout/internal/config"
	"github.com/paper-scout/internal/logger"
)

type checkpointFake struct {
	completed  map[Stage]bool
	persistErr error
}

func (f *checkpointFake) stageCompleted(_ context.Context, _ *Pipeline, stage Stage, output interface{}) (bool, error) {
	if !f.completed[stage] {
		return false, nil
	}
	switch value := output.(type) {
	case **agent.ExpandedQuery:
		*value = &agent.ExpandedQuery{Queries: []string{"test topic"}}
	case *[]agent.RankedPaper:
		*value = []agent.RankedPaper{{ID: "paper-1", Title: "Paper"}}
	case *[]agent.ResearchGap:
		*value = []agent.ResearchGap{{Title: "Gap"}}
	}
	return true, nil
}
func (*checkpointFake) startStage(context.Context, *Pipeline, Stage) error { return nil }
func (*checkpointFake) completeStage(context.Context, *Pipeline, Stage, interface{}) error {
	return nil
}
func (*checkpointFake) failStage(context.Context, *Pipeline, Stage, error) error { return nil }
func (f *checkpointFake) persistTerminalState(context.Context, *Pipeline) error  { return f.persistErr }

type stagesFake struct {
	record     func(Stage)
	analyzeErr error
}

func (f *stagesFake) Expand(context.Context, string, string) (*agent.ExpandedQuery, error) {
	f.record(StageQueryExpand)
	return &agent.ExpandedQuery{Queries: []string{"test topic"}}, nil
}
func (f *stagesFake) Discover(_ context.Context, _ string, _ string, expanded *agent.ExpandedQuery) ([]agent.DiscoveredPaper, error) {
	if expanded == nil {
		return nil, errors.New("missing expanded query")
	}
	f.record(StageDiscovery)
	return []agent.DiscoveredPaper{{ID: "paper-1", Title: "Paper"}}, nil
}
func (*stagesFake) CountPapers(context.Context, string) (int64, error) { return 1, nil }
func (f *stagesFake) Rank(context.Context, string, string, int) ([]agent.RankedPaper, error) {
	f.record(StageRanking)
	return []agent.RankedPaper{{ID: "paper-1", Title: "Paper"}}, nil
}
func (*stagesFake) PendingPapers(_ context.Context, _ string, papers []agent.RankedPaper) ([]agent.RankedPaper, error) {
	return papers, nil
}
func (f *stagesFake) Analyze(context.Context, string, []agent.RankedPaper, func(int, int)) error {
	f.record(StageAnalysis)
	return f.analyzeErr
}
func (f *stagesFake) Detect(context.Context, string, string) ([]agent.ResearchGap, error) {
	f.record(StageGapDetection)
	return []agent.ResearchGap{{Title: "Gap"}}, nil
}
func (f *stagesFake) Evaluate(context.Context, string, []agent.ResearchGap) ([]agent.FeasibilityResult, error) {
	f.record(StageFeasibility)
	return nil, nil
}
func (f *stagesFake) GenerateReport(context.Context, string) (*agent.Report, error) {
	f.record(StageReport)
	return &agent.Report{}, nil
}

// Protects pipeline stops when analysis batch fails.
func TestPipelineStopsWhenAnalysisBatchFails(t *testing.T) {
	var executed []Stage
	runner := newStageTestRunner(func(stage Stage) { executed = append(executed, stage) }, errors.New("one paper failed"), nil, nil)
	pipeline := testPipeline()
	runner.Run(context.Background(), pipeline)
	if pipeline.Status != "failed" || pipeline.Stage != StageFailed {
		t.Fatalf("pipeline terminal state = %s/%s, want failed", pipeline.Status, pipeline.Stage)
	}
	if want := []Stage{StageQueryExpand, StageDiscovery, StageRanking, StageAnalysis}; !reflect.DeepEqual(executed, want) {
		t.Fatalf("executed stages = %v, want %v", executed, want)
	}
}

// Protects pipeline does not publish completed before durable persistence.
func TestPipelineDoesNotPublishCompletedBeforeDurablePersistence(t *testing.T) {
	runner := newStageTestRunner(func(Stage) {}, nil, nil, errors.New("database unavailable"))
	pipeline := testPipeline()
	runner.Run(context.Background(), pipeline)
	if pipeline.Status != "failed" || pipeline.Stage != StageFailed {
		t.Fatalf("pipeline terminal state = %s/%s, want failed", pipeline.Status, pipeline.Stage)
	}
}

// Protects pipeline executes seven stages in order.
func TestPipelineExecutesSevenStagesInOrder(t *testing.T) {
	var executed []Stage
	var mu sync.Mutex
	runner := newStageTestRunner(func(stage Stage) { mu.Lock(); executed = append(executed, stage); mu.Unlock() }, nil, nil, nil)
	pipeline := testPipeline()
	runner.Run(context.Background(), pipeline)
	want := []Stage{StageQueryExpand, StageDiscovery, StageRanking, StageAnalysis, StageGapDetection, StageFeasibility, StageReport}
	if !reflect.DeepEqual(executed, want) {
		t.Fatalf("executed stages = %v, want %v", executed, want)
	}
	if pipeline.Stage != StageCompleted || pipeline.Status != "completed" {
		t.Fatalf("pipeline terminal state = %s/%s, want completed", pipeline.Status, pipeline.Stage)
	}
}

// Protects recovered pipeline resumes from checkpoint.
func TestRecoveredPipelineResumesFromCheckpoint(t *testing.T) {
	var executed []Stage
	completed := map[Stage]bool{StageQueryExpand: true, StageDiscovery: true, StageRanking: true}
	runner := newStageTestRunner(func(stage Stage) { executed = append(executed, stage) }, nil, completed, nil)
	runner.Run(context.Background(), testPipeline())
	if want := []Stage{StageAnalysis, StageGapDetection, StageFeasibility, StageReport}; !reflect.DeepEqual(executed, want) {
		t.Fatalf("recovered pipeline executed %v, want %v", executed, want)
	}
}

func newStageTestRunner(record func(Stage), analyzeErr error, completed map[Stage]bool, persistErr error) *PipelineRunner {
	state := &PipelineStateService{sse: NewSSEManager(context.Background()), pipelines: make(map[string]*Pipeline)}
	return &PipelineRunner{appCtx: context.Background(), config: &config.Config{Pipeline: config.PipelineConfig{MaxPapers: 10, MinPapersForAnalysis: 1}},
		checkpoints: &checkpointFake{completed: completed, persistErr: persistErr}, stages: &stagesFake{record: record, analyzeErr: analyzeErr},
		state: state, reports: &ReportService{cache: make(map[string]*agent.Report)}, logs: &logger.Manager{}}
}

func testPipeline() *Pipeline {
	return &Pipeline{TopicID: "topic-1", RunID: "run-1", Topic: "test topic", StartedAt: time.Now()}
}
