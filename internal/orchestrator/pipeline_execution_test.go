package orchestrator

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/paper-scout/internal/agent"
	"github.com/paper-scout/internal/config"
)

func TestPipelineExecutesSevenStagesInOrder(t *testing.T) {
	var executed []Stage
	var mu sync.Mutex
	o := newStageTestOrchestrator(func(stage Stage) { mu.Lock(); executed = append(executed, stage); mu.Unlock() })
	pipeline := &Pipeline{TopicID: "topic-1", RunID: "run-1", Topic: "test topic", StartedAt: time.Now()}

	o.runPipelineWithContext(context.Background(), pipeline)

	want := []Stage{StageQueryExpand, StageDiscovery, StageRanking, StageAnalysis, StageGapDetection, StageFeasibility, StageReport}
	if !reflect.DeepEqual(executed, want) {
		t.Fatalf("executed stages = %v, want %v", executed, want)
	}
	if pipeline.Stage != StageCompleted || pipeline.Status != "completed" {
		t.Fatalf("pipeline terminal state = %s/%s, want completed", pipeline.Status, pipeline.Stage)
	}
}

func TestRecoveredPipelineResumesFromCheckpoint(t *testing.T) {
	var executed []Stage
	var mu sync.Mutex
	o := newStageTestOrchestrator(func(stage Stage) { mu.Lock(); executed = append(executed, stage); mu.Unlock() })
	completed := map[Stage]bool{StageQueryExpand: true, StageDiscovery: true, StageRanking: true}
	o.stageCompletedFn = func(_ context.Context, _ *Pipeline, stage Stage, output interface{}) (bool, error) {
		if !completed[stage] {
			return false, nil
		}
		switch value := output.(type) {
		case **agent.ExpandedQuery:
			*value = &agent.ExpandedQuery{Queries: []string{"test topic"}}
		case *[]agent.RankedPaper:
			*value = []agent.RankedPaper{{ID: "paper-1", Title: "Paper"}}
		}
		return true, nil
	}

	o.runPipelineWithContext(context.Background(), &Pipeline{TopicID: "topic-1", RunID: "run-1", Topic: "test topic", StartedAt: time.Now()})

	want := []Stage{StageAnalysis, StageGapDetection, StageFeasibility, StageReport}
	if !reflect.DeepEqual(executed, want) {
		t.Fatalf("recovered pipeline executed %v, want %v", executed, want)
	}
}

func newStageTestOrchestrator(record func(Stage)) *Orchestrator {
	o := &Orchestrator{
		appCtx:  context.Background(),
		config:  &config.Config{Pipeline: config.PipelineConfig{MaxPapers: 10, MinPapersForAnalysis: 1}},
		sse:     NewSSEManager(),
		reports: make(map[string]*agent.Report),
	}
	o.stageCompletedFn = func(context.Context, *Pipeline, Stage, interface{}) (bool, error) { return false, nil }
	o.startStageFn = func(context.Context, *Pipeline, Stage) error { return nil }
	o.completeStageFn = func(context.Context, *Pipeline, Stage, interface{}) error { return nil }
	o.persistTerminalStateFn = func(context.Context, *Pipeline) error { return nil }
	o.expandFn = func(context.Context, string, string) (*agent.ExpandedQuery, error) {
		record(StageQueryExpand)
		return &agent.ExpandedQuery{Queries: []string{"test topic"}}, nil
	}
	o.discoverFn = func(context.Context, string, string, *agent.ExpandedQuery) ([]agent.DiscoveredPaper, error) {
		record(StageDiscovery)
		return []agent.DiscoveredPaper{{ID: "paper-1", Title: "Paper"}}, nil
	}
	o.countPapersFn = func(context.Context, string) (int64, error) { return 1, nil }
	o.rankFn = func(context.Context, string, string, int) ([]agent.RankedPaper, error) {
		record(StageRanking)
		return []agent.RankedPaper{{ID: "paper-1", Title: "Paper"}}, nil
	}
	o.pendingRankedPapersFn = func(_ context.Context, _ string, papers []agent.RankedPaper) ([]agent.RankedPaper, error) {
		return papers, nil
	}
	o.analyzePapersFn = func(context.Context, string, []agent.RankedPaper) error { record(StageAnalysis); return nil }
	o.detectFn = func(context.Context, string, string) ([]agent.ResearchGap, error) {
		record(StageGapDetection)
		return []agent.ResearchGap{{Title: "Gap"}}, nil
	}
	o.evaluateFn = func(context.Context, string, []agent.ResearchGap) ([]agent.FeasibilityResult, error) {
		record(StageFeasibility)
		return nil, nil
	}
	o.generateReportFn = func(context.Context, string) (*agent.Report, error) {
		record(StageReport)
		return &agent.Report{}, nil
	}
	return o
}
