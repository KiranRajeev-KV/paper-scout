package agent

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/paper-scout/internal/storage/postgres"
)

// Protects gap persistence failures from being reported as successful detection.
func TestGapPersistenceFailureIsTerminal(t *testing.T) {
	detector := &GapDetector{storeGapFn: func(context.Context, string, ResearchGap) error { return errors.New("write failed") }}
	err := detector.persistGaps(context.Background(), uuid.NewString(), []ResearchGap{{Title: "gap"}})
	var batchErr *BatchError
	if !errors.As(err, &batchErr) || batchErr.Succeeded != 0 || len(batchErr.Failures) != 1 {
		t.Fatalf("error = %#v, want one gap persistence failure", err)
	}
}

// Protects novel-direction persistence failures from being reported as evaluated gaps.
func TestFeasibilityPersistenceFailureIsTerminal(t *testing.T) {
	evaluator := &FeasibilityEvaluator{
		evaluateFn: func(context.Context, ResearchGap) (*FeasibilityResult, error) {
			return &FeasibilityResult{Difficulty: "low"}, nil
		},
		storeFn: func(context.Context, string, ResearchGap, *FeasibilityResult) error {
			return errors.New("write failed")
		},
	}
	results, err := evaluator.Evaluate(context.Background(), uuid.NewString(), []ResearchGap{{Title: "gap"}})
	if err == nil || len(results) != 0 {
		t.Fatalf("Evaluate() = (%v, %v), want no persisted results and an error", results, err)
	}
}

type failingReportStore struct{ err error }

func (s failingReportStore) GetResearchTopic(context.Context, uuid.UUID) (*postgres.ResearchTopic, error) {
	return nil, s.err
}
func (s failingReportStore) GetPapersByTopicForAnalysis(context.Context, uuid.UUID) ([]*postgres.GetPapersByTopicForAnalysisRow, error) {
	return nil, nil
}
func (s failingReportStore) GetResearchGapsByTopic(context.Context, uuid.UUID) ([]*postgres.ResearchGap, error) {
	return nil, nil
}
func (s failingReportStore) GetNovelDirectionsByTopic(context.Context, uuid.UUID) ([]*postgres.NovelDirection, error) {
	return nil, nil
}

// Protects report generation from converting database query failures into partial reports.
func TestReportQueryFailureIsTerminal(t *testing.T) {
	generator := &ReportGenerator{postgres: failingReportStore{err: errors.New("database unavailable")}}
	if report, err := generator.Generate(context.Background(), uuid.NewString()); err == nil || report != nil {
		t.Fatalf("Generate() = (%v, %v), want a terminal query error", report, err)
	}
}
