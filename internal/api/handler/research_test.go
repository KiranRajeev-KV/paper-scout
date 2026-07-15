package handler

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/paper-scout/internal/agent"
	"github.com/paper-scout/internal/orchestrator"
)

// Protects build research response uses documented structured fields.
func TestBuildResearchResponseUsesDocumentedStructuredFields(t *testing.T) {
	pipeline := &orchestrator.Pipeline{
		TopicID:   "topic-1",
		Topic:     "test topic",
		Status:    "completed",
		Stage:     orchestrator.StageCompleted,
		Progress:  1,
		StartedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
	}
	report := &agent.Report{
		Topic:            "test topic",
		ExecutiveSummary: "summary",
		LiteratureReview: "review",
		Papers: []agent.PaperSummary{{
			ID:      "paper-1",
			Title:   "Paper",
			Authors: []string{"Author"},
		}},
		Gaps:        []agent.GapSummary{{Title: "Gap"}},
		Directions:  []agent.DirectionSummary{{Title: "Direction"}},
		GeneratedAt: time.Date(2026, 1, 2, 4, 4, 5, 0, time.UTC),
	}

	body, err := json.Marshal(buildResearchResponse(pipeline, report))
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	for _, field := range []string{"papers", "research_gaps", "novel_directions", "executive_summary", "literature_review"} {
		if _, ok := decoded[field]; !ok {
			t.Fatalf("response missing documented field %q: %s", field, body)
		}
	}
	if _, ok := decoded["stage"]; !ok {
		t.Fatalf("response missing pipeline status fields: %s", body)
	}
}

// Protects full research response contract.
func TestFullResearchResponseContract(t *testing.T) {
	pipeline := &orchestrator.Pipeline{
		TopicID: "topic-1", Topic: "test topic", Status: "completed", Stage: orchestrator.StageCompleted, Progress: 1, StartedAt: time.Now(),
	}
	report := &agent.Report{
		Papers:     []agent.PaperSummary{{ID: "paper-1", Title: "Paper", Authors: []string{"Ada"}}},
		Gaps:       []agent.GapSummary{{Type: "limitation", Title: "Gap", Description: "Description", Evidence: "paper-1"}},
		Directions: []agent.DirectionSummary{{Title: "Direction", IndustryViability: "High demand"}},
	}
	response := buildResearchResponse(pipeline, report)
	body, err := json.Marshal(response)
	if err != nil {
		t.Fatalf("marshal response: %v", err)
	}
	var decoded struct {
		Papers          []PaperResponse     `json:"papers"`
		ResearchGaps    []GapResponse       `json:"research_gaps"`
		NovelDirections []DirectionResponse `json:"novel_directions"`
	}
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(decoded.Papers) != 1 || len(decoded.ResearchGaps) != 1 || len(decoded.NovelDirections) != 1 {
		t.Fatalf("documented arrays missing from response: %s", body)
	}
	if got := decoded.NovelDirections[0].IndustryViability; got != "High demand" {
		t.Fatalf("industry viability = %q, want High demand", got)
	}
}

// Protects build research response does not require report while processing.
func TestBuildResearchResponseDoesNotRequireReportWhileProcessing(t *testing.T) {
	pipeline := &orchestrator.Pipeline{
		TopicID:   "topic-1",
		Topic:     "test topic",
		Status:    "processing",
		Stage:     orchestrator.StageAnalysis,
		Progress:  0.5,
		StartedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
	}

	response := buildResearchResponse(pipeline, nil)
	if response.Status != "processing" || response.Stage != string(orchestrator.StageAnalysis) || response.Progress != 0.5 {
		t.Fatalf("unexpected status response: %+v", response)
	}
	if response.Papers == nil || response.ResearchGaps == nil || response.NovelDirections == nil {
		t.Fatalf("result arrays must be initialized: %+v", response)
	}
}
