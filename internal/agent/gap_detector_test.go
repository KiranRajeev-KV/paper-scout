package agent

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/paper-scout/internal/storage/postgres"
)

func TestBuildGapPromptUsesStablePaperIndices(t *testing.T) {
	papers := []*postgres.GetPapersByTopicForAnalysisRow{
		{ID: uuid.New(), Title: "First paper"},
		{ID: uuid.New(), Title: "Second paper"},
	}

	prompt := buildGapPrompt("test topic", papers)
	if !strings.Contains(prompt, "1. {\"title\": \"First paper\"") ||
		!strings.Contains(prompt, "2. {\"title\": \"Second paper\"") {
		t.Fatalf("prompt does not contain numbered paper objects: %s", prompt)
	}
	if strings.Contains(prompt, papers[0].ID.String()[:8]) || strings.Contains(prompt, papers[1].ID.String()[:8]) {
		t.Fatalf("prompt contains machine identifiers: %s", prompt)
	}
}

func TestResolveGapReferencesMapsFullUUIDs(t *testing.T) {
	paperA := &postgres.GetPapersByTopicForAnalysisRow{ID: uuid.New()}
	paperB := &postgres.GetPapersByTopicForAnalysisRow{ID: uuid.New()}

	gaps, err := resolveGapReferences([]gapDetectionItem{{
		GapType:             "limitation",
		Title:               "Missing evaluation",
		Description:         "The papers do not compare against a common baseline.",
		EvidenceIndices:     []int{2},
		RelatedPaperIndices: []int{1, 2},
	}}, []*postgres.GetPapersByTopicForAnalysisRow{paperA, paperB})
	if err != nil {
		t.Fatalf("resolveGapReferences returned error: %v", err)
	}
	if got, want := gaps[0].Evidence, paperB.ID.String(); got != want {
		t.Fatalf("evidence = %q, want %q", got, want)
	}
	if len(gaps[0].RelatedPapers) != 2 || gaps[0].RelatedPapers[0] != paperA.ID.String() || gaps[0].RelatedPapers[1] != paperB.ID.String() {
		t.Fatalf("related papers = %#v", gaps[0].RelatedPapers)
	}
}

func TestResolveGapReferencesRejectsInvalidAndDuplicateIndices(t *testing.T) {
	papers := []*postgres.GetPapersByTopicForAnalysisRow{{ID: uuid.New()}, {ID: uuid.New()}}
	for name, indices := range map[string][]int{
		"zero":         {0},
		"negative":     {-1},
		"out of range": {3},
		"duplicate":    {1, 1},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := resolveGapReferences([]gapDetectionItem{{Title: "Gap", EvidenceIndices: indices}}, papers)
			if err == nil {
				t.Fatal("resolveGapReferences accepted invalid indices")
			}
		})
	}
}

func TestResolveGapReferencesRejectsMalformedGapFields(t *testing.T) {
	papers := []*postgres.GetPapersByTopicForAnalysisRow{{ID: uuid.New()}}
	base := gapDetectionItem{
		GapType:             "limitation",
		Title:               "Missing evaluation",
		Description:         "The papers lack a shared baseline.",
		EvidenceIndices:     []int{1},
		RelatedPaperIndices: []int{1},
	}
	for name, mutate := range map[string]func(*gapDetectionItem){
		"invalid type":        func(item *gapDetectionItem) { item.GapType = "other" },
		"missing title":       func(item *gapDetectionItem) { item.Title = "" },
		"missing description": func(item *gapDetectionItem) { item.Description = "" },
		"missing evidence":    func(item *gapDetectionItem) { item.EvidenceIndices = nil },
	} {
		t.Run(name, func(t *testing.T) {
			item := base
			mutate(&item)
			if _, err := resolveGapReferences([]gapDetectionItem{item}, papers); err == nil {
				t.Fatal("validation accepted malformed gap")
			}
		})
	}
}
