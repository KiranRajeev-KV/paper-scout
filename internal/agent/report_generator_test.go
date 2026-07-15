package agent

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/paper-scout/internal/storage/postgres"
	"github.com/paper-scout/internal/tools/bibtex"
)

// Protects build paper summaries includes authors.
func TestBuildPaperSummariesIncludesAuthors(t *testing.T) {
	paper := &postgres.GetPapersByTopicForAnalysisRow{
		ID:      uuid.New(),
		Title:   "A paper",
		Authors: []string{"Ada Lovelace", "Alan Turing"},
		Abstract: pgtype.Text{
			String: "abstract",
			Valid:  true,
		},
	}

	summaries := (&ReportGenerator{}).buildPaperSummaries([]*postgres.GetPapersByTopicForAnalysisRow{paper})
	if len(summaries) != 1 || len(summaries[0].Authors) != 2 || summaries[0].Authors[0] != "Ada Lovelace" || summaries[0].Authors[1] != "Alan Turing" {
		t.Fatalf("authors = %#v", summaries[0].Authors)
	}
}

// Protects generate bib te x includes authors.
func TestGenerateBibTeXIncludesAuthors(t *testing.T) {
	generator := &ReportGenerator{bibtexGen: bibtex.NewGenerator()}
	paper := &postgres.GetPapersByTopicForAnalysisRow{
		ID:      uuid.New(),
		Title:   "A paper",
		Authors: []string{"Ada Lovelace", "Alan Turing"},
	}

	bib := generator.generateBibTeX([]*postgres.GetPapersByTopicForAnalysisRow{paper})
	if !strings.Contains(bib, "author = {Ada Lovelace and Alan Turing}") {
		t.Fatalf("BibTeX omitted authors: %s", bib)
	}
}

// Protects report includes authors and industry viability.
func TestReportIncludesAuthorsAndIndustryViability(t *testing.T) {
	generator := &ReportGenerator{bibtexGen: bibtex.NewGenerator()}
	paper := &postgres.GetPapersByTopicForAnalysisRow{ID: uuid.New(), Title: "A paper", Authors: []string{"Ada Lovelace"}}
	bib := generator.generateBibTeX([]*postgres.GetPapersByTopicForAnalysisRow{paper})
	markdown := generator.GenerateMarkdown(&Report{
		Directions: []DirectionSummary{{Title: "Direction", IndustryViability: "Applicable to industrial forecasting"}},
		BibTeX:     bib,
	})
	if !strings.Contains(markdown, "Ada Lovelace") {
		t.Fatalf("markdown references omitted author: %s", markdown)
	}
	if !strings.Contains(markdown, "Applicable to industrial forecasting") {
		t.Fatalf("markdown omitted industry viability: %s", markdown)
	}
}

// Protects report markdown formatter is canonical.
func TestReportMarkdownFormatterIsCanonical(t *testing.T) {
	generator := &ReportGenerator{}
	report := &Report{
		Topic:            "time-series forecasting",
		ExecutiveSummary: "Summary",
		LiteratureReview: "Review",
		Gaps: []GapSummary{{
			Type:        "limitation",
			Title:       "Missing evaluation",
			Description: "The evaluation is narrow.",
			Evidence:    "Paper A",
		}},
		Directions: []DirectionSummary{{
			Title:             "Broader evaluation",
			Difficulty:        "medium",
			EstimatedCost:     "moderate",
			IndustryViability: "High",
			TimeToMVP:         "3 months",
			Description:       "Evaluate across more datasets.",
		}},
		BibTeX: "@article{paper}",
	}

	if got, want := generator.GenerateMarkdown(report), FormatMarkdown(report); got != want {
		t.Fatalf("method formatter differs from canonical formatter\n got: %s\nwant: %s", got, want)
	}
}

// Protects gap evidence from being rendered as opaque identifiers instead of BibTeX citations.
func TestFormatMarkdownRendersEvidenceAsBibTeXCitations(t *testing.T) {
	first, second := uuid.NewString(), uuid.NewString()
	report := &Report{
		Gaps:   []GapSummary{{Title: "Gap", Evidence: first + ", " + second}},
		BibTeX: "@article{" + first + "}\n@article{" + second + "}",
	}

	markdown := FormatMarkdown(report)
	want := "**Evidence:** [@" + first + "; @" + second + "]"
	if !strings.Contains(markdown, want) {
		t.Fatalf("markdown evidence = %q, want citation keys %q", markdown, want)
	}
}

// Protects executive summary from hiding gaps or directions after an arbitrary preview count.
func TestExecutiveSummaryIncludesEveryGapAndDirection(t *testing.T) {
	report := &Report{
		Topic: "topic",
		Gaps: []GapSummary{
			{Title: "Gap one", Type: "limitation", Description: "one"},
			{Title: "Gap two", Type: "limitation", Description: "two"},
			{Title: "Gap three", Type: "limitation", Description: "three"},
			{Title: "Gap four", Type: "limitation", Description: "four"},
			{Title: "Gap five", Type: "limitation", Description: "five"},
			{Title: "Gap six", Type: "limitation", Description: "six"},
		},
		Directions: []DirectionSummary{
			{Title: "Direction one"}, {Title: "Direction two"},
			{Title: "Direction three"}, {Title: "Direction four"},
		},
	}

	summary := (&ReportGenerator{}).generateExecutiveSummary(report)
	for _, expected := range []string{"Gap six", "Direction four"} {
		if !strings.Contains(summary, expected) {
			t.Fatalf("summary omitted %q: %s", expected, summary)
		}
	}
}
