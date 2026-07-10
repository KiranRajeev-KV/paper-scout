package agent

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/paper-scout/internal/storage/postgres"
	"github.com/paper-scout/internal/tools/bibtex"
)

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
