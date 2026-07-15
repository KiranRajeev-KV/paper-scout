package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/paper-scout/internal/logger"
	"github.com/paper-scout/internal/storage/postgres"
	"github.com/paper-scout/internal/tools/bibtex"
)

type ReportGenerator struct {
	postgres  reportStore
	bibtexGen *bibtex.Generator
}

type reportStore interface {
	GetResearchTopic(context.Context, uuid.UUID) (*postgres.ResearchTopic, error)
	GetPapersByTopicForAnalysis(context.Context, uuid.UUID) ([]*postgres.GetPapersByTopicForAnalysisRow, error)
	GetResearchGapsByTopic(context.Context, uuid.UUID) ([]*postgres.ResearchGap, error)
	GetNovelDirectionsByTopic(context.Context, uuid.UUID) ([]*postgres.NovelDirection, error)
}

func NewReportGenerator(pg *postgres.Client) *ReportGenerator {
	return &ReportGenerator{
		postgres:  pg.Queries(),
		bibtexGen: bibtex.NewGenerator(),
	}
}

type Report struct {
	Topic            string
	ExecutiveSummary string
	LiteratureReview string
	Papers           []PaperSummary
	Gaps             []GapSummary
	Directions       []DirectionSummary
	BibTeX           string
	GeneratedAt      time.Time
}

type PaperSummary struct {
	ID               string
	Title            string
	Authors          []string
	Year             int
	Venue            string
	Abstract         string
	ProblemStatement string
	Methodology      string
	KeyFindings      string
	Limitations      string
	RelevanceScore   float64
}

type GapSummary struct {
	Type        string
	Title       string
	Description string
	Evidence    string
}

type DirectionSummary struct {
	Title             string
	Description       string
	Difficulty        string
	EstimatedCost     string
	IndustryViability string
	TimeToMVP         string
	FeasibilityScore  float64
}

func (r *ReportGenerator) Generate(ctx context.Context, topicID string) (*Report, error) {
	logger.From(ctx).Info().Str("topic_id", topicID).Msg("Generating report")

	id, err := parseID("topic ID", topicID)
	if err != nil {
		return nil, err
	}
	topic, err := r.postgres.GetResearchTopic(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to get topic: %w", err)
	}

	papers, err := r.postgres.GetPapersByTopicForAnalysis(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to get papers: %w", err)
	}

	gaps, err := r.postgres.GetResearchGapsByTopic(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to get research gaps: %w", err)
	}

	directions, err := r.postgres.GetNovelDirectionsByTopic(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("failed to get novel directions: %w", err)
	}

	report := &Report{
		Topic:       topic.Topic,
		GeneratedAt: time.Now(),
		Papers:      r.buildPaperSummaries(papers),
		Gaps:        r.buildGapSummaries(gaps),
		Directions:  r.buildDirectionSummaries(directions),
	}

	report.ExecutiveSummary = r.generateExecutiveSummary(report)
	report.LiteratureReview = r.generateLiteratureReview(report.Papers)
	report.BibTeX = r.generateBibTeX(papers)

	logger.From(ctx).Info().Str("topic_id", topicID).Msg("Report generated")

	return report, nil
}

func (r *ReportGenerator) buildPaperSummaries(papers []*postgres.GetPapersByTopicForAnalysisRow) []PaperSummary {
	summaries := make([]PaperSummary, 0, len(papers))

	for _, p := range papers {
		summary := PaperSummary{
			ID:       p.ID.String(),
			Title:    p.Title,
			Authors:  append([]string(nil), p.Authors...),
			Abstract: pgTextVal(p.Abstract),
			Venue:    pgTextVal(p.Venue),
		}

		summary.RelevanceScore = pgFloat64Val(p.TopicRelevanceScore)
		summary.Year = pgDateVal(p.PublicationDate)

		if p.TopicAnalysis != nil {
			var analysis PaperAnalysis
			if err := json.Unmarshal(p.TopicAnalysis, &analysis); err == nil {
				summary.ProblemStatement = analysis.ProblemStatement
				summary.Methodology = analysis.Methodology
				summary.KeyFindings = analysis.KeyFindings
				summary.Limitations = analysis.Limitations
			}
		}

		summaries = append(summaries, summary)
	}

	return summaries
}

func (r *ReportGenerator) buildGapSummaries(gaps []*postgres.ResearchGap) []GapSummary {
	summaries := make([]GapSummary, 0, len(gaps))

	for _, g := range gaps {
		summary := GapSummary{
			Type:        g.GapType,
			Title:       g.Title,
			Description: pgTextVal(g.Description),
			Evidence:    pgTextVal(g.Evidence),
		}
		summaries = append(summaries, summary)
	}

	return summaries
}

func (r *ReportGenerator) buildDirectionSummaries(directions []*postgres.NovelDirection) []DirectionSummary {
	summaries := make([]DirectionSummary, 0, len(directions))

	for _, d := range directions {
		summary := DirectionSummary{
			Title:             d.Title,
			Description:       pgTextVal(d.Description),
			Difficulty:        pgTextVal(d.ImplementationComplexity),
			EstimatedCost:     pgTextVal(d.EstimatedCost),
			IndustryViability: pgTextVal(d.IndustryViability),
			TimeToMVP:         pgTextVal(d.TimeToMvp),
		}

		summary.FeasibilityScore = pgFloat64Val(d.FeasibilityScore)

		summaries = append(summaries, summary)
	}

	return summaries
}

func (r *ReportGenerator) generateExecutiveSummary(report *Report) string {
	var b strings.Builder

	b.WriteString("# Executive Summary\n\n")
	b.WriteString(fmt.Sprintf("This report analyzes **%d** academic papers related to: **%s**\n\n", len(report.Papers), report.Topic))

	if len(report.Gaps) > 0 {
		b.WriteString(fmt.Sprintf("### Research Gaps Identified: %d\n\n", len(report.Gaps)))
		for _, gap := range report.Gaps {
			b.WriteString(fmt.Sprintf("- **%s** (%s): %s\n", gap.Title, gap.Type, gap.Description))
		}
		b.WriteString("\n")
	}

	if len(report.Directions) > 0 {
		b.WriteString(fmt.Sprintf("### Research Directions Proposed: %d\n\n", len(report.Directions)))
		for _, dir := range report.Directions {
			b.WriteString(fmt.Sprintf("- **%s** (Difficulty: %s, Score: %.1f)\n", dir.Title, dir.Difficulty, dir.FeasibilityScore))
		}
		b.WriteString("\n")
	}

	b.WriteString(fmt.Sprintf("*Report generated on %s*\n", report.GeneratedAt.Format("January 2, 2006")))

	return b.String()
}

func (r *ReportGenerator) generateLiteratureReview(papers []PaperSummary) string {
	var b strings.Builder

	b.WriteString("# Literature Review\n\n")
	b.WriteString(fmt.Sprintf("Total papers analyzed: %d\n\n", len(papers)))

	b.WriteString("## Comparative Table\n\n")
	b.WriteString("| Title | Methodology | Key Findings | Limitations |\n")
	b.WriteString("|-------|-------------|--------------|-------------|\n")

	for _, p := range papers {
		b.WriteString(fmt.Sprintf("| %s | %s | %s | %s |\n", p.Title, p.Methodology, p.KeyFindings, p.Limitations))
	}

	return b.String()
}

func (r *ReportGenerator) generateBibTeX(papers []*postgres.GetPapersByTopicForAnalysisRow) string {
	entries := make([]*bibtex.Entry, 0, len(papers))

	for _, p := range papers {
		entry := &bibtex.Entry{
			ID:       p.ID.String(),
			Authors:  append([]string(nil), p.Authors...),
			Title:    p.Title,
			Abstract: pgTextVal(p.Abstract),
			Venue:    pgTextVal(p.Venue),
			Year:     pgDateVal(p.PublicationDate),
		}

		entries = append(entries, entry)
	}

	return r.bibtexGen.GenerateBatch(entries)
}

func (r *ReportGenerator) GenerateMarkdown(report *Report) string {
	return FormatMarkdown(report)
}

// FormatMarkdown renders the canonical human-readable research report.
func FormatMarkdown(report *Report) string {
	var b strings.Builder

	b.WriteString(report.ExecutiveSummary)
	b.WriteString("\n\n---\n\n")
	b.WriteString(report.LiteratureReview)

	if len(report.Gaps) > 0 {
		b.WriteString("\n\n---\n\n")
		b.WriteString("# Research Gaps\n\n")
		for i, gap := range report.Gaps {
			b.WriteString(fmt.Sprintf("## %d. %s\n\n", i+1, gap.Title))
			b.WriteString(fmt.Sprintf("**Type:** %s\n\n", gap.Type))
			b.WriteString(fmt.Sprintf("**Description:** %s\n\n", gap.Description))
			b.WriteString(fmt.Sprintf("**Evidence:** %s\n\n", formatEvidenceCitations(gap.Evidence)))
		}
	}

	if len(report.Directions) > 0 {
		b.WriteString("\n\n---\n\n")
		b.WriteString("# Proposed Research Directions\n\n")
		for i, dir := range report.Directions {
			b.WriteString(fmt.Sprintf("## %d. %s\n\n", i+1, dir.Title))
			b.WriteString(fmt.Sprintf("**Difficulty:** %s\n", dir.Difficulty))
			b.WriteString(fmt.Sprintf("**Estimated Cost:** %s\n", dir.EstimatedCost))
			b.WriteString(fmt.Sprintf("**Industry Viability:** %s\n", dir.IndustryViability))
			b.WriteString(fmt.Sprintf("**Time to MVP:** %s\n", dir.TimeToMVP))
			b.WriteString(fmt.Sprintf("**Description:** %s\n\n", dir.Description))
		}
	}

	b.WriteString("\n\n---\n\n")
	b.WriteString("# References\n\n")
	b.WriteString("```bibtex\n")
	b.WriteString(report.BibTeX)
	b.WriteString("\n```\n")

	return b.String()
}

// formatEvidenceCitations renders the comma-separated paper IDs stored with a
// research gap as Pandoc/BibLaTeX citations. Paper IDs are the canonical
// BibTeX entry keys emitted by generateBibTeX, so the rendered report always
// points to an entry in its References section.
func formatEvidenceCitations(evidence string) string {
	keys := make([]string, 0)
	for _, key := range strings.Split(evidence, ",") {
		if key = strings.TrimSpace(key); key != "" {
			keys = append(keys, "@"+key)
		}
	}
	if len(keys) == 0 {
		return "Not specified"
	}
	return "[" + strings.Join(keys, "; ") + "]"
}
