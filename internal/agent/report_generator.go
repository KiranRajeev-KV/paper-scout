package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/paper-scout/internal/logger"
	"github.com/paper-scout/internal/storage/postgres"
	"github.com/paper-scout/internal/tools/bibtex"
)

type ReportGenerator struct {
	postgres  *postgres.Client
	bibtexGen *bibtex.Generator
}

func NewReportGenerator(pg *postgres.Client) *ReportGenerator {
	return &ReportGenerator{
		postgres:  pg,
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
	logger.Info().Str("topic_id", topicID).Msg("Generating report")

	topic, err := r.postgres.Queries().GetResearchTopic(ctx, pgUUID(topicID))
	if err != nil {
		return nil, fmt.Errorf("failed to get topic: %w", err)
	}

	papers, err := r.postgres.Queries().GetPapersByTopicForAnalysis(ctx, pgUUID(topicID))
	if err != nil {
		logger.Warn().Err(err).Msg("Failed to get papers")
	}

	gaps, err := r.postgres.Queries().GetResearchGapsByTopic(ctx, pgUUID(topicID))
	if err != nil {
		logger.Warn().Err(err).Msg("Failed to get research gaps")
	}

	directions, err := r.postgres.Queries().GetNovelDirectionsByTopic(ctx, pgUUID(topicID))
	if err != nil {
		logger.Warn().Err(err).Msg("Failed to get novel directions")
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

	logger.Info().Str("topic_id", topicID).Msg("Report generated")

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
			Title:         d.Title,
			Description:   pgTextVal(d.Description),
			Difficulty:    pgTextVal(d.ImplementationComplexity),
			EstimatedCost: pgTextVal(d.EstimatedCost),
			TimeToMVP:     pgTextVal(d.TimeToMvp),
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
		for i, gap := range report.Gaps {
			if i >= 5 {
				break
			}
			b.WriteString(fmt.Sprintf("- **%s** (%s): %s\n", gap.Title, gap.Type, truncateText(gap.Description, 100)))
		}
		b.WriteString("\n")
	}

	if len(report.Directions) > 0 {
		b.WriteString(fmt.Sprintf("### Research Directions Proposed: %d\n\n", len(report.Directions)))
		for i, dir := range report.Directions {
			if i >= 3 {
				break
			}
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

	for i, p := range papers {
		if i >= 20 {
			break
		}
		title := truncateText(p.Title, 50)
		method := truncateText(p.Methodology, 40)
		findings := truncateText(p.KeyFindings, 40)
		limits := truncateText(p.Limitations, 40)
		b.WriteString(fmt.Sprintf("| %s | %s | %s | %s |\n", title, method, findings, limits))
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
			b.WriteString(fmt.Sprintf("**Evidence:** %s\n\n", gap.Evidence))
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
