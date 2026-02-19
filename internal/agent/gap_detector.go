package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/research-agent/internal/llm"
	"github.com/research-agent/internal/logger"
	"github.com/research-agent/internal/storage/postgres"
)

type GapDetector struct {
	llm      *llm.Client
	postgres *postgres.Client
}

func NewGapDetector(llmClient *llm.Client, pg *postgres.Client) *GapDetector {
	return &GapDetector{
		llm:      llmClient,
		postgres: pg,
	}
}

type ResearchGap struct {
	GapType       string   `json:"gap_type"`
	Title         string   `json:"title"`
	Description   string   `json:"description"`
	Evidence      string   `json:"evidence"`
	RelatedPapers []string `json:"related_paper_ids"`
}

func (g *GapDetector) Detect(ctx context.Context, topicID, topic string) ([]ResearchGap, error) {
	logger.Info().
		Str("topic_id", topicID).
		Msg("Starting gap detection")

	papers, err := g.postgres.Queries().GetPapersByTopicForAnalysis(ctx, pgUUID(topicID))
	if err != nil {
		return nil, fmt.Errorf("failed to get analyzed papers: %w", err)
	}

	if len(papers) == 0 {
		return nil, fmt.Errorf("no analyzed papers available for gap detection")
	}

	logger.Info().Int("papers", len(papers)).Msg("Papers available for gap detection")

	var papersSummary []string
	for _, p := range papers {
		var analysis PaperAnalysis
		if p.Analysis != nil {
			if err := json.Unmarshal(p.Analysis, &analysis); err == nil {
				summary := fmt.Sprintf("- %s: %s (Limitations: %s)", p.ID.String()[:8], truncateText(p.Title, 50), truncateText(analysis.Limitations, 50))
				papersSummary = append(papersSummary, summary)
			}
		}
	}

	prompt := fmt.Sprintf(`Analyze these papers. Identify 3-5 research gaps. Answer with numbered lists.

Topic: %s

Papers:
%s

Format for each gap:
---GAP---
1. Type: unexplored/conflicting/limitation
2. Title: max 60 chars
3. Description: max 100 chars
4. Evidence: paper IDs (comma-separated)
5. Related: paper IDs (comma-separated)

Answer with gaps only. Use ---GAP--- to separate each.`, topic, strings.Join(papersSummary, "\n"))

	result, err := g.llm.Generate(ctx, prompt)
	if err != nil {
		return nil, fmt.Errorf("failed to detect gaps: %w", err)
	}

	logger.Debug().
		Int("result_len", len(result)).
		Str("result", truncateText(result, 500)).
		Msg("LLM gap detection result")

	gaps := parseGapsFromNumberedList(result)

	if len(gaps) == 0 {
		logger.Warn().Str("result", truncateText(result, 1000)).Msg("No gaps parsed from response")
		return nil, fmt.Errorf("no valid gaps found in response")
	}

	for i := range gaps {
		if err := g.storeGap(ctx, topicID, gaps[i]); err != nil {
			logger.Warn().Err(err).Str("gap", gaps[i].Title).Msg("Failed to store gap")
		}
	}

	logger.Info().
		Int("gaps", len(gaps)).
		Msg("Gap detection complete")

	return gaps, nil
}

func (g *GapDetector) storeGap(ctx context.Context, topicID string, gap ResearchGap) error {
	_, err := g.postgres.Queries().CreateResearchGap(ctx, postgres.CreateResearchGapParams{
		TopicID:         pgUUID(topicID),
		GapType:         gap.GapType,
		Title:           gap.Title,
		Description:     pgText(gap.Description),
		RelatedPaperIds: pgUUIDsFromStrings(gap.RelatedPapers),
		Evidence:        pgText(gap.Evidence),
	})

	return err
}

func parseGapsFromNumberedList(result string) []ResearchGap {
	var gaps []ResearchGap

	blocks := strings.Split(result, "---GAP---")

	for _, block := range blocks {
		block = strings.TrimSpace(block)
		if block == "" {
			continue
		}

		gap := parseSingleGap(block)
		if gap != nil && gap.Title != "" {
			gaps = append(gaps, *gap)
		}
	}

	return gaps
}

func parseSingleGap(block string) *ResearchGap {
	lines := strings.Split(block, "\n")
	if len(lines) < 4 {
		return nil
	}

	extractField := func(lines []string, prefix string, maxLen int) string {
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if strings.HasPrefix(strings.ToLower(line), strings.ToLower(prefix)) {
				parts := strings.SplitN(line, ":", 2)
				if len(parts) > 1 {
					value := strings.TrimSpace(parts[1])
					if len(value) > maxLen {
						value = value[:maxLen]
					}
					return value
				}
			}
		}
		return ""
	}

	gap := &ResearchGap{
		GapType:     extractField(lines, "1. type", 20),
		Title:       extractField(lines, "2. title", 60),
		Description: extractField(lines, "3. description", 100),
		Evidence:    extractField(lines, "4. evidence", 200),
	}

	relatedStr := extractField(lines, "5. related", 200)
	if relatedStr != "" {
		ids := strings.Split(relatedStr, ",")
		for i, id := range ids {
			ids[i] = strings.TrimSpace(id)
		}
		gap.RelatedPapers = ids
	}

	if gap.GapType == "" {
		gap.GapType = "unexplored"
	}

	return gap
}
