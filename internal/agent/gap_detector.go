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
	llm        *llm.Client
	postgres   *postgres.Client
	structured *llm.StructuredOutput
}

func NewGapDetector(llmClient *llm.Client, pg *postgres.Client) *GapDetector {
	return &GapDetector{
		llm:        llmClient,
		postgres:   pg,
		structured: llm.NewStructuredOutput(llmClient),
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
				summary := fmt.Sprintf("- ID: %s\n  Title: %s\n  Key Findings: %s\n  Limitations: %s",
					p.ID.String(), p.Title, analysis.KeyFindings, analysis.Limitations)
				papersSummary = append(papersSummary, summary)
			}
		}
	}

	prompt := fmt.Sprintf(`You are analyzing a collection of research papers to identify gaps and opportunities.

Topic: %s

Papers Summary:
%s

Identify research gaps and respond in JSON format:
{
  "gaps": [
    {
      "gap_type": "unexplored|conflicting|limitation",
      "title": "Brief title for the gap",
      "description": "Detailed description",
      "evidence": "Which papers support this finding",
      "related_paper_ids": ["id1", "id2"]
    }
  ]
}

Focus on:
1. Unexplored areas: Topics not adequately covered
2. Conflicting results: Papers with contradictory findings
3. Limitations: Repeated limitations across multiple papers

Limit to 5-10 most important gaps.`, topic, strings.Join(papersSummary, "\n\n"))

	schema := map[string]interface{}{
		"gaps": []map[string]interface{}{
			{
				"gap_type":          "",
				"title":             "",
				"description":       "",
				"evidence":          "",
				"related_paper_ids": []string{},
			},
		},
	}

	result, err := g.structured.Generate(ctx, prompt, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to detect gaps: %w", err)
	}

	var response struct {
		Gaps []ResearchGap `json:"gaps"`
	}

	if err := json.Unmarshal([]byte(result), &response); err != nil {
		return nil, fmt.Errorf("failed to parse gaps: %w", err)
	}

	for i := range response.Gaps {
		if err := g.storeGap(ctx, topicID, response.Gaps[i]); err != nil {
			logger.Warn().Err(err).Str("gap", response.Gaps[i].Title).Msg("Failed to store gap")
		}
	}

	logger.Info().
		Int("gaps", len(response.Gaps)).
		Msg("Gap detection complete")

	return response.Gaps, nil
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
