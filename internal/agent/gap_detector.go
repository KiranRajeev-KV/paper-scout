package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/paper-scout/internal/llm"
	"github.com/paper-scout/internal/logger"
	"github.com/paper-scout/internal/storage/postgres"
)

type GapDetector struct {
	postgres   *postgres.Client
	structured *llm.StructuredOutput
}

func NewGapDetector(llmClient *llm.Client, pg *postgres.Client) *GapDetector {
	return &GapDetector{
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

type gapDetectionResponse struct {
	Gaps []gapDetectionItem `json:"gaps"`
}

type gapDetectionItem struct {
	GapType             string `json:"gap_type"`
	Title               string `json:"title"`
	Description         string `json:"description"`
	EvidenceIndices     []int  `json:"evidence_indices"`
	RelatedPaperIndices []int  `json:"related_paper_indices"`
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

	prompt := buildGapPrompt(topic, papers)

	var response gapDetectionResponse
	if err := g.structured.GenerateInto(ctx, prompt, gapDetectionResponse{
		Gaps: []gapDetectionItem{{
			GapType:             "unexplored",
			Title:               "",
			Description:         "",
			EvidenceIndices:     []int{1},
			RelatedPaperIndices: []int{1},
		}},
	}, &response); err != nil {
		return nil, fmt.Errorf("failed to detect gaps: %w", err)
	}

	gaps, err := resolveGapReferences(response.Gaps, papers)
	if err != nil {
		return nil, fmt.Errorf("invalid gap references: %w", err)
	}
	if len(gaps) == 0 {
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

func buildGapPrompt(topic string, papers []*postgres.GetPapersByTopicForAnalysisRow) string {
	papersSummary := make([]string, 0, len(papers))
	for i, p := range papers {
		limitations := ""
		var analysis PaperAnalysis
		if p.TopicAnalysis != nil {
			if err := json.Unmarshal(p.TopicAnalysis, &analysis); err == nil {
				limitations = analysis.Limitations
			}
		}
		papersSummary = append(papersSummary, fmt.Sprintf("%d. {\"title\": %q, \"limitation\": %q}",
			i+1, truncateText(p.Title, 100), truncateText(limitations, 200)))
	}

	return fmt.Sprintf(`Analyze these papers. Identify 3-5 research gaps.

Topic: %s

Papers:
%s

Return JSON only with this shape:
{
  "gaps": [
    {
      "gap_type": "unexplored|conflicting|limitation",
      "title": "max 60 chars",
      "description": "max 100 chars",
      "evidence_indices": [1, 3],
      "related_paper_indices": [1, 3]
    }
  ]
}
Use only 1-based paper indices from the list. Do not generate or reconstruct UUIDs.`, topic, strings.Join(papersSummary, "\n"))
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

func resolveGapReferences(items []gapDetectionItem, papers []*postgres.GetPapersByTopicForAnalysisRow) ([]ResearchGap, error) {
	gaps := make([]ResearchGap, 0, len(items))
	for itemIndex, item := range items {
		if err := validateGapItem(item, itemIndex); err != nil {
			return nil, err
		}

		evidenceIDs, err := resolvePaperIndices(item.EvidenceIndices, papers, fmt.Sprintf("gap %d evidence", itemIndex+1))
		if err != nil {
			return nil, err
		}
		relatedIDs, err := resolvePaperIndices(item.RelatedPaperIndices, papers, fmt.Sprintf("gap %d related papers", itemIndex+1))
		if err != nil {
			return nil, err
		}

		gaps = append(gaps, ResearchGap{
			GapType:       item.GapType,
			Title:         truncateText(item.Title, 60),
			Description:   truncateText(item.Description, 100),
			Evidence:      strings.Join(evidenceIDs, ","),
			RelatedPapers: relatedIDs,
		})
	}
	return gaps, nil
}

func validateGapItem(item gapDetectionItem, itemIndex int) error {
	prefix := fmt.Sprintf("gap %d", itemIndex+1)
	switch item.GapType {
	case "unexplored", "conflicting", "limitation":
	default:
		return fmt.Errorf("%s has invalid gap_type %q", prefix, item.GapType)
	}
	if strings.TrimSpace(item.Title) == "" {
		return fmt.Errorf("%s is missing title", prefix)
	}
	if utf8.RuneCountInString(item.Title) > 60 {
		return fmt.Errorf("%s title exceeds 60 characters", prefix)
	}
	if strings.TrimSpace(item.Description) == "" {
		return fmt.Errorf("%s is missing description", prefix)
	}
	if utf8.RuneCountInString(item.Description) > 100 {
		return fmt.Errorf("%s description exceeds 100 characters", prefix)
	}
	if len(item.EvidenceIndices) == 0 {
		return fmt.Errorf("%s is missing evidence_indices", prefix)
	}
	return nil
}

func resolvePaperIndices(indices []int, papers []*postgres.GetPapersByTopicForAnalysisRow, field string) ([]string, error) {
	ids := make([]string, 0, len(indices))
	seen := make(map[int]struct{}, len(indices))
	for _, index := range indices {
		if index < 1 || index > len(papers) {
			return nil, fmt.Errorf("%s contains index %d, want 1..%d", field, index, len(papers))
		}
		if _, exists := seen[index]; exists {
			return nil, fmt.Errorf("%s contains duplicate index %d", field, index)
		}
		seen[index] = struct{}{}
		ids = append(ids, papers[index-1].ID.String())
	}
	return ids, nil
}
