package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/paper-scout/internal/llm"
	"github.com/paper-scout/internal/logger"
	"github.com/paper-scout/internal/storage/postgres"
	"github.com/paper-scout/internal/tools/embedding"
)

type GapDetector struct {
	postgres   *postgres.Client
	structured *llm.StructuredOutput
	embedder   *embedding.Generator
	maxChunks  int
	retrieveFn func(context.Context, string, string) []retrievedChunk
	storeGapFn func(context.Context, string, ResearchGap) error
}

func NewGapDetector(llmClient llm.Generator, pg *postgres.Client, embedder *embedding.Generator, maxChunks int) *GapDetector {
	if maxChunks <= 0 {
		maxChunks = 12
	}
	return &GapDetector{
		postgres:   pg,
		structured: llm.NewStructuredOutput(llmClient),
		embedder:   embedder,
		maxChunks:  maxChunks,
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
	logger.From(ctx).Info().
		Str("topic_id", topicID).
		Msg("Starting gap detection")

	topicUUID, err := parseID("topic ID", topicID)
	if err != nil {
		return nil, err
	}
	papers, err := g.postgres.Queries().GetPapersByTopicForAnalysis(ctx, topicUUID)
	if err != nil {
		return nil, fmt.Errorf("failed to get analyzed papers: %w", err)
	}

	if len(papers) == 0 {
		return nil, fmt.Errorf("no analyzed papers available for gap detection")
	}

	logger.From(ctx).Info().Int("papers", len(papers)).Msg("Papers available for gap detection")

	retrieved, err := g.retrieve(ctx, topicID, topic)
	if err != nil {
		return nil, err
	}
	prompt := buildGapPrompt(topic, papers, retrieved)

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

	if err := g.persistGaps(ctx, topicID, gaps); err != nil {
		return nil, err
	}

	logger.From(ctx).Info().
		Int("gaps", len(gaps)).
		Msg("Gap detection complete")

	return gaps, nil
}

func (g *GapDetector) persistGaps(ctx context.Context, topicID string, gaps []ResearchGap) error {
	failures := make([]ItemFailure, 0)
	for i := range gaps {
		storeGap := g.storeGap
		if g.storeGapFn != nil {
			storeGap = g.storeGapFn
		}
		if err := storeGap(ctx, topicID, gaps[i]); err != nil {
			logger.From(ctx).Warn().Err(err).Str("gap", gaps[i].Title).Msg("Failed to store gap")
			failures = append(failures, ItemFailure{Kind: "research_gap", Identifier: gaps[i].Title, Err: err})
		}
	}
	return newBatchError("research gap persistence", len(gaps), failures)
}

func (g *GapDetector) retrieve(ctx context.Context, topicID, topic string) ([]retrievedChunk, error) {
	if g.retrieveFn != nil {
		return g.retrieveFn(ctx, topicID, topic), nil
	}
	return g.retrieveChunks(ctx, topicID, topic)
}

type retrievedChunk struct {
	PaperID string
	Text    string
	Score   float32
}

func (g *GapDetector) retrieveChunks(ctx context.Context, topicID, topic string) ([]retrievedChunk, error) {
	if g.embedder == nil {
		return nil, fmt.Errorf("retrieve gap evidence: embedding provider is not configured")
	}
	vector, err := g.embedder.Generate(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("embed gap evidence query for topic %s: %w", topicID, err)
	}
	results, err := g.embedder.SearchChunks(ctx, vector, uint64(g.maxChunks), topicID)
	if err != nil {
		return nil, fmt.Errorf("retrieve gap evidence for topic %s: %w", topicID, err)
	}
	chunks := make([]retrievedChunk, 0, len(results))
	for _, result := range results {
		if result.Text == "" || result.PaperID == "" {
			continue
		}
		chunks = append(chunks, retrievedChunk{PaperID: result.PaperID, Text: result.Text, Score: result.Score})
	}
	return chunks, nil
}

func buildGapPrompt(topic string, papers []*postgres.GetPapersByTopicForAnalysisRow, retrieved ...[]retrievedChunk) string {
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

	evidence := ""
	if len(retrieved) > 0 && len(retrieved[0]) > 0 {
		paperIndices := make(map[string]int, len(papers))
		for index, paper := range papers {
			paperIndices[paper.ID.String()] = index + 1
		}
		lines := make([]string, 0, len(retrieved[0]))
		for _, chunk := range retrieved[0] {
			if index, ok := paperIndices[chunk.PaperID]; ok {
				lines = append(lines, fmt.Sprintf("paper %d: %q", index, truncateText(chunk.Text, 500)))
			}
		}
		if len(lines) > 0 {
			evidence = "\nRetrieved full-text evidence:\n" + strings.Join(lines, "\n")
		}
	}

	return fmt.Sprintf(`Analyze these papers. Identify 3-5 research gaps.

Topic: %s

Papers:
%s
%s

Return JSON only with this shape:
{
  "gaps": [
    {
      "gap_type": "unexplored|conflicting|limitation",
	  "title": "string",
	  "description": "string",
      "evidence_indices": [1, 3],
      "related_paper_indices": [1, 3]
    }
  ]
}
Use only 1-based paper indices from the list. Do not generate or reconstruct UUIDs.`, topic, strings.Join(papersSummary, "\n"), evidence)
}

func (g *GapDetector) storeGap(ctx context.Context, topicID string, gap ResearchGap) error {
	topicUUID, err := parseID("topic ID", topicID)
	if err != nil {
		return err
	}
	relatedPaperIDs, err := parseIDs("related paper ID", gap.RelatedPapers)
	if err != nil {
		return err
	}
	_, err = g.postgres.Queries().CreateResearchGap(ctx, postgres.CreateResearchGapParams{
		TopicID:         topicUUID,
		GapType:         gap.GapType,
		Title:           gap.Title,
		Description:     pgText(gap.Description),
		RelatedPaperIds: relatedPaperIDs,
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
			Title:         item.Title,
			Description:   item.Description,
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
	if strings.TrimSpace(item.Description) == "" {
		return fmt.Errorf("%s is missing description", prefix)
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
