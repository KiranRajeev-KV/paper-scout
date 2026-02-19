package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/research-agent/internal/llm"
	"github.com/research-agent/internal/logger"
	"github.com/research-agent/internal/storage/postgres"
	"github.com/research-agent/internal/tools/embedding"
)

const (
	RerankBatchSize = 10
	RerankTopK      = 50
)

type Ranker struct {
	postgres   *postgres.Client
	embedder   *embedding.Generator
	llm        *llm.Client
	structured *llm.StructuredOutput
}

func NewRanker(pg *postgres.Client, emb *embedding.Generator, llmClient *llm.Client) *Ranker {
	return &Ranker{
		postgres:   pg,
		embedder:   emb,
		llm:        llmClient,
		structured: llm.NewStructuredOutput(llmClient),
	}
}

type RankedPaper struct {
	ID             string
	Title          string
	Abstract       string
	RelevanceScore float64
}

type paperWithScore struct {
	paper *postgres.Paper
	score float64
}

func (r *Ranker) Rank(ctx context.Context, topicID string, topic string, maxPapers int) ([]RankedPaper, error) {
	logger.Info().
		Str("topic_id", topicID).
		Str("topic", topic).
		Msg("Starting paper ranking")

	topicVector, err := r.embedder.Generate(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("failed to embed topic: %w", err)
	}

	papers, err := r.postgres.Queries().GetPapersByTopic(ctx, pgUUID(topicID))
	if err != nil {
		return nil, fmt.Errorf("failed to get papers: %w", err)
	}

	logger.Info().Int("papers", len(papers)).Msg("Papers retrieved for ranking")

	var scored []paperWithScore

	for _, paper := range papers {
		abstract := pgTextVal(paper.Abstract)
		if abstract == "" {
			continue
		}

		abstractVector, err := r.embedder.Generate(ctx, abstract)
		if err != nil {
			logger.Warn().Err(err).Str("paper_id", paper.ID.String()).Msg("Failed to embed abstract")
			continue
		}

		score := cosineSimilarity(topicVector, abstractVector)
		scored = append(scored, paperWithScore{
			paper: paper,
			score: score,
		})
	}

	sort.Slice(scored, func(i, j int) bool {
		return scored[i].score > scored[j].score
	})

	topK := RerankTopK
	if len(scored) < topK {
		topK = len(scored)
	}
	scored = scored[:topK]

	if r.llm != nil && len(scored) > 0 {
		reranked, err := r.rerankWithLLM(ctx, topic, scored)
		if err != nil {
			logger.Warn().Err(err).Msg("LLM reranking failed, using embedding scores")
		} else {
			scored = reranked
		}
	}

	if len(scored) > maxPapers {
		scored = scored[:maxPapers]
	}

	ranked := make([]RankedPaper, 0, len(scored))
	for _, s := range scored {
		ranked = append(ranked, RankedPaper{
			ID:             s.paper.ID.String(),
			Title:          s.paper.Title,
			Abstract:       pgTextVal(s.paper.Abstract),
			RelevanceScore: s.score,
		})

		_, err := r.postgres.Queries().UpdatePaperRelevanceScore(ctx, postgres.UpdatePaperRelevanceScoreParams{
			ID:             s.paper.ID,
			RelevanceScore: pgFloat64(s.score),
		})
		if err != nil {
			logger.Warn().Err(err).Str("paper_id", s.paper.ID.String()).Msg("Failed to update relevance score")
		}
	}

	logger.Info().
		Int("ranked_papers", len(ranked)).
		Msg("Paper ranking complete")

	return ranked, nil
}

func (r *Ranker) rerankWithLLM(ctx context.Context, topic string, papers []paperWithScore) ([]paperWithScore, error) {
	logger.Info().Int("papers", len(papers)).Msg("Starting LLM reranking")

	allScored := make([]paperWithScore, 0, len(papers))

	for i := 0; i < len(papers); i += RerankBatchSize {
		end := i + RerankBatchSize
		if end > len(papers) {
			end = len(papers)
		}
		batch := papers[i:end]

		batchScored, err := r.rerankBatch(ctx, topic, batch)
		if err != nil {
			logger.Warn().Err(err).Int("batch_start", i).Msg("Failed to rerank batch")
			allScored = append(allScored, batch...)
			continue
		}

		allScored = append(allScored, batchScored...)
	}

	sort.Slice(allScored, func(i, j int) bool {
		return allScored[i].score > allScored[j].score
	})

	logger.Info().Int("papers", len(allScored)).Msg("LLM reranking complete")
	return allScored, nil
}

func (r *Ranker) rerankBatch(ctx context.Context, topic string, papers []paperWithScore) ([]paperWithScore, error) {
	var paperList strings.Builder
	for i, p := range papers {
		abstract := truncateText(pgTextVal(p.paper.Abstract), 500)
		paperList.WriteString(fmt.Sprintf("\n[%d] Title: %s\n    Abstract: %s", i+1, p.paper.Title, abstract))
	}

	prompt := fmt.Sprintf(`You are a research assistant. Rank the following papers by relevance to the given research topic.

Research Topic: %s

Papers:%s

For each paper, provide a relevance score from 0.0 to 1.0 based on:
- Direct relevance to the topic
- Quality of methodology (if discernible from abstract)
- Significance of contribution

IMPORTANT: Respond with ONLY valid JSON. No markdown, no explanations outside JSON.
The response must be a JSON object with a "scores" array.

Example:
{"scores":[{"index":1,"score":0.95,"reason":"highly relevant"},{"index":2,"score":0.75,"reason":"somewhat relevant"}]}

Respond with JSON only:`, topic, paperList.String())

	schema := map[string]interface{}{
		"scores": []map[string]interface{}{
			{
				"index":  0,
				"score":  0.0,
				"reason": "",
			},
		},
	}

	result, err := r.structured.Generate(ctx, prompt, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to generate rerank scores: %w", err)
	}

	scores, err := parseRerankResponse(result)
	if err != nil {
		return nil, fmt.Errorf("failed to parse rerank response: %w", err)
	}

	scoreMap := make(map[int]float64)
	for _, s := range scores {
		if s.Index >= 1 && s.Index <= len(papers) {
			scoreMap[s.Index-1] = s.Score
		}
	}

	resultPapers := make([]paperWithScore, len(papers))
	for i, p := range papers {
		resultPapers[i] = paperWithScore{
			paper: p.paper,
			score: p.score,
		}
		if llmScore, ok := scoreMap[i]; ok {
			resultPapers[i].score = 0.3*p.score + 0.7*llmScore
		}
	}

	return resultPapers, nil
}

type scoreEntry struct {
	Index  int     `json:"index"`
	Score  float64 `json:"score"`
	Reason string  `json:"reason"`
}

func parseRerankResponse(result string) ([]scoreEntry, error) {
	cleaned := strings.TrimSpace(result)
	cleaned = strings.TrimPrefix(cleaned, "```json")
	cleaned = strings.TrimPrefix(cleaned, "```")
	cleaned = strings.TrimSuffix(cleaned, "```")
	cleaned = strings.TrimSpace(cleaned)

	var response struct {
		Scores []scoreEntry `json:"scores"`
	}

	if err := json.Unmarshal([]byte(cleaned), &response); err != nil {
		return nil, fmt.Errorf("failed to parse: %w", err)
	}

	if len(response.Scores) == 0 {
		return nil, fmt.Errorf("no scores in response")
	}

	return response.Scores, nil
}

func cosineSimilarity(a, b []float32) float64 {
	if len(a) != len(b) {
		return 0
	}

	var dotProduct, normA, normB float64
	for i := range a {
		dotProduct += float64(a[i]) * float64(b[i])
		normA += float64(a[i]) * float64(a[i])
		normB += float64(b[i]) * float64(b[i])
	}

	if normA == 0 || normB == 0 {
		return 0
	}

	return dotProduct / (sqrt(normA) * sqrt(normB))
}

func sqrt(x float64) float64 {
	if x <= 0 {
		return 0
	}
	z := x
	for i := 0; i < 10; i++ {
		z = z - (z*z-x)/(2*z)
	}
	return z
}
