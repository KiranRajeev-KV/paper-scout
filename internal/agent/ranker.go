package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/paper-scout/internal/llm"
	"github.com/paper-scout/internal/logger"
	"github.com/paper-scout/internal/storage/postgres"
	"github.com/paper-scout/internal/tools/embedding"
)

const (
	RerankBatchSize          = 10
	RerankTopK               = 50
	ChunkTypeAbstract        = "abstract"
	EmbeddingStatusCompleted = "completed"
	EmbeddingStatusFailed    = "failed"
)

type Ranker struct {
	postgres   *postgres.Client
	embedder   *embedding.Generator
	llm        *llm.Client
	structured *llm.StructuredOutput

	generateFn              func(ctx context.Context, text string) ([]float32, error)
	generateBatchFn         func(ctx context.Context, texts []string) ([][]float32, error)
	storeEmbeddingFn        func(ctx context.Context, emb embedding.PaperEmbedding) error
	searchSimilarFn         func(ctx context.Context, vector []float32, limit uint64, topicID string) ([]*embedding.SearchResult, error)
	getPapersByTopicFn      func(ctx context.Context, topicID string) ([]*postgres.Paper, error)
	updateRelevanceScoreFn  func(ctx context.Context, topicID string, paperID uuid.UUID, score float64) error
	updateEmbeddingStatusFn func(ctx context.Context, paperID uuid.UUID, status string) error
}

func NewRanker(pg *postgres.Client, emb *embedding.Generator, llmClient *llm.Client) *Ranker {
	ranker := &Ranker{
		postgres:   pg,
		embedder:   emb,
		llm:        llmClient,
		structured: llm.NewStructuredOutput(llmClient),
	}
	ranker.generateFn = emb.Generate
	ranker.generateBatchFn = emb.GenerateBatch
	ranker.storeEmbeddingFn = emb.StoreEmbedding
	ranker.searchSimilarFn = emb.SearchSimilar
	ranker.getPapersByTopicFn = ranker.getPapersByTopic
	ranker.updateRelevanceScoreFn = ranker.updateRelevanceScore
	ranker.updateEmbeddingStatusFn = ranker.updateEmbeddingStatus
	return ranker
}

type RankedPaper struct {
	ID             string
	Title          string
	Abstract       string
	PDFURL         string
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

	logger.Info().Msg("Embedding topic for Qdrant search")
	topicVector, err := r.generateFn(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("failed to embed topic: %w", err)
	}

	papers, err := r.getPapersByTopicFn(ctx, topicID)
	if err != nil {
		return nil, fmt.Errorf("failed to get papers: %w", err)
	}

	logger.Info().Int("papers", len(papers)).Msg("Papers retrieved for ranking")

	queryablePapers, err := r.ensureEmbeddings(ctx, topicID, papers)
	if err != nil {
		return nil, err
	}
	if len(queryablePapers) == 0 {
		return nil, fmt.Errorf("no papers with embeddings available for ranking")
	}

	queryLimit := rankQueryLimit(len(queryablePapers), maxPapers)
	searchResults, err := r.searchSimilarFn(ctx, topicVector, uint64(queryLimit), topicID)
	if err != nil {
		return nil, fmt.Errorf("failed to search qdrant: %w", err)
	}

	scored := rankSearchResults(searchResults, queryablePapers)
	if len(scored) == 0 {
		return nil, fmt.Errorf("qdrant returned no ranked papers for topic %s", topicID)
	}

	topK := RerankTopK
	if len(scored) < topK {
		topK = len(scored)
	}
	scored = scored[:topK]

	logger.Info().Int("papers", topK).Msg("Top papers selected from Qdrant for LLM reranking")

	if r.llm != nil && len(scored) > 0 {
		reranked, err := r.rerankWithLLM(ctx, topic, scored)
		if err != nil {
			logger.Warn().Err(err).Msg("LLM reranking failed, using Qdrant scores")
		} else {
			scored = reranked
		}
	}

	if maxPapers > 0 && len(scored) > maxPapers {
		scored = scored[:maxPapers]
	}

	logger.Info().Int("papers", len(scored)).Msg("Storing relevance scores")

	ranked := make([]RankedPaper, 0, len(scored))
	for _, s := range scored {
		ranked = append(ranked, RankedPaper{
			ID:             s.paper.ID.String(),
			Title:          s.paper.Title,
			Abstract:       pgTextVal(s.paper.Abstract),
			PDFURL:         pgTextVal(s.paper.PdfUrl),
			RelevanceScore: s.score,
		})

		if err := r.updateRelevanceScoreFn(ctx, topicID, s.paper.ID, s.score); err != nil {
			logger.Warn().Err(err).Str("paper_id", s.paper.ID.String()).Msg("Failed to update relevance score")
		}
	}

	logger.Info().
		Int("ranked_papers", len(ranked)).
		Msg("Paper ranking complete")

	return ranked, nil
}

func (r *Ranker) ensureEmbeddings(ctx context.Context, topicID string, papers []*postgres.Paper) (map[string]*postgres.Paper, error) {
	queryablePapers := make(map[string]*postgres.Paper)
	texts := make([]string, 0, len(papers))
	papersToEmbed := make([]*postgres.Paper, 0, len(papers))

	for _, paper := range papers {
		abstract := pgTextVal(paper.Abstract)
		if abstract == "" {
			logger.Debug().Str("paper_id", paper.ID.String()).Msg("Skipping paper with no abstract")
			continue
		}

		queryablePapers[paper.ID.String()] = paper
		papersToEmbed = append(papersToEmbed, paper)
		texts = append(texts, abstract)
	}

	if len(papersToEmbed) == 0 {
		return queryablePapers, nil
	}

	logger.Info().Int("papers", len(papersToEmbed)).Msg("Generating abstract embeddings for Qdrant")
	vectors, err := r.generateBatchFn(ctx, texts)
	if err != nil {
		return nil, fmt.Errorf("failed to generate paper embeddings: %w", err)
	}
	if len(vectors) != len(papersToEmbed) {
		return nil, fmt.Errorf("embedding vector count mismatch: got %d want %d", len(vectors), len(papersToEmbed))
	}

	for i, paper := range papersToEmbed {
		err := r.storeEmbeddingFn(ctx, embedding.PaperEmbedding{
			PaperID:    paper.ID.String(),
			TopicID:    topicID,
			ChunkType:  ChunkTypeAbstract,
			ChunkIndex: 0,
			Text:       texts[i],
			Vector:     vectors[i],
		})
		if err != nil {
			logger.Warn().Err(err).Str("paper_id", paper.ID.String()).Msg("Failed to store embedding in Qdrant")
			delete(queryablePapers, paper.ID.String())
			_ = r.updateEmbeddingStatusFn(ctx, paper.ID, EmbeddingStatusFailed)
			continue
		}

		if err := r.updateEmbeddingStatusFn(ctx, paper.ID, EmbeddingStatusCompleted); err != nil {
			logger.Warn().Err(err).Str("paper_id", paper.ID.String()).Msg("Failed to update embedding status")
		}
	}

	return queryablePapers, nil
}

func rankSearchResults(results []*embedding.SearchResult, papersByID map[string]*postgres.Paper) []paperWithScore {
	bestByPaper := make(map[string]paperWithScore)

	for _, result := range results {
		paper, ok := papersByID[result.PaperID]
		if !ok {
			continue
		}

		score := float64(result.Score)
		current, exists := bestByPaper[result.PaperID]
		if !exists || score > current.score {
			bestByPaper[result.PaperID] = paperWithScore{
				paper: paper,
				score: score,
			}
		}
	}

	scored := make([]paperWithScore, 0, len(bestByPaper))
	for _, paper := range bestByPaper {
		scored = append(scored, paper)
	}

	sort.Slice(scored, func(i, j int) bool {
		return scored[i].score > scored[j].score
	})

	return scored
}

func rankQueryLimit(totalPapers, maxPapers int) int {
	limit := RerankTopK
	if maxPapers > limit {
		limit = maxPapers
	}
	if totalPapers < limit {
		limit = totalPapers
	}
	if limit < 1 {
		return 1
	}
	return limit
}

func (r *Ranker) rerankWithLLM(ctx context.Context, topic string, papers []paperWithScore) ([]paperWithScore, error) {
	logger.Info().Int("papers", len(papers)).Msg("Starting LLM reranking")

	allScored := make([]paperWithScore, 0, len(papers))
	totalBatches := (len(papers) + RerankBatchSize - 1) / RerankBatchSize

	for i := 0; i < len(papers); i += RerankBatchSize {
		end := i + RerankBatchSize
		if end > len(papers) {
			end = len(papers)
		}
		batch := papers[i:end]
		batchNum := (i / RerankBatchSize) + 1

		logger.Info().
			Int("batch", batchNum).
			Int("total_batches", totalBatches).
			Int("papers_in_batch", len(batch)).
			Msg("Processing rerank batch")

		batchScored, err := r.rerankBatch(ctx, topic, batch)
		if err != nil {
			logger.Warn().Err(err).Int("batch_start", i).Msg("Failed to rerank batch")
			allScored = append(allScored, batch...)
			continue
		}

		allScored = append(allScored, batchScored...)
		logger.Info().Int("batch", batchNum).Msg("Batch reranked successfully")
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

	var response rerankResponse
	err := r.structured.GenerateInto(ctx, prompt, rerankResponse{
		Scores: []scoreEntry{{Index: 1, Score: 0.5, Reason: "reason"}},
	}, &response)
	if err != nil {
		return nil, fmt.Errorf("failed to generate rerank scores: %w", err)
	}

	if err := validateRerankScores(response.Scores, len(papers)); err != nil {
		return nil, fmt.Errorf("failed to parse rerank response: %w", err)
	}

	scoreMap := make(map[int]float64)
	for _, s := range response.Scores {
		scoreMap[s.Index-1] = s.Score
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

func (r *Ranker) getPapersByTopic(ctx context.Context, topicID string) ([]*postgres.Paper, error) {
	return r.postgres.Queries().GetPapersByTopic(ctx, pgUUID(topicID))
}

func (r *Ranker) updateRelevanceScore(ctx context.Context, topicID string, paperID uuid.UUID, score float64) error {
	err := r.postgres.Queries().UpdatePaperRelevanceScore(ctx, postgres.UpdatePaperRelevanceScoreParams{
		TopicID:        pgUUID(topicID),
		PaperID:        paperID,
		RelevanceScore: pgFloat64(score),
	})
	return err
}

func (r *Ranker) updateEmbeddingStatus(ctx context.Context, paperID uuid.UUID, status string) error {
	_, err := r.postgres.Queries().UpdatePaperEmbeddingStatus(ctx, postgres.UpdatePaperEmbeddingStatusParams{
		ID: paperID,
		EmbeddingStatus: pgtype.Text{
			String: status,
			Valid:  status != "",
		},
	})
	return err
}

type scoreEntry struct {
	Index  int     `json:"index"`
	Score  float64 `json:"score"`
	Reason string  `json:"reason"`
}

type rerankResponse struct {
	Scores []scoreEntry `json:"scores"`
}

func parseRerankResponse(result string) ([]scoreEntry, error) {
	cleaned := strings.TrimSpace(result)
	cleaned = strings.TrimPrefix(cleaned, "```json")
	cleaned = strings.TrimPrefix(cleaned, "```")
	cleaned = strings.TrimSuffix(cleaned, "```")
	cleaned = strings.TrimSpace(cleaned)

	var response rerankResponse

	if err := json.Unmarshal([]byte(cleaned), &response); err != nil {
		return nil, fmt.Errorf("failed to parse: %w", err)
	}

	if err := validateRerankScores(response.Scores, 0); err != nil {
		return nil, err
	}

	return response.Scores, nil
}

func validateRerankScores(scores []scoreEntry, paperCount int) error {
	if len(scores) == 0 {
		return fmt.Errorf("no scores in response")
	}
	seen := make(map[int]struct{}, len(scores))
	for _, score := range scores {
		if score.Index < 1 || (paperCount > 0 && score.Index > paperCount) {
			return fmt.Errorf("score index out of range: %d", score.Index)
		}
		if math.IsNaN(score.Score) || math.IsInf(score.Score, 0) || score.Score < 0 || score.Score > 1 {
			return fmt.Errorf("score for index %d is outside [0,1]: %g", score.Index, score.Score)
		}
		if _, ok := seen[score.Index]; ok {
			return fmt.Errorf("duplicate score index: %d", score.Index)
		}
		seen[score.Index] = struct{}{}
	}
	if paperCount > 0 && len(seen) != paperCount {
		return fmt.Errorf("expected one score for each of %d papers, got %d", paperCount, len(seen))
	}
	return nil
}
