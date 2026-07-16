package agent

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/paper-scout/internal/llm"
	"github.com/paper-scout/internal/logger"
	"github.com/paper-scout/internal/storage/postgres"
	"github.com/paper-scout/internal/tools/embedding"
)

const RerankBatchSize = 10

// AbstractIndexer makes papers queryable through durable abstract embeddings.
type AbstractIndexer interface {
	Ensure(context.Context, string, []*postgres.Paper) (map[string]*postgres.Paper, error)
}

type rankingRepository interface {
	Papers(context.Context, string) ([]*postgres.Paper, error)
	StoreScore(context.Context, string, uuid.UUID, float64) error
}

type vectorSearcher interface {
	Generate(context.Context, string) ([]float32, error)
	SearchSimilar(context.Context, []float32, uint64, string) ([]*embedding.SearchResult, error)
}

// Ranker owns vector ranking, optional LLM reranking, and relevance-score persistence.
type Ranker struct {
	papers     rankingRepository
	vectors    vectorSearcher
	llm        llm.Generator
	structured *llm.StructuredOutput
	index      AbstractIndexer
}

type postgresRankingRepository struct{ postgres *postgres.Client }

// NewRanker constructs a ranking service using a durable abstract index.
func NewRanker(pg *postgres.Client, vectors *embedding.Generator, llmClient llm.Generator, index AbstractIndexer) (*Ranker, error) {
	if pg == nil || vectors == nil || index == nil {
		return nil, fmt.Errorf("ranker requires postgres, embedding generator, and abstract index")
	}
	return newRanker(&postgresRankingRepository{postgres: pg}, vectors, llmClient, index), nil
}

func newRanker(papers rankingRepository, vectors vectorSearcher, llmClient llm.Generator, index AbstractIndexer) *Ranker {
	return &Ranker{papers: papers, vectors: vectors, llm: llmClient, structured: llm.NewStructuredOutput(llmClient), index: index}
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

// Rank returns papers ordered by vector relevance and optional LLM reranking.
func (r *Ranker) Rank(ctx context.Context, topicID string, topic string, maxPapers int) ([]RankedPaper, error) {
	logger.From(ctx).Info().Str("topic_id", topicID).Int("topic_chars", len(topic)).Msg("Starting paper ranking")
	topicVector, err := r.vectors.Generate(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("embed topic: %w", err)
	}
	papers, err := r.papers.Papers(ctx, topicID)
	if err != nil {
		return nil, fmt.Errorf("load papers for ranking: %w", err)
	}
	queryablePapers, err := r.index.Ensure(ctx, topicID, papers)
	if err != nil {
		return nil, fmt.Errorf("ensure abstract embeddings: %w", err)
	}
	if len(queryablePapers) == 0 {
		return nil, fmt.Errorf("no papers with embeddings available for ranking")
	}
	searchResults, err := r.vectors.SearchSimilar(ctx, topicVector, uint64(rankQueryLimit(len(queryablePapers), maxPapers)), topicID)
	if err != nil {
		return nil, fmt.Errorf("search qdrant: %w", err)
	}
	scored := rankSearchResults(searchResults, queryablePapers)
	if len(scored) == 0 {
		return nil, fmt.Errorf("qdrant returned no ranked papers for topic %s", topicID)
	}
	if r.llm != nil {
		scored, err = r.rerankWithLLM(ctx, topic, scored)
		if err != nil {
			return nil, fmt.Errorf("LLM reranking failed: %w", err)
		}
	}
	if maxPapers > 0 && len(scored) > maxPapers {
		scored = scored[:maxPapers]
	}

	ranked := make([]RankedPaper, 0, len(scored))
	var persistenceFailures []ItemFailure
	for _, scoredPaper := range scored {
		ranked = append(ranked, RankedPaper{
			ID: scoredPaper.paper.ID.String(), Title: scoredPaper.paper.Title, Abstract: pgTextVal(scoredPaper.paper.Abstract),
			PDFURL: pgTextVal(scoredPaper.paper.PdfUrl), RelevanceScore: scoredPaper.score,
		})
		if err := r.papers.StoreScore(ctx, topicID, scoredPaper.paper.ID, scoredPaper.score); err != nil {
			persistenceFailures = append(persistenceFailures, ItemFailure{Kind: "paper", Identifier: scoredPaper.paper.ID.String(), Err: err})
		}
	}
	if err := newBatchError("ranking persistence", len(scored), persistenceFailures); err != nil {
		return nil, err
	}
	logger.From(ctx).Info().Int("ranked_papers", len(ranked)).Msg("Paper ranking complete")
	return ranked, nil
}

func (r *postgresRankingRepository) Papers(ctx context.Context, topicID string) ([]*postgres.Paper, error) {
	id, err := parseID("topic ID", topicID)
	if err != nil {
		return nil, err
	}
	return r.postgres.Queries().GetPapersByTopic(ctx, id)
}

func (r *postgresRankingRepository) StoreScore(ctx context.Context, topicID string, paperID uuid.UUID, score float64) error {
	id, err := parseID("topic ID", topicID)
	if err != nil {
		return err
	}
	err = r.postgres.Queries().UpdatePaperRelevanceScore(ctx, postgres.UpdatePaperRelevanceScoreParams{
		TopicID: id, PaperID: paperID, RelevanceScore: pgFloat64(score),
	})
	return err
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
			bestByPaper[result.PaperID] = paperWithScore{paper: paper, score: score}
		}
	}
	scored := make([]paperWithScore, 0, len(bestByPaper))
	for _, paper := range bestByPaper {
		scored = append(scored, paper)
	}
	sort.Slice(scored, func(i, j int) bool { return scored[i].score > scored[j].score })
	return scored
}

func rankQueryLimit(totalPapers, maxPapers int) int {
	limit := totalPapers
	if maxPapers > 0 && maxPapers < limit {
		limit = maxPapers
	}
	if limit < 1 {
		return 1
	}
	return limit
}

func (r *Ranker) rerankWithLLM(ctx context.Context, topic string, papers []paperWithScore) ([]paperWithScore, error) {
	allScored := make([]paperWithScore, 0, len(papers))
	for start := 0; start < len(papers); start += RerankBatchSize {
		end := min(start+RerankBatchSize, len(papers))
		batch, err := r.rerankBatch(ctx, topic, papers[start:end])
		if err != nil {
			logger.From(ctx).Warn().Err(err).Int("batch_start", start).Msg("Failed to rerank batch")
			allScored = append(allScored, papers[start:end]...)
			continue
		}
		allScored = append(allScored, batch...)
	}
	sort.Slice(allScored, func(i, j int) bool { return allScored[i].score > allScored[j].score })
	return allScored, nil
}

func (r *Ranker) rerankBatch(ctx context.Context, topic string, papers []paperWithScore) ([]paperWithScore, error) {
	var paperList strings.Builder
	for i, paper := range papers {
		paperList.WriteString(fmt.Sprintf("\n[%d] Title: %s\n    Abstract: %s", i+1, paper.paper.Title, truncateText(pgTextVal(paper.paper.Abstract), 500)))
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
	if err := r.structured.GenerateInto(ctx, prompt, rerankResponse{Scores: []scoreEntry{{Index: 1, Score: 0.5, Reason: "reason"}}}, &response); err != nil {
		return nil, fmt.Errorf("generate rerank scores: %w", err)
	}
	if err := validateRerankScores(response.Scores, len(papers)); err != nil {
		return nil, fmt.Errorf("parse rerank response: %w", err)
	}
	scoreMap := make(map[int]float64, len(response.Scores))
	for _, score := range response.Scores {
		scoreMap[score.Index-1] = score.Score
	}
	result := make([]paperWithScore, len(papers))
	for i, paper := range papers {
		result[i] = paperWithScore{paper: paper.paper, score: paper.score}
		if score, ok := scoreMap[i]; ok {
			result[i].score = 0.3*paper.score + 0.7*score
		}
	}
	return result, nil
}

type scoreEntry struct {
	Index  int     `json:"index"`
	Score  float64 `json:"score"`
	Reason string  `json:"reason"`
}

type rerankResponse struct {
	Scores []scoreEntry `json:"scores"`
}

func validateRerankScores(scores []scoreEntry, paperCount int) error {
	if len(scores) == 0 {
		return fmt.Errorf("no scores in response")
	}
	seen := make(map[int]struct{}, len(scores))
	for _, score := range scores {
		if score.Index < 1 || paperCount > 0 && score.Index > paperCount {
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
