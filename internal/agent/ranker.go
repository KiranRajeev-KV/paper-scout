package agent

import (
	"context"
	"fmt"
	"sort"

	"github.com/research-agent/internal/logger"
	"github.com/research-agent/internal/storage/postgres"
	"github.com/research-agent/internal/tools/embedding"
)

type Ranker struct {
	postgres *postgres.Client
	embedder *embedding.Generator
}

func NewRanker(pg *postgres.Client, emb *embedding.Generator) *Ranker {
	return &Ranker{
		postgres: pg,
		embedder: emb,
	}
}

type RankedPaper struct {
	ID             string
	Title          string
	Abstract       string
	RelevanceScore float64
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

	type paperWithScore struct {
		paper *postgres.Paper
		score float64
	}

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
