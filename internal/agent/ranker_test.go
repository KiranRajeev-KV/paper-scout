package agent

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/paper-scout/internal/storage/postgres"
	"github.com/paper-scout/internal/tools/embedding"
)

func TestRankerRankUsesQdrantSearchResults(t *testing.T) {
	paperA := testPaper("paper-a", "Alpha", "alpha abstract")
	paperB := testPaper("paper-b", "Beta", "beta abstract")

	ranker := &Ranker{llm: nil}

	var stored []embedding.PaperEmbedding
	var searchCalled bool
	updatedScores := make(map[uuid.UUID]float64)

	ranker.generateFn = func(ctx context.Context, text string) ([]float32, error) {
		if text != "test topic" {
			t.Fatalf("unexpected topic text: %q", text)
		}
		return []float32{0.1, 0.2}, nil
	}
	ranker.generateBatchFn = func(ctx context.Context, texts []string) ([][]float32, error) {
		if len(texts) != 2 {
			t.Fatalf("unexpected text count: %d", len(texts))
		}
		return [][]float32{{0.3, 0.4}, {0.5, 0.6}}, nil
	}
	ranker.storeEmbeddingFn = func(ctx context.Context, emb embedding.PaperEmbedding) error {
		stored = append(stored, emb)
		return nil
	}
	ranker.searchSimilarFn = func(ctx context.Context, vector []float32, limit uint64, topicID string) ([]*embedding.SearchResult, error) {
		searchCalled = true
		if topicID != "topic-1" {
			t.Fatalf("unexpected topicID: %s", topicID)
		}
		if limit != 2 {
			t.Fatalf("unexpected limit: %d", limit)
		}
		return []*embedding.SearchResult{
			{PaperID: paperB.ID.String(), Score: 0.92},
			{PaperID: paperA.ID.String(), Score: 0.41},
		}, nil
	}
	ranker.getPapersByTopicFn = func(ctx context.Context, topicID string) ([]*postgres.Paper, error) {
		return []*postgres.Paper{paperA, paperB}, nil
	}
	ranker.updateRelevanceScoreFn = func(ctx context.Context, topicID string, paperID uuid.UUID, score float64) error {
		updatedScores[paperID] = score
		return nil
	}
	ranker.updateEmbeddingStatusFn = func(ctx context.Context, paperID uuid.UUID, status string) error {
		return nil
	}

	ranked, err := ranker.Rank(context.Background(), "topic-1", "test topic", 2)
	if err != nil {
		t.Fatalf("Rank returned error: %v", err)
	}

	if !searchCalled {
		t.Fatal("expected Qdrant search to be called")
	}
	if len(stored) != 2 {
		t.Fatalf("stored %d embeddings, want 2", len(stored))
	}
	if stored[0].ChunkType != ChunkTypeAbstract || stored[1].ChunkType != ChunkTypeAbstract {
		t.Fatalf("unexpected chunk types: %+v", stored)
	}
	if len(ranked) != 2 {
		t.Fatalf("ranked %d papers, want 2", len(ranked))
	}
	if ranked[0].ID != paperB.ID.String() || ranked[1].ID != paperA.ID.String() {
		t.Fatalf("unexpected ranking order: %+v", ranked)
	}
	if !cmp.Equal(updatedScores[paperB.ID], 0.92, cmpopts.EquateApprox(0, 1e-6)) ||
		!cmp.Equal(updatedScores[paperA.ID], 0.41, cmpopts.EquateApprox(0, 1e-6)) {
		t.Fatalf("unexpected stored scores: %+v", updatedScores)
	}
}

func TestRankerRankDeduplicatesQdrantResultsByPaper(t *testing.T) {
	paper := testPaper("paper-a", "Alpha", "alpha abstract")

	ranker := &Ranker{llm: nil}
	ranker.generateFn = func(ctx context.Context, text string) ([]float32, error) {
		return []float32{0.1}, nil
	}
	ranker.generateBatchFn = func(ctx context.Context, texts []string) ([][]float32, error) {
		return [][]float32{{0.2}}, nil
	}
	ranker.storeEmbeddingFn = func(ctx context.Context, emb embedding.PaperEmbedding) error {
		return nil
	}
	ranker.searchSimilarFn = func(ctx context.Context, vector []float32, limit uint64, topicID string) ([]*embedding.SearchResult, error) {
		return []*embedding.SearchResult{
			{PaperID: paper.ID.String(), Score: 0.45},
			{PaperID: paper.ID.String(), Score: 0.88},
		}, nil
	}
	ranker.getPapersByTopicFn = func(ctx context.Context, topicID string) ([]*postgres.Paper, error) {
		return []*postgres.Paper{paper}, nil
	}
	ranker.updateRelevanceScoreFn = func(ctx context.Context, topicID string, paperID uuid.UUID, score float64) error {
		if !cmp.Equal(score, 0.88, cmpopts.EquateApprox(0, 1e-6)) {
			t.Fatalf("unexpected score: %f", score)
		}
		return nil
	}
	ranker.updateEmbeddingStatusFn = func(ctx context.Context, paperID uuid.UUID, status string) error {
		return nil
	}

	ranked, err := ranker.Rank(context.Background(), "topic-1", "test topic", 1)
	if err != nil {
		t.Fatalf("Rank returned error: %v", err)
	}
	if len(ranked) != 1 {
		t.Fatalf("ranked %d papers, want 1", len(ranked))
	}
	if !cmp.Equal(ranked[0].RelevanceScore, 0.88, cmpopts.EquateApprox(0, 1e-6)) {
		t.Fatalf("unexpected relevance score: %f", ranked[0].RelevanceScore)
	}
}

func testPaper(externalID, title, abstract string) *postgres.Paper {
	return &postgres.Paper{
		ID:         uuid.New(),
		ExternalID: externalID,
		Title:      title,
		Abstract: pgtype.Text{
			String: abstract,
			Valid:  true,
		},
	}
}
