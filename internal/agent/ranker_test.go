package agent

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/paper-scout/internal/storage/postgres"
	"github.com/paper-scout/internal/tools/embedding"
)

type rankRepositoryFake struct {
	papers []*postgres.Paper
	scores map[uuid.UUID]float64
}

func (f *rankRepositoryFake) Papers(context.Context, string) ([]*postgres.Paper, error) {
	return f.papers, nil
}
func (f *rankRepositoryFake) StoreScore(_ context.Context, _ string, paperID uuid.UUID, score float64) error {
	f.scores[paperID] = score
	return nil
}

type vectorSearcherFake struct {
	results []*embedding.SearchResult
	limit   uint64
}

func (f *vectorSearcherFake) Generate(context.Context, string) ([]float32, error) {
	return []float32{.1, .2}, nil
}
func (f *vectorSearcherFake) SearchSimilar(_ context.Context, _ []float32, limit uint64, _ string) ([]*embedding.SearchResult, error) {
	f.limit = limit
	return f.results, nil
}

type abstractIndexerFunc func(context.Context, string, []*postgres.Paper) (map[string]*postgres.Paper, error)

func (f abstractIndexerFunc) Ensure(ctx context.Context, topicID string, papers []*postgres.Paper) (map[string]*postgres.Paper, error) {
	return f(ctx, topicID, papers)
}

// Protects ranking delegates abstract embedding lifecycle to its index dependency.
func TestRankerDelegatesAbstractIndexLifecycle(t *testing.T) {
	paper := testPaper("paper-a", "Alpha", "alpha abstract")
	repository := &rankRepositoryFake{papers: []*postgres.Paper{paper}, scores: make(map[uuid.UUID]float64)}
	vectors := &vectorSearcherFake{results: []*embedding.SearchResult{{PaperID: paper.ID.String(), Score: .8}}}
	called := false
	ranker := newRanker(repository, vectors, nil, abstractIndexerFunc(func(_ context.Context, topicID string, papers []*postgres.Paper) (map[string]*postgres.Paper, error) {
		called = true
		if topicID != "topic-1" || len(papers) != 1 {
			t.Fatalf("index input = %q/%d, want topic-1/1", topicID, len(papers))
		}
		return map[string]*postgres.Paper{paper.ID.String(): paper}, nil
	}))
	if _, err := ranker.Rank(context.Background(), "topic-1", "test topic", 1); err != nil {
		t.Fatalf("Rank returned error: %v", err)
	}
	if !called {
		t.Fatal("expected ranker to delegate abstract indexing")
	}
}

// Protects ranker rank uses Qdrant search results.
func TestRankerRankUsesQdrantSearchResults(t *testing.T) {
	paperA := testPaper("paper-a", "Alpha", "alpha abstract")
	paperB := testPaper("paper-b", "Beta", "beta abstract")
	repository := &rankRepositoryFake{papers: []*postgres.Paper{paperA, paperB}, scores: make(map[uuid.UUID]float64)}
	vectors := &vectorSearcherFake{results: []*embedding.SearchResult{{PaperID: paperB.ID.String(), Score: .92}, {PaperID: paperA.ID.String(), Score: .41}}}
	ranker := newRanker(repository, vectors, nil, abstractIndexerFunc(func(_ context.Context, _ string, papers []*postgres.Paper) (map[string]*postgres.Paper, error) {
		return map[string]*postgres.Paper{paperA.ID.String(): papers[0], paperB.ID.String(): papers[1]}, nil
	}))
	ranked, err := ranker.Rank(context.Background(), "topic-1", "test topic", 2)
	if err != nil {
		t.Fatalf("Rank returned error: %v", err)
	}
	if vectors.limit != 2 {
		t.Fatalf("search limit = %d, want 2", vectors.limit)
	}
	if len(ranked) != 2 || ranked[0].ID != paperB.ID.String() || ranked[1].ID != paperA.ID.String() {
		t.Fatalf("unexpected ranking order: %+v", ranked)
	}
	if !cmp.Equal(repository.scores[paperB.ID], .92, cmpopts.EquateApprox(0, 1e-6)) || !cmp.Equal(repository.scores[paperA.ID], .41, cmpopts.EquateApprox(0, 1e-6)) {
		t.Fatalf("unexpected stored scores: %+v", repository.scores)
	}
}

// Protects validate rerank scores rejects malformed batch.
func TestValidateRerankScoresRejectsMalformedBatch(t *testing.T) {
	for name, scores := range map[string][]scoreEntry{"out of range index": {{Index: 3, Score: .5}}, "incomplete": {{Index: 1, Score: .5}}, "nan": {{Index: 1, Score: math.NaN()}}, "infinity": {{Index: 1, Score: math.Inf(1)}}} {
		t.Run(name, func(t *testing.T) {
			if err := validateRerankScores(scores, 2); err == nil {
				t.Fatal("validation accepted malformed score batch")
			}
		})
	}
}

// Protects ranker rank deduplicates Qdrant results by paper.
func TestRankerRankDeduplicatesQdrantResultsByPaper(t *testing.T) {
	paper := testPaper("paper-a", "Alpha", "alpha abstract")
	repository := &rankRepositoryFake{papers: []*postgres.Paper{paper}, scores: make(map[uuid.UUID]float64)}
	vectors := &vectorSearcherFake{results: []*embedding.SearchResult{{PaperID: paper.ID.String(), Score: .45}, {PaperID: paper.ID.String(), Score: .88}}}
	ranker := newRanker(repository, vectors, nil, abstractIndexerFunc(func(_ context.Context, _ string, _ []*postgres.Paper) (map[string]*postgres.Paper, error) {
		return map[string]*postgres.Paper{paper.ID.String(): paper}, nil
	}))
	ranked, err := ranker.Rank(context.Background(), "topic-1", "test topic", 1)
	if err != nil {
		t.Fatalf("Rank returned error: %v", err)
	}
	if len(ranked) != 1 || !cmp.Equal(ranked[0].RelevanceScore, .88, cmpopts.EquateApprox(0, 1e-6)) {
		t.Fatalf("unexpected ranking: %+v", ranked)
	}
}

// Protects ranking surfaces durable abstract-index failures.
func TestRankerReturnsAbstractIndexFailure(t *testing.T) {
	paper := testPaper("paper-a", "Alpha", "alpha abstract")
	repository := &rankRepositoryFake{papers: []*postgres.Paper{paper}, scores: make(map[uuid.UUID]float64)}
	ranker := newRanker(repository, &vectorSearcherFake{}, nil, abstractIndexerFunc(func(context.Context, string, []*postgres.Paper) (map[string]*postgres.Paper, error) {
		return nil, errors.New("qdrant unavailable")
	}))
	if _, err := ranker.Rank(context.Background(), "topic-1", "test topic", 1); err == nil {
		t.Fatal("Rank accepted abstract-index failure")
	}
}

// Protects ranking from applying a fixed rerank ceiling below the configured paper limit.
func TestRankQueryLimitUsesConfiguredLimitOnly(t *testing.T) {
	if got := rankQueryLimit(75, 0); got != 75 {
		t.Fatalf("unbounded query limit = %d, want 75", got)
	}
	if got := rankQueryLimit(75, 60); got != 60 {
		t.Fatalf("configured query limit = %d, want 60", got)
	}
}

func testPaper(externalID, title, abstract string) *postgres.Paper {
	return &postgres.Paper{ID: uuid.New(), ExternalID: externalID, Title: title, Abstract: pgtype.Text{String: abstract, Valid: true}}
}
