package agent

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestDiscoverSearchesSourcesConcurrently(t *testing.T) {
	discoverer := &PaperDiscoverer{maxPapers: 10}
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var keywordsSeen []string
	var keywordsMu sync.Mutex
	discoverer.searchSemanticScholarFn = func(ctx context.Context, query string, limit int) ([]DiscoveredPaper, error) {
		started <- struct{}{}
		<-release
		return []DiscoveredPaper{{Source: "semantic_scholar", ExternalID: "ss-1", Title: "Semantic paper"}}, nil
	}
	discoverer.searchArXivFn = func(ctx context.Context, query string, limit int, keywords []string) ([]DiscoveredPaper, error) {
		keywordsMu.Lock()
		keywordsSeen = append(keywordsSeen, keywords...)
		keywordsMu.Unlock()
		started <- struct{}{}
		<-release
		return []DiscoveredPaper{{Source: "arxiv", ExternalID: "2401.00001", ArXivID: "2401.00001", Title: "ArXiv paper"}}, nil
	}
	discoverer.storePaperFn = func(context.Context, string, DiscoveredPaper) error { return nil }

	resultCh := make(chan []DiscoveredPaper, 1)
	errCh := make(chan error, 1)
	go func() {
		papers, err := discoverer.Discover(context.Background(), "topic-1", []string{"query"}, []string{"keyword"})
		resultCh <- papers
		errCh <- err
	}()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first source did not start")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("second source did not start concurrently")
	}
	close(release)

	if err := <-errCh; err != nil {
		t.Fatalf("Discover returned error: %v", err)
	}
	if got := len(<-resultCh); got != 2 {
		t.Fatalf("discovered %d papers, want 2", got)
	}
	keywordsMu.Lock()
	defer keywordsMu.Unlock()
	if len(keywordsSeen) != 1 || keywordsSeen[0] != "keyword" {
		t.Fatalf("arXiv keywords = %v, want [keyword]", keywordsSeen)
	}
}

func TestDiscoverBoundsPerQuerySearchLimit(t *testing.T) {
	discoverer := &PaperDiscoverer{maxPapers: 10}
	var mu sync.Mutex
	var actual []int
	discoverer.searchSemanticScholarFn = func(_ context.Context, _ string, limit int) ([]DiscoveredPaper, error) {
		mu.Lock()
		actual = append(actual, limit)
		mu.Unlock()
		return nil, nil
	}
	discoverer.searchArXivFn = func(_ context.Context, _ string, limit int, _ []string) ([]DiscoveredPaper, error) {
		mu.Lock()
		actual = append(actual, limit)
		mu.Unlock()
		return nil, nil
	}
	discoverer.storePaperFn = func(context.Context, string, DiscoveredPaper) error { return nil }

	if _, err := discoverer.Discover(context.Background(), "topic-1", []string{"q1", "q2", "q3"}, nil); err != nil {
		t.Fatalf("Discover returned error: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(actual) != 6 {
		t.Fatalf("search calls = %d, want 6", len(actual))
	}
	for _, limit := range actual {
		if limit < 3 || limit > 4 {
			t.Fatalf("per-query limit = %d, want 3 or 4", limit)
		}
	}
}

func TestDiscoverDeduplicatesCrossSourcePaper(t *testing.T) {
	discoverer := &PaperDiscoverer{maxPapers: 10}
	discoverer.searchSemanticScholarFn = func(context.Context, string, int) ([]DiscoveredPaper, error) {
		return []DiscoveredPaper{{
			ID:         "ss-1",
			Source:     "semantic_scholar",
			ExternalID: "ss-1",
			Title:      "A Study on Retrieval",
			Year:       2024,
			DOI:        "10.1234/ABC",
			Abstract:   "abstract",
		}}, nil
	}
	discoverer.searchArXivFn = func(context.Context, string, int, []string) ([]DiscoveredPaper, error) {
		return []DiscoveredPaper{{
			Source:     "arxiv",
			ExternalID: "2401.00001v2",
			ArXivID:    "2401.00001v2",
			Title:      "A Study on Retrieval!",
			Year:       2024,
			DOI:        "https://doi.org/10.1234/abc",
			PDFURL:     "https://arxiv.org/pdf/2401.00001.pdf",
		}}, nil
	}
	discoverer.storePaperFn = func(context.Context, string, DiscoveredPaper) error { return nil }

	papers, err := discoverer.Discover(context.Background(), "topic-1", []string{"query"}, nil)
	if err != nil {
		t.Fatalf("Discover returned error: %v", err)
	}
	if len(papers) != 1 {
		t.Fatalf("discovered %d papers, want 1", len(papers))
	}
	if papers[0].Source != "semantic_scholar" || papers[0].ID != "ss-1" {
		t.Fatalf("unexpected primary paper: %+v", papers[0])
	}
	if papers[0].PDFURL != "https://arxiv.org/pdf/2401.00001.pdf" {
		t.Fatalf("arXiv metadata was not merged: %+v", papers[0])
	}
}

func TestDiscoverDeduplicatesByNormalizedTitleAndYear(t *testing.T) {
	results := []discoverySearchResult{
		{
			source: "semantic_scholar",
			papers: []DiscoveredPaper{{Source: "semantic_scholar", ExternalID: "ss-1", Title: "Neural Methods: A Study", Year: 2023, Authors: []DiscoveredAuthor{{Name: "Ada Lovelace"}}}},
		},
		{
			source: "arxiv",
			papers: []DiscoveredPaper{{Source: "arxiv", ExternalID: "2401.00001", ArXivID: "2401.00001", Title: " neural methods a study ", Year: 2023, Authors: []DiscoveredAuthor{{Name: "Ada Lovelace"}}}},
		},
	}

	if got := len(reconcileDiscoveredPapers(results)); got != 1 {
		t.Fatalf("reconciled %d papers, want 1", got)
	}
}

func TestDiscoverPreservesSameTitleAndYearWithoutCorroboration(t *testing.T) {
	results := []discoverySearchResult{
		{source: "semantic_scholar", papers: []DiscoveredPaper{{Source: "semantic_scholar", ExternalID: "ss-1", Title: "Common Title", Year: 2023}}},
		{source: "arxiv", papers: []DiscoveredPaper{{Source: "arxiv", ExternalID: "2401.00001", ArXivID: "2401.00001", Title: "Common Title", Year: 2023}}},
	}

	if got := len(reconcileDiscoveredPapers(results)); got != 2 {
		t.Fatalf("reconciled %d papers, want 2 distinct papers", got)
	}
}

func TestDiscoverReturnsPartialResultsAndFailsWhenAllSearchesFail(t *testing.T) {
	discoverer := &PaperDiscoverer{maxPapers: 10}
	discoverer.searchSemanticScholarFn = func(context.Context, string, int) ([]DiscoveredPaper, error) {
		return []DiscoveredPaper{{Source: "semantic_scholar", ExternalID: "ss-1", Title: "Available"}}, nil
	}
	discoverer.searchArXivFn = func(context.Context, string, int, []string) ([]DiscoveredPaper, error) {
		return nil, errors.New("arXiv unavailable")
	}
	discoverer.storePaperFn = func(context.Context, string, DiscoveredPaper) error { return nil }

	papers, err := discoverer.Discover(context.Background(), "topic-1", []string{"query"}, nil)
	if err != nil || len(papers) != 1 {
		t.Fatalf("partial discovery = papers %d, err %v; want one paper and nil error", len(papers), err)
	}

	discoverer.searchSemanticScholarFn = func(context.Context, string, int) ([]DiscoveredPaper, error) {
		return nil, errors.New("Semantic Scholar unavailable")
	}
	if _, err := discoverer.Discover(context.Background(), "topic-1", []string{"query"}, nil); err == nil {
		t.Fatal("all-failed discovery returned nil error")
	}
}
