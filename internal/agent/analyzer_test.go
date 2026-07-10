package agent

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/paper-scout/internal/worker"
)

func TestAnalyzerAnalyzeWaitsForWorkerCompletion(t *testing.T) {
	pool := worker.NewPool(2, 8)
	analyzer := NewAnalyzer(nil, nil, pool)

	var mu sync.Mutex
	stored := make([]string, 0, 3)
	progress := make([]int, 0, 3)

	analyzer.analyzeFn = func(ctx context.Context, paperID, abstract, pdfURL string) (*PaperAnalysis, error) {
		time.Sleep(20 * time.Millisecond)
		return &PaperAnalysis{ProblemStatement: abstract, Dataset: pdfURL}, nil
	}
	analyzer.storeFn = func(ctx context.Context, topicID, paperID string, analysis *PaperAnalysis) error {
		mu.Lock()
		defer mu.Unlock()
		stored = append(stored, paperID)
		return nil
	}

	pool.SetHandler(analyzer.HandleJob)
	pool.SetCompletionHook(analyzer.HandleJobCompletion)
	pool.Start()
	defer pool.Stop()

	papers := []AnalysisPaper{
		{ID: "paper-1", Abstract: "alpha", PDFURL: "pdf-1"},
		{ID: "paper-2", Abstract: "beta", PDFURL: "pdf-2"},
		{ID: "paper-3", Abstract: "gamma", PDFURL: "pdf-3"},
	}

	if err := analyzer.Analyze(context.Background(), "topic-1", papers, func(completed, total int) {
		mu.Lock()
		defer mu.Unlock()
		progress = append(progress, completed)
	}); err != nil {
		t.Fatalf("Analyze returned error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(stored) != len(papers) {
		t.Fatalf("stored %d analyses, want %d", len(stored), len(papers))
	}

	if len(progress) == 0 || progress[len(progress)-1] != len(papers) {
		t.Fatalf("progress = %v, want final completion %d", progress, len(papers))
	}
}

func TestAnalyzerAnalyzeContinuesOnPaperFailures(t *testing.T) {
	pool := worker.NewPool(1, 4)
	analyzer := NewAnalyzer(nil, nil, pool)

	var mu sync.Mutex
	stored := make([]string, 0, 1)
	progress := make([]int, 0, 2)

	analyzer.analyzeFn = func(ctx context.Context, paperID, abstract, pdfURL string) (*PaperAnalysis, error) {
		if paperID == "paper-fail" {
			return nil, errors.New("analysis failed")
		}
		return &PaperAnalysis{ProblemStatement: abstract}, nil
	}
	analyzer.storeFn = func(ctx context.Context, topicID, paperID string, analysis *PaperAnalysis) error {
		mu.Lock()
		defer mu.Unlock()
		stored = append(stored, paperID)
		return nil
	}

	pool.SetHandler(analyzer.HandleJob)
	pool.SetCompletionHook(analyzer.HandleJobCompletion)
	pool.Start()
	defer pool.Stop()

	papers := []AnalysisPaper{
		{ID: "paper-ok", Abstract: "ok"},
		{ID: "paper-fail", Abstract: "fail"},
	}

	if err := analyzer.Analyze(context.Background(), "topic-2", papers, func(completed, total int) {
		mu.Lock()
		defer mu.Unlock()
		progress = append(progress, completed)
	}); err != nil {
		t.Fatalf("Analyze returned error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(stored) != 1 || stored[0] != "paper-ok" {
		t.Fatalf("stored = %v, want only successful paper", stored)
	}

	if len(progress) == 0 || progress[len(progress)-1] != len(papers) {
		t.Fatalf("progress = %v, want final completion %d", progress, len(papers))
	}
}

func TestMultipleAnalysisBatches(t *testing.T) {
	pool := worker.NewPool(2, 8)
	analyzer := NewAnalyzer(nil, nil, pool)
	analyzer.analyzeFn = func(context.Context, string, string, string) (*PaperAnalysis, error) {
		time.Sleep(5 * time.Millisecond)
		return &PaperAnalysis{ProblemStatement: "ok"}, nil
	}
	analyzer.storeFn = func(context.Context, string, string, *PaperAnalysis) error { return nil }
	pool.SetHandler(analyzer.HandleJob)
	pool.SetCompletionHook(analyzer.HandleJobCompletion)
	pool.Start()
	defer pool.Stop()

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, topicID := range []string{"topic-a", "topic-b"} {
		wg.Add(1)
		go func(topicID string) {
			defer wg.Done()
			errs <- analyzer.Analyze(context.Background(), topicID, []AnalysisPaper{
				{ID: topicID + "-1", Abstract: "one"},
				{ID: topicID + "-2", Abstract: "two"},
			}, nil)
		}(topicID)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("Analyze returned error: %v", err)
		}
	}

	analyzer.mu.Lock()
	defer analyzer.mu.Unlock()
	if len(analyzer.batches) != 0 || len(analyzer.jobToBatch) != 0 {
		t.Fatalf("unfinished analysis state: batches=%d jobs=%d", len(analyzer.batches), len(analyzer.jobToBatch))
	}
}

func TestAnalyzerHandleJobUsesPayloadMetadata(t *testing.T) {
	analyzer := NewAnalyzer(nil, nil, nil)

	var gotPaperID string
	var gotAbstract string
	var gotPDFURL string

	analyzer.analyzeFn = func(ctx context.Context, paperID, abstract, pdfURL string) (*PaperAnalysis, error) {
		gotPaperID = paperID
		gotAbstract = abstract
		gotPDFURL = pdfURL
		return &PaperAnalysis{}, nil
	}
	analyzer.storeFn = func(ctx context.Context, topicID, paperID string, analysis *PaperAnalysis) error {
		return nil
	}

	job := worker.NewJob(worker.TypePaperAnalysis, map[string]interface{}{
		"paper_id": "paper-123",
		"topic_id": "topic-123",
		"abstract": "short abstract",
		"pdf_url":  "https://example.com/paper.pdf",
	})

	if err := analyzer.HandleJob(context.Background(), job); err != nil {
		t.Fatalf("HandleJob returned error: %v", err)
	}

	if gotPaperID != "paper-123" || gotAbstract != "short abstract" || gotPDFURL != "https://example.com/paper.pdf" {
		t.Fatalf("got (%q, %q, %q)", gotPaperID, gotAbstract, gotPDFURL)
	}
}

func TestAnalyzerAnalyzeSyncUsesAbstractOnly(t *testing.T) {
	analyzer := NewAnalyzer(nil, nil, nil)

	var prompt string
	analyzer.generateFn = func(ctx context.Context, got string) (string, error) {
		prompt = got
		return validAnalysisResponse(), nil
	}

	analysis, err := analyzer.AnalyzeSync(context.Background(), "paper-1", "ABSTRACT_ONLY_MARKER", "http://pdf.test/paper.pdf")
	if err != nil {
		t.Fatalf("AnalyzeSync returned error: %v", err)
	}
	if analysis == nil {
		t.Fatal("expected analysis")
	}
	if !strings.Contains(prompt, "ABSTRACT_ONLY_MARKER") {
		t.Fatalf("prompt did not include abstract fallback: %q", prompt)
	}
}

func validAnalysisResponse() string {
	return `{"problem_statement":"problem","methodology":"method","dataset":"dataset","evaluation_metrics":["accuracy","f1"],"key_findings":"finding","limitations":"limitation","future_work":"future work"}`
}

func TestValidatePaperAnalysisResponseRejectsMalformedOutput(t *testing.T) {
	valid := func() paperAnalysisResponse {
		return paperAnalysisResponse{
			ProblemStatement:  stringPtr("problem"),
			Methodology:       stringPtr("method"),
			Dataset:           stringPtr("dataset"),
			EvaluationMetrics: &[]string{"accuracy"},
			KeyFindings:       stringPtr("finding"),
			Limitations:       stringPtr("limitation"),
			FutureWork:        stringPtr("future"),
		}
	}

	for name, mutate := range map[string]func(*paperAnalysisResponse){
		"missing required field": func(response *paperAnalysisResponse) { response.Methodology = nil },
		"empty required field":   func(response *paperAnalysisResponse) { response.Methodology = stringPtr(" ") },
		"missing metrics":        func(response *paperAnalysisResponse) { response.EvaluationMetrics = nil },
		"empty metric":           func(response *paperAnalysisResponse) { response.EvaluationMetrics = &[]string{""} },
		"oversized field":        func(response *paperAnalysisResponse) { response.Dataset = stringPtr(strings.Repeat("界", 51)) },
	} {
		t.Run(name, func(t *testing.T) {
			response := valid()
			mutate(&response)
			if _, err := validatePaperAnalysisResponse(response); err == nil {
				t.Fatal("validation accepted malformed analysis")
			}
		})
	}
}
