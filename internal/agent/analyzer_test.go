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

// Protects analyzer analyze waits for worker completion.
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
	if err := pool.Start(); err != nil {
		t.Fatal(err)
	}
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

// Protects analysis jobs from consuming their timeout while queued behind another LLM request.
func TestAnalyzerAnalyzeSerializesLLMJobs(t *testing.T) {
	pool := worker.NewPool(3, 8)
	analyzer := NewAnalyzer(nil, nil, pool)

	var mu sync.Mutex
	active := 0
	peak := 0
	analyzer.analyzeFn = func(context.Context, string, string, string) (*PaperAnalysis, error) {
		mu.Lock()
		active++
		if active > peak {
			peak = active
		}
		mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		mu.Lock()
		active--
		mu.Unlock()
		return &PaperAnalysis{ProblemStatement: "ok"}, nil
	}
	analyzer.storeFn = func(context.Context, string, string, *PaperAnalysis) error { return nil }

	pool.SetHandler(analyzer.HandleJob)
	pool.SetCompletionHook(analyzer.HandleJobCompletion)
	if err := pool.Start(); err != nil {
		t.Fatal(err)
	}
	defer pool.Stop()

	err := analyzer.Analyze(context.Background(), "topic-serial", []AnalysisPaper{
		{ID: "paper-1", Abstract: "one"},
		{ID: "paper-2", Abstract: "two"},
		{ID: "paper-3", Abstract: "three"},
	}, nil)
	if err != nil {
		t.Fatalf("Analyze returned error: %v", err)
	}
	if peak != 1 {
		t.Fatalf("peak active analyses = %d, want 1", peak)
	}
}

// Protects analyzer analyze returns typed batch error after paper failures.
func TestAnalyzerAnalyzeReturnsTypedBatchErrorAfterPaperFailures(t *testing.T) {
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
	if err := pool.Start(); err != nil {
		t.Fatal(err)
	}
	defer pool.Stop()

	papers := []AnalysisPaper{
		{ID: "paper-ok", Abstract: "ok"},
		{ID: "paper-fail", Abstract: "fail"},
	}

	err := analyzer.Analyze(context.Background(), "topic-2", papers, func(completed, total int) {
		mu.Lock()
		defer mu.Unlock()
		progress = append(progress, completed)
	})
	var batchErr *BatchError
	if !errors.As(err, &batchErr) {
		t.Fatalf("Analyze error = %v, want *BatchError", err)
	}
	if batchErr.Total != 2 || batchErr.Succeeded != 1 || len(batchErr.Failures) != 1 {
		t.Fatalf("batch error = %#v, want 1 of 2 successful with one failure", batchErr)
	}
	if batchErr.Failures[0].Identifier != "paper-fail" || !strings.Contains(batchErr.Error(), "analysis failed") {
		t.Fatalf("batch failure = %#v, want paper-fail cause", batchErr.Failures[0])
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

// Protects analyzer analyze empty batch returns immediately.
func TestAnalyzerAnalyzeEmptyBatchReturnsImmediately(t *testing.T) {
	pool := worker.NewPool(1, 1)
	analyzer := NewAnalyzer(nil, nil, pool)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := analyzer.Analyze(ctx, "topic-empty", nil, nil); err != nil {
		t.Fatalf("Analyze returned error for empty batch: %v", err)
	}
}

// Protects multiple analysis batches.
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
	if err := pool.Start(); err != nil {
		t.Fatal(err)
	}
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

// Protects analyzer handle job uses payload metadata.
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

// Protects analyzer analyze sync uses abstract only.
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

// Protects validate paper analysis response rejects malformed output.
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

// Protects analysis validation accepts long fields but bounds the complete payload.
func TestValidatePaperAnalysisResponseBoundsOverallPayload(t *testing.T) {
	response := paperAnalysisResponse{
		ProblemStatement:  stringPtr("problem"),
		Methodology:       stringPtr("method"),
		Dataset:           stringPtr("dataset"),
		EvaluationMetrics: &[]string{"accuracy"},
		KeyFindings:       stringPtr("finding"),
		Limitations:       stringPtr(strings.Repeat("x", 81)),
		FutureWork:        stringPtr("future"),
	}
	if _, err := validatePaperAnalysisResponse(response); err != nil {
		t.Fatalf("validation rejected a long limitations field: %v", err)
	}

	response.Limitations = stringPtr(strings.Repeat("x", maxPaperAnalysisBytes))
	if _, err := validatePaperAnalysisResponse(response); err == nil {
		t.Fatal("validation accepted an oversized analysis payload")
	}
}
