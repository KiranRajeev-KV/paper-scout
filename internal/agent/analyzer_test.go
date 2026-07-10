package agent

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/paper-scout/internal/tools/pdf"
	"github.com/paper-scout/internal/worker"
)

func TestAnalyzerAnalyzeWaitsForWorkerCompletion(t *testing.T) {
	pool := worker.NewPool(2, 8)
	analyzer := NewAnalyzer(nil, nil, nil, nil, pool)

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
	analyzer := NewAnalyzer(nil, nil, nil, nil, pool)

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

func TestAnalyzerHandleJobUsesPayloadMetadata(t *testing.T) {
	analyzer := NewAnalyzer(nil, nil, nil, nil, nil)

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

func TestAnalyzerAnalyzeSyncUsesParsedPDFTextWhenPDFURLPresent(t *testing.T) {
	pdfClient := pdf.NewDownloader(time.Second)
	pdfClient.SetHTTPClient(newFakeHTTPClient(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet {
			t.Fatalf("unexpected method: %s", r.Method)
		}
		return textResponse(http.StatusOK, "%PDF-1.4 fake"), nil
	}))

	grobidClient := pdf.NewGrobidClient("http://grobid.test", time.Second)
	grobidClient.SetHTTPClient(newFakeHTTPClient(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost {
			t.Fatalf("unexpected method: %s", r.Method)
		}
		if r.URL.Path != "/api/processFulltextDocument" {
			t.Fatalf("unexpected path: %s", r.URL.Path)
		}
		if !strings.Contains(r.Header.Get("Content-Type"), "multipart/form-data") {
			t.Fatalf("unexpected content type: %s", r.Header.Get("Content-Type"))
		}
		if err := r.ParseMultipartForm(1024 * 1024); err != nil {
			t.Fatalf("ParseMultipartForm: %v", err)
		}
		file, _, err := r.FormFile("input")
		if err != nil {
			t.Fatalf("FormFile: %v", err)
		}
		defer file.Close()
		body, err := io.ReadAll(file)
		if err != nil {
			t.Fatalf("ReadAll: %v", err)
		}
		if len(body) == 0 {
			t.Fatal("expected uploaded PDF bytes")
		}

		return textResponse(http.StatusOK, `<TEI><text><body><div><head>Results</head><p>PDF_ONLY_MARKER</p></div></body></text></TEI>`), nil
	}))

	analyzer := NewAnalyzer(nil, nil, pdfClient, grobidClient, nil)

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
	if !strings.Contains(prompt, "PDF_ONLY_MARKER") {
		t.Fatalf("prompt did not include parsed PDF text: %q", prompt)
	}
	if strings.Contains(prompt, "ABSTRACT_ONLY_MARKER") {
		t.Fatalf("prompt unexpectedly fell back to abstract: %q", prompt)
	}
}

func TestAnalyzerAnalyzeSyncFallsBackToAbstractWhenPDFURLMissing(t *testing.T) {
	analyzer := NewAnalyzer(nil, nil, pdf.NewDownloader(time.Second), pdf.NewGrobidClient("http://invalid", time.Second), nil)

	var prompt string
	analyzer.generateFn = func(ctx context.Context, got string) (string, error) {
		prompt = got
		return validAnalysisResponse(), nil
	}

	analysis, err := analyzer.AnalyzeSync(context.Background(), "paper-2", "ABSTRACT_ONLY_MARKER", "")
	if err != nil {
		t.Fatalf("AnalyzeSync returned error: %v", err)
	}
	if analysis == nil {
		t.Fatal("expected analysis")
	}
	if !strings.Contains(prompt, "ABSTRACT_ONLY_MARKER") {
		t.Fatalf("prompt did not include abstract: %q", prompt)
	}
}

func TestAnalyzerAnalyzeSyncFallsBackToAbstractWhenPDFDownloadFails(t *testing.T) {
	pdfClient := pdf.NewDownloader(time.Second)
	pdfClient.SetHTTPClient(newFakeHTTPClient(func(r *http.Request) (*http.Response, error) {
		return textResponse(http.StatusBadGateway, "boom"), nil
	}))

	analyzer := NewAnalyzer(nil, nil, pdfClient, pdf.NewGrobidClient("http://invalid", time.Second), nil)

	var prompt string
	analyzer.generateFn = func(ctx context.Context, got string) (string, error) {
		prompt = got
		return validAnalysisResponse(), nil
	}

	analysis, err := analyzer.AnalyzeSync(context.Background(), "paper-3", "ABSTRACT_ONLY_MARKER", "http://pdf.test/paper.pdf")
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

func TestAnalyzerAnalyzeSyncFallsBackToAbstractWhenGrobidFails(t *testing.T) {
	pdfClient := pdf.NewDownloader(time.Second)
	pdfClient.SetHTTPClient(newFakeHTTPClient(func(r *http.Request) (*http.Response, error) {
		return textResponse(http.StatusOK, "%PDF-1.4 fake"), nil
	}))

	grobidClient := pdf.NewGrobidClient("http://grobid.test", time.Second)
	grobidClient.SetHTTPClient(newFakeHTTPClient(func(r *http.Request) (*http.Response, error) {
		return textResponse(http.StatusBadGateway, "parse failed"), nil
	}))

	analyzer := NewAnalyzer(nil, nil, pdfClient, grobidClient, nil)

	var prompt string
	analyzer.generateFn = func(ctx context.Context, got string) (string, error) {
		prompt = got
		return validAnalysisResponse(), nil
	}

	analysis, err := analyzer.AnalyzeSync(context.Background(), "paper-4", "ABSTRACT_ONLY_MARKER", "http://pdf.test/paper.pdf")
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
	return strings.Join([]string{
		"1. Problem: problem",
		"2. Method: method",
		"3. Dataset: dataset",
		"4. Metrics: accuracy, f1",
		"5. Finding: finding",
		"6. Limitation: limitation",
		"7. Future work: future work",
	}, "\n")
}

type fakeRoundTripper func(*http.Request) (*http.Response, error)

func (f fakeRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func newFakeHTTPClient(fn fakeRoundTripper) *http.Client {
	return &http.Client{Transport: fn}
}

func textResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}
