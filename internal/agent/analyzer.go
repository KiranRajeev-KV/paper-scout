package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/paper-scout/internal/llm"
	"github.com/paper-scout/internal/logger"
	"github.com/paper-scout/internal/storage/postgres"
	"github.com/paper-scout/internal/tools/pdf"
	"github.com/paper-scout/internal/worker"
)

type Analyzer struct {
	llm        *llm.Client
	postgres   *postgres.Client
	structured *llm.StructuredOutput
	downloader *pdf.Downloader
	parser     *pdf.GrobidClient
	pool       *worker.Pool
	mu         sync.Mutex
	batches    map[string]*analysisBatch
	jobToBatch map[string]string
	analyzeFn  func(ctx context.Context, paperID, abstract, pdfURL string) (*PaperAnalysis, error)
	storeFn    func(ctx context.Context, paperID string, analysis *PaperAnalysis) error
	generateFn func(ctx context.Context, prompt string) (string, error)
}

func NewAnalyzer(llmClient *llm.Client, pg *postgres.Client, dl *pdf.Downloader, parser *pdf.GrobidClient, pool *worker.Pool) *Analyzer {
	analyzer := &Analyzer{
		llm:        llmClient,
		postgres:   pg,
		structured: llm.NewStructuredOutput(llmClient),
		downloader: dl,
		parser:     parser,
		pool:       pool,
		batches:    make(map[string]*analysisBatch),
		jobToBatch: make(map[string]string),
	}
	analyzer.analyzeFn = analyzer.analyzeSync
	analyzer.storeFn = analyzer.storeAnalysis
	analyzer.generateFn = analyzer.generate
	return analyzer
}

type AnalysisPaper struct {
	ID       string
	Title    string
	Abstract string
	PDFURL   string
}

type PaperAnalysis struct {
	ProblemStatement  string   `json:"problem_statement"`
	Methodology       string   `json:"methodology"`
	Dataset           string   `json:"dataset"`
	EvaluationMetrics []string `json:"evaluation_metrics"`
	KeyFindings       string   `json:"key_findings"`
	Limitations       string   `json:"limitations"`
	FutureWork        string   `json:"future_work"`
}

type analysisBatch struct {
	topicID    string
	total      int
	completed  int
	failures   int
	done       chan struct{}
	onProgress func(completed, total int)
}

func (a *Analyzer) Analyze(ctx context.Context, topicID string, papers []AnalysisPaper, onProgress func(completed, total int)) error {
	logger.Info().
		Str("topic_id", topicID).
		Int("papers", len(papers)).
		Msg("Starting paper analysis")

	batchID := uuid.NewString()
	batch := &analysisBatch{
		topicID:    topicID,
		total:      len(papers),
		done:       make(chan struct{}),
		onProgress: onProgress,
	}

	a.mu.Lock()
	a.batches[batchID] = batch
	a.mu.Unlock()

	for _, paper := range papers {
		job := worker.NewJobWithTimeout(worker.TypePaperAnalysis, map[string]interface{}{
			"paper_id": paper.ID,
			"topic_id": topicID,
			"abstract": paper.Abstract,
			"pdf_url":  paper.PDFURL,
		}, 10*60*1000*1000*1000)

		a.mu.Lock()
		a.jobToBatch[job.ID] = batchID
		a.mu.Unlock()

		if err := a.pool.Submit(job); err != nil {
			a.cancelBatch(batchID)
			return fmt.Errorf("failed to submit analysis job for paper %s: %w", paper.ID, err)
		}
	}

	logger.Info().Msg("All paper analysis jobs submitted")
	return a.waitForBatch(ctx, batchID)
}

func (a *Analyzer) HandleJob(ctx context.Context, job worker.Job) error {
	paperID := job.GetString("paper_id")
	if paperID == "" {
		return fmt.Errorf("missing paper_id in job payload")
	}

	analysis, err := a.AnalyzeSync(ctx, paperID, job.GetString("abstract"), job.GetString("pdf_url"))
	if err != nil {
		return err
	}

	if err := a.StoreAnalysis(ctx, paperID, analysis); err != nil {
		return fmt.Errorf("failed to store analysis: %w", err)
	}

	return nil
}

func (a *Analyzer) HandleJobCompletion(job worker.Job, err error, terminal bool) {
	if !terminal {
		return
	}

	a.mu.Lock()
	batchID, ok := a.jobToBatch[job.ID]
	if !ok {
		a.mu.Unlock()
		return
	}

	batch, exists := a.batches[batchID]
	if !exists {
		delete(a.jobToBatch, job.ID)
		a.mu.Unlock()
		return
	}

	delete(a.jobToBatch, job.ID)
	batch.completed++
	if err != nil {
		batch.failures++
	}
	completed := batch.completed
	total := batch.total
	onProgress := batch.onProgress
	done := completed >= total
	if done {
		delete(a.batches, batchID)
		close(batch.done)
	}
	a.mu.Unlock()

	if onProgress != nil {
		onProgress(completed, total)
	}

	if err != nil {
		logger.Warn().
			Err(err).
			Str("topic_id", batch.topicID).
			Str("paper_id", job.GetString("paper_id")).
			Int("completed", completed).
			Int("total", total).
			Msg("Paper analysis job failed")
	}
}

func (a *Analyzer) waitForBatch(ctx context.Context, batchID string) error {
	a.mu.Lock()
	batch, ok := a.batches[batchID]
	a.mu.Unlock()
	if !ok {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-batch.done:
		logger.Info().
			Str("topic_id", batch.topicID).
			Int("completed", batch.completed).
			Int("failed", batch.failures).
			Int("total", batch.total).
			Msg("Paper analysis batch completed")
		return nil
	}
}

func (a *Analyzer) cancelBatch(batchID string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	delete(a.batches, batchID)
	for jobID, candidateBatchID := range a.jobToBatch {
		if candidateBatchID == batchID {
			delete(a.jobToBatch, jobID)
		}
	}
}

func (a *Analyzer) AnalyzeSync(ctx context.Context, paperID, abstract, pdfURL string) (*PaperAnalysis, error) {
	return a.analyzeFn(ctx, paperID, abstract, pdfURL)
}

func (a *Analyzer) analyzeSync(ctx context.Context, paperID, abstract, pdfURL string) (*PaperAnalysis, error) {
	var fullText string

	if pdfURL != "" {
		filename, data, err := a.downloader.Download(ctx, pdfURL)
		if err != nil {
			logger.Warn().Err(err).Str("paper_id", paperID).Msg("PDF download failed, using abstract")
		} else {
			parseResp, err := a.parser.Parse(ctx, filename, data)
			if err != nil {
				logger.Warn().Err(err).Str("paper_id", paperID).Msg("PDF parse failed, using abstract")
			} else {
				fullText = a.parser.ExtractText(parseResp)
			}
		}
	}

	if fullText == "" {
		fullText = abstract
	}

	prompt := fmt.Sprintf(`Analyze this paper. Answer with a numbered list ONLY. No JSON. No explanation.

Text: %s

1. Problem (max 80 chars):
2. Method (max 80 chars):
3. Dataset (or "Not specified"):
4. Metrics (comma-separated):
5. Finding (max 80 chars):
6. Limitation (max 80 chars):
7. Future work (max 80 chars):

Answer with 7 lines only.`, truncateText(fullText, 5000))

	result, err := a.generateFn(ctx, prompt)
	if err != nil {
		return nil, fmt.Errorf("failed to analyze paper: %w", err)
	}

	logger.Debug().
		Str("paper_id", paperID).
		Int("result_len", len(result)).
		Str("result", truncateText(result, 500)).
		Msg("LLM analysis result")

	analysis := parseNumberedListToAnalysis(result)
	if analysis == nil {
		logger.Error().
			Str("paper_id", paperID).
			Int("result_len", len(result)).
			Str("result", truncateText(result, 1000)).
			Msg("Failed to parse LLM analysis response")
		return nil, fmt.Errorf("failed to parse analysis: invalid format")
	}

	return analysis, nil
}

func parseNumberedListToAnalysis(result string) *PaperAnalysis {
	lines := strings.Split(result, "\n")
	if len(lines) < 7 {
		return nil
	}

	extractField := func(line string, maxLen int) string {
		parts := strings.SplitN(line, ":", 2)
		if len(parts) < 2 {
			return ""
		}
		value := strings.TrimSpace(parts[1])
		if len(value) > maxLen {
			value = value[:maxLen]
		}
		return value
	}

	analysis := &PaperAnalysis{
		ProblemStatement: extractField(lines[0], 80),
		Methodology:      extractField(lines[1], 80),
		Dataset:          extractField(lines[2], 50),
		KeyFindings:      extractField(lines[4], 80),
		Limitations:      extractField(lines[5], 80),
		FutureWork:       extractField(lines[6], 80),
	}

	metricsLine := extractField(lines[3], 200)
	if metricsLine != "" && metricsLine != "Not specified" {
		metrics := strings.Split(metricsLine, ",")
		for i, m := range metrics {
			metrics[i] = strings.TrimSpace(m)
		}
		analysis.EvaluationMetrics = metrics
	}

	return analysis
}

func (a *Analyzer) StoreAnalysis(ctx context.Context, paperID string, analysis *PaperAnalysis) error {
	return a.storeFn(ctx, paperID, analysis)
}

func (a *Analyzer) storeAnalysis(ctx context.Context, paperID string, analysis *PaperAnalysis) error {
	analysisJSON, err := json.Marshal(analysis)
	if err != nil {
		return fmt.Errorf("failed to marshal analysis: %w", err)
	}

	_, err = a.postgres.Queries().UpdatePaperAnalysis(ctx, postgres.UpdatePaperAnalysisParams{
		ID:       pgUUID(paperID),
		Analysis: analysisJSON,
	})

	return err
}

func (a *Analyzer) generate(ctx context.Context, prompt string) (string, error) {
	if a.llm == nil {
		return "", fmt.Errorf("llm client is not configured")
	}
	return a.llm.Generate(ctx, prompt)
}
