package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"unicode/utf8"

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
	storeFn    func(ctx context.Context, topicID, paperID string, analysis *PaperAnalysis) error
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

type paperAnalysisResponse struct {
	ProblemStatement  *string   `json:"problem_statement"`
	Methodology       *string   `json:"methodology"`
	Dataset           *string   `json:"dataset"`
	EvaluationMetrics *[]string `json:"evaluation_metrics"`
	KeyFindings       *string   `json:"key_findings"`
	Limitations       *string   `json:"limitations"`
	FutureWork        *string   `json:"future_work"`
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
	topicID := job.GetString("topic_id")
	if topicID == "" {
		return fmt.Errorf("missing topic_id in job payload")
	}

	analysis, err := a.AnalyzeSync(ctx, paperID, job.GetString("abstract"), job.GetString("pdf_url"))
	if err != nil {
		return err
	}

	if err := a.StoreAnalysis(ctx, topicID, paperID, analysis); err != nil {
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

	prompt := fmt.Sprintf(`Analyze this paper. Return JSON only. Do not include markdown or explanations.

Text: %s

	{"problem_statement":"max 80 chars","methodology":"max 80 chars","dataset":"Not specified","evaluation_metrics":["metric"],"key_findings":"max 80 chars","limitations":"max 80 chars","future_work":"max 80 chars"}`, truncateText(fullText, 5000))

	var response paperAnalysisResponse
	var result string
	var err error
	if a.generateFn != nil {
		result, err = a.generateFn(ctx, prompt)
		if err == nil {
			err = json.Unmarshal([]byte(result), &response)
		}
	} else {
		err = a.structured.GenerateInto(ctx, prompt, paperAnalysisResponse{
			ProblemStatement:  stringPtr(""),
			Methodology:       stringPtr(""),
			Dataset:           stringPtr("Not specified"),
			EvaluationMetrics: &[]string{"metric"},
			KeyFindings:       stringPtr(""),
			Limitations:       stringPtr(""),
			FutureWork:        stringPtr(""),
		}, &response)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to analyze paper: %w", err)
	}

	logger.Debug().
		Str("paper_id", paperID).
		Int("result_len", len(result)).
		Str("result", truncateText(result, 500)).
		Msg("LLM analysis result")

	analysis, err := validatePaperAnalysisResponse(response)
	if err != nil {
		logger.Error().
			Str("paper_id", paperID).
			Int("result_len", len(result)).
			Str("result", truncateText(result, 1000)).
			Msg("Failed to parse LLM analysis response")
		return nil, fmt.Errorf("failed to parse analysis: %w", err)
	}

	return analysis, nil
}

func validatePaperAnalysisResponse(response paperAnalysisResponse) (*PaperAnalysis, error) {
	fields := []struct {
		name  string
		value *string
		max   int
	}{
		{"problem_statement", response.ProblemStatement, 80},
		{"methodology", response.Methodology, 80},
		{"dataset", response.Dataset, 50},
		{"key_findings", response.KeyFindings, 80},
		{"limitations", response.Limitations, 80},
		{"future_work", response.FutureWork, 80},
	}
	for _, field := range fields {
		if field.value == nil || strings.TrimSpace(*field.value) == "" {
			return nil, fmt.Errorf("missing required field %q", field.name)
		}
		if utf8.RuneCountInString(*field.value) > field.max {
			return nil, fmt.Errorf("field %q exceeds %d characters", field.name, field.max)
		}
	}
	if response.EvaluationMetrics == nil {
		return nil, fmt.Errorf("missing required field %q", "evaluation_metrics")
	}
	for i, metric := range *response.EvaluationMetrics {
		if strings.TrimSpace(metric) == "" {
			return nil, fmt.Errorf("evaluation_metrics[%d] is empty", i)
		}
	}
	return &PaperAnalysis{
		ProblemStatement:  strings.TrimSpace(*response.ProblemStatement),
		Methodology:       strings.TrimSpace(*response.Methodology),
		Dataset:           strings.TrimSpace(*response.Dataset),
		EvaluationMetrics: *response.EvaluationMetrics,
		KeyFindings:       strings.TrimSpace(*response.KeyFindings),
		Limitations:       strings.TrimSpace(*response.Limitations),
		FutureWork:        strings.TrimSpace(*response.FutureWork),
	}, nil
}

func stringPtr(value string) *string { return &value }

func (a *Analyzer) StoreAnalysis(ctx context.Context, topicID, paperID string, analysis *PaperAnalysis) error {
	return a.storeFn(ctx, topicID, paperID, analysis)
}

func (a *Analyzer) storeAnalysis(ctx context.Context, topicID, paperID string, analysis *PaperAnalysis) error {
	analysisJSON, err := json.Marshal(analysis)
	if err != nil {
		return fmt.Errorf("failed to marshal analysis: %w", err)
	}

	err = a.postgres.Queries().UpdatePaperAnalysis(ctx, postgres.UpdatePaperAnalysisParams{
		TopicID:  pgUUID(topicID),
		PaperID:  pgUUID(paperID),
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
