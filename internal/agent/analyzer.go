package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/paper-scout/internal/llm"
	"github.com/paper-scout/internal/logger"
	"github.com/paper-scout/internal/storage/postgres"
	"github.com/paper-scout/internal/worker"
	"github.com/rs/zerolog"
)

const maxPaperAnalysisBytes = 16 << 10

type Analyzer struct {
	postgres   *postgres.Client
	structured *llm.StructuredOutput
	pool       *worker.Pool
	jobTimeout time.Duration
	dispatchMu sync.Mutex
	mu         sync.Mutex
	batches    map[string]*analysisBatch
	jobToBatch map[string]string
	analyzeFn  func(ctx context.Context, paperID, abstract, pdfURL string) (*PaperAnalysis, error)
	storeFn    func(ctx context.Context, topicID, paperID string, analysis *PaperAnalysis) error
	generateFn func(ctx context.Context, prompt string) (string, error)
}

func NewAnalyzer(llmClient llm.Generator, pg *postgres.Client, pool *worker.Pool, timeout ...time.Duration) *Analyzer {
	jobTimeout := 10 * time.Minute
	if len(timeout) > 0 && timeout[0] > 0 {
		jobTimeout = timeout[0]
	}
	analyzer := &Analyzer{
		postgres:   pg,
		structured: llm.NewStructuredOutput(llmClient),
		pool:       pool,
		jobTimeout: jobTimeout,
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
	failures   []ItemFailure
	done       chan struct{}
	onProgress func(completed, total int)
	log        zerolog.Logger
	updated    chan struct{}
}

func (a *Analyzer) Analyze(ctx context.Context, topicID string, papers []AnalysisPaper, onProgress func(completed, total int)) error {
	logger.From(ctx).Info().
		Str("topic_id", topicID).
		Int("papers", len(papers)).
		Msg("Starting paper analysis")

	batchID := uuid.NewString()
	batch := &analysisBatch{
		topicID:    topicID,
		total:      len(papers),
		done:       make(chan struct{}),
		onProgress: onProgress,
		log:        *logger.From(ctx),
		updated:    make(chan struct{}, 1),
	}

	a.mu.Lock()
	a.batches[batchID] = batch
	if batch.total == 0 {
		close(batch.done)
	}
	a.mu.Unlock()

	for _, paper := range papers {
		a.dispatchMu.Lock()
		job := worker.NewJobWithTimeout(worker.TypePaperAnalysis, map[string]interface{}{
			"paper_id": paper.ID,
			"topic_id": topicID,
			"abstract": paper.Abstract,
			"pdf_url":  paper.PDFURL,
		}, a.jobTimeout)

		a.mu.Lock()
		a.jobToBatch[job.ID] = batchID
		a.mu.Unlock()

		if err := a.pool.Submit(job); err != nil {
			a.dispatchMu.Unlock()
			a.cancelBatch(batchID)
			return fmt.Errorf("failed to submit analysis job for paper %s: %w", paper.ID, err)
		}
		if err := a.waitForJob(ctx, batch, job.ID); err != nil {
			a.dispatchMu.Unlock()
			a.cancelBatch(batchID)
			return err
		}
		a.dispatchMu.Unlock()
	}

	logger.From(ctx).Info().Msg("All paper analysis jobs completed")
	return a.waitForBatch(ctx, batchID)
}

func (a *Analyzer) waitForJob(ctx context.Context, batch *analysisBatch, jobID string) error {
	for {
		a.mu.Lock()
		_, pending := a.jobToBatch[jobID]
		a.mu.Unlock()
		if !pending {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-batch.updated:
		}
	}
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

	analysis, err := a.analyzeStoredPaper(ctx, topicID, paperID, job.GetString("abstract"), job.GetString("pdf_url"))
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
		batch.failures = append(batch.failures, ItemFailure{
			Kind:       string(job.Type),
			Identifier: job.GetString("paper_id"),
			Err:        err,
		})
	}
	completed := batch.completed
	total := batch.total
	onProgress := batch.onProgress
	done := completed >= total
	if done {
		close(batch.done)
	}
	a.mu.Unlock()
	select {
	case batch.updated <- struct{}{}:
	default:
	}

	if onProgress != nil {
		onProgress(completed, total)
	}

	if err != nil {
		batch.log.Warn().
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
		return fmt.Errorf("analysis batch %s not found", batchID)
	}
	defer a.releaseBatch(batchID)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-batch.done:
		batch.log.Info().
			Str("topic_id", batch.topicID).
			Int("completed", batch.completed).
			Int("failed", len(batch.failures)).
			Int("total", batch.total).
			Msg("Paper analysis batch completed")
		return newBatchError("paper analysis", batch.total, batch.failures)
	}
}

func (a *Analyzer) releaseBatch(batchID string) {
	a.mu.Lock()
	delete(a.batches, batchID)
	a.mu.Unlock()
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
	_ = paperID
	_ = pdfURL
	return a.analyzeText(ctx, paperID, abstract)
}

func (a *Analyzer) analyzeStoredPaper(ctx context.Context, topicID, paperID, abstract, pdfURL string) (*PaperAnalysis, error) {
	text := abstract
	if a.postgres != nil {
		topicUUID, err := parseID("topic ID", topicID)
		if err != nil {
			return nil, err
		}
		paperUUID, err := parseID("paper ID", paperID)
		if err != nil {
			return nil, err
		}
		chunks, err := a.postgres.Queries().GetPaperChunks(ctx, postgres.GetPaperChunksParams{
			TopicID: topicUUID,
			PaperID: paperUUID,
		})
		if err != nil {
			return nil, fmt.Errorf("load indexed paper chunks: %w", err)
		}
		if len(chunks) > 0 {
			parts := make([]string, 0, len(chunks))
			for _, chunk := range chunks {
				if chunk.ChunkType == "pdf" {
					parts = append(parts, chunk.Text)
				}
			}
			if len(parts) > 0 {
				text = strings.Join(parts, "\n\n")
			}
		}
	}

	if text == abstract {
		logger.From(ctx).Debug().Str("paper_id", paperID).Msg("No indexed PDF chunks available; using abstract")
	}
	return a.analyzeFn(ctx, paperID, text, pdfURL)
}

func (a *Analyzer) analyzeText(ctx context.Context, paperID, fullText string) (*PaperAnalysis, error) {

	prompt := fmt.Sprintf(`Analyze this paper. Return concise JSON only. Do not include markdown or explanations.

Text: %s

	{"problem_statement":"string","methodology":"string","dataset":"string or Not specified","evaluation_metrics":["metric"],"key_findings":"string","limitations":"string","future_work":"string"}`, truncateText(fullText, 5000))

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

	logger.From(ctx).Debug().
		Str("paper_id", paperID).
		Int("result_len", len(result)).
		Msg("LLM analysis result")

	analysis, err := validatePaperAnalysisResponse(response)
	if err != nil {
		logger.From(ctx).Error().
			Str("paper_id", paperID).
			Int("result_len", len(result)).
			Msg("Failed to parse LLM analysis response")
		return nil, fmt.Errorf("failed to parse analysis: %w", err)
	}

	return analysis, nil
}

func validatePaperAnalysisResponse(response paperAnalysisResponse) (*PaperAnalysis, error) {
	fields := []struct {
		name  string
		value *string
	}{
		{"problem_statement", response.ProblemStatement},
		{"methodology", response.Methodology},
		{"dataset", response.Dataset},
		{"key_findings", response.KeyFindings},
		{"limitations", response.Limitations},
		{"future_work", response.FutureWork},
	}
	for _, field := range fields {
		if field.value == nil || strings.TrimSpace(*field.value) == "" {
			return nil, fmt.Errorf("missing required field %q", field.name)
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
	encoded, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("marshal analysis response for size validation: %w", err)
	}
	if len(encoded) > maxPaperAnalysisBytes {
		return nil, fmt.Errorf("analysis response exceeds %d bytes", maxPaperAnalysisBytes)
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
	topicUUID, err := parseID("topic ID", topicID)
	if err != nil {
		return err
	}
	paperUUID, err := parseID("paper ID", paperID)
	if err != nil {
		return err
	}
	analysisJSON, err := json.Marshal(analysis)
	if err != nil {
		return fmt.Errorf("failed to marshal analysis: %w", err)
	}

	err = a.postgres.Queries().UpdatePaperAnalysis(ctx, postgres.UpdatePaperAnalysisParams{
		TopicID:  topicUUID,
		PaperID:  paperUUID,
		Analysis: analysisJSON,
	})

	return err
}
