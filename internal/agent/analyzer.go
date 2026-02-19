package agent

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/research-agent/internal/llm"
	"github.com/research-agent/internal/logger"
	"github.com/research-agent/internal/storage/postgres"
	"github.com/research-agent/internal/tools/pdf"
	"github.com/research-agent/internal/worker"
)

type Analyzer struct {
	llm        *llm.Client
	postgres   *postgres.Client
	structured *llm.StructuredOutput
	downloader *pdf.Downloader
	parser     *pdf.UnstructuredClient
	pool       *worker.Pool
}

func NewAnalyzer(llmClient *llm.Client, pg *postgres.Client, dl *pdf.Downloader, parser *pdf.UnstructuredClient, pool *worker.Pool) *Analyzer {
	return &Analyzer{
		llm:        llmClient,
		postgres:   pg,
		structured: llm.NewStructuredOutput(llmClient),
		downloader: dl,
		parser:     parser,
		pool:       pool,
	}
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

func (a *Analyzer) Analyze(ctx context.Context, topicID string, paperIDs []string) error {
	logger.Info().
		Str("topic_id", topicID).
		Int("papers", len(paperIDs)).
		Msg("Starting paper analysis")

	for _, paperID := range paperIDs {
		job := worker.NewJobWithTimeout(worker.TypePaperAnalysis, map[string]interface{}{
			"paper_id": paperID,
			"topic_id": topicID,
		}, 10*60*1000*1000*1000)

		if err := a.pool.Submit(job); err != nil {
			logger.Warn().Err(err).Str("paper_id", paperID).Msg("Failed to submit analysis job")
		}
	}

	logger.Info().Msg("All paper analysis jobs submitted")
	return nil
}

func (a *Analyzer) AnalyzeSync(ctx context.Context, paperID, abstract, pdfURL string) (*PaperAnalysis, error) {
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

	prompt := fmt.Sprintf(`Analyze this academic paper and extract BRIEF structured information.

Text: %s

Respond with ONLY valid JSON. Keep each field under 100 characters.
{
  "problem_statement": "One sentence problem summary",
  "methodology": "One sentence method summary",
  "dataset": "Dataset name or 'Not specified'",
  "evaluation_metrics": ["metric1", "metric2"],
  "key_findings": "One sentence main finding",
  "limitations": "One sentence limitation",
  "future_work": "One sentence future work"
}`, truncateText(fullText, 10000))

	schema := map[string]interface{}{
		"problem_statement":  "",
		"methodology":        "",
		"dataset":            "",
		"evaluation_metrics": []string{},
		"key_findings":       "",
		"limitations":        "",
		"future_work":        "",
	}

	result, err := a.structured.Generate(ctx, prompt, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to analyze paper: %w", err)
	}

	logger.Debug().
		Str("paper_id", paperID).
		Int("result_len", len(result)).
		Str("result", truncateText(result, 500)).
		Msg("LLM analysis result")

	var analysis PaperAnalysis
	if err := json.Unmarshal([]byte(result), &analysis); err != nil {
		logger.Error().
			Err(err).
			Str("paper_id", paperID).
			Int("result_len", len(result)).
			Str("result", truncateText(result, 1000)).
			Msg("Failed to parse LLM analysis response")
		return nil, fmt.Errorf("failed to parse analysis: %w", err)
	}

	return &analysis, nil
}

func (a *Analyzer) StoreAnalysis(ctx context.Context, paperID string, analysis *PaperAnalysis) error {
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
