package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

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
}

func NewAnalyzer(llmClient *llm.Client, pg *postgres.Client, dl *pdf.Downloader, parser *pdf.GrobidClient, pool *worker.Pool) *Analyzer {
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

	result, err := a.llm.Generate(ctx, prompt)
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
