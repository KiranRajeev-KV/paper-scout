package worker

import (
	"context"
	"fmt"

	"github.com/research-agent/internal/logger"
	"github.com/research-agent/internal/storage/postgres"
	"github.com/research-agent/internal/tools/embedding"
	"github.com/research-agent/internal/tools/pdf"
)

type Processor struct {
	db         *postgres.Client
	downloader *pdf.Downloader
	parser     *pdf.GrobidClient
	embedder   *embedding.Generator
}

func NewProcessor(
	db *postgres.Client,
	downloader *pdf.Downloader,
	parser *pdf.GrobidClient,
	embedder *embedding.Generator,
) *Processor {
	return &Processor{
		db:         db,
		downloader: downloader,
		parser:     parser,
		embedder:   embedder,
	}
}

func (p *Processor) HandleJob(ctx context.Context, job Job) error {
	switch job.Type {
	case TypePaperAnalysis:
		return p.handlePaperAnalysis(ctx, job)
	case TypePDFDownload:
		return p.handlePDFDownload(ctx, job)
	case TypeEmbedding:
		return p.handleEmbedding(ctx, job)
	default:
		return fmt.Errorf("unknown job type: %s", job.Type)
	}
}

func (p *Processor) handlePaperAnalysis(ctx context.Context, job Job) error {
	paperID := job.GetString("paper_id")
	pdfURL := job.GetString("pdf_url")
	abstract := job.GetString("abstract")

	if paperID == "" {
		return fmt.Errorf("missing paper_id in job payload")
	}

	logger.Info().
		Str("job_id", job.ID).
		Str("paper_id", paperID).
		Msg("Processing paper analysis")

	if pdfURL != "" {
		filename, data, err := p.downloader.Download(ctx, pdfURL)
		if err != nil {
			logger.Warn().
				Err(err).
				Str("paper_id", paperID).
				Msg("PDF download failed, using abstract only")
		} else {
			parseResp, err := p.parser.Parse(ctx, filename, data)
			if err != nil {
				logger.Warn().
					Err(err).
					Str("paper_id", paperID).
					Msg("PDF parsing failed, using abstract only")
			} else {
				fullText := p.parser.ExtractText(parseResp)
				job.Payload["full_text"] = fullText
			}
		}
	}

	if abstract != "" {
		job.Payload["full_text"] = abstract
	}

	return nil
}

func (p *Processor) handlePDFDownload(ctx context.Context, job Job) error {
	paperID := job.GetString("paper_id")
	pdfURL := job.GetString("pdf_url")

	if paperID == "" || pdfURL == "" {
		return fmt.Errorf("missing paper_id or pdf_url in job payload")
	}

	logger.Info().
		Str("job_id", job.ID).
		Str("paper_id", paperID).
		Msg("Downloading PDF")

	filename, data, err := p.downloader.Download(ctx, pdfURL)
	if err != nil {
		return fmt.Errorf("PDF download failed: %w", err)
	}

	job.Payload["filename"] = filename
	job.Payload["data"] = data

	return nil
}

func (p *Processor) handleEmbedding(ctx context.Context, job Job) error {
	paperID := job.GetString("paper_id")
	topicID := job.GetString("topic_id")
	text := job.GetString("text")
	chunkType := job.GetString("chunk_type")
	chunkIndex := job.GetInt("chunk_index")

	if paperID == "" || topicID == "" || text == "" {
		return fmt.Errorf("missing required fields in embedding job")
	}

	logger.Info().
		Str("job_id", job.ID).
		Str("paper_id", paperID).
		Int("chunk_index", chunkIndex).
		Msg("Generating embedding")

	vector, err := p.embedder.Generate(ctx, text)
	if err != nil {
		return fmt.Errorf("embedding generation failed: %w", err)
	}

	emb := embedding.PaperEmbedding{
		PaperID:    paperID,
		TopicID:    topicID,
		ChunkType:  chunkType,
		ChunkIndex: chunkIndex,
		Text:       text,
		Vector:     vector,
	}

	if err := p.embedder.StoreEmbedding(ctx, emb); err != nil {
		return fmt.Errorf("embedding storage failed: %w", err)
	}

	return nil
}

func (p *Processor) CreateHandler() JobHandler {
	return p.HandleJob
}
