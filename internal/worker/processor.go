package worker

import (
	"context"
	"crypto/sha256"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/paper-scout/internal/logger"
	"github.com/paper-scout/internal/storage/postgres"
	"github.com/paper-scout/internal/tools/embedding"
	"github.com/paper-scout/internal/tools/pdf"
)

type EmbeddingScheduler interface {
	SubmitEmbeddings(ctx context.Context, parent Job, jobs []Job) error
}

type Processor struct {
	db                 *postgres.Client
	downloader         *pdf.Downloader
	parser             *pdf.GrobidClient
	embedder           *embedding.Generator
	analyzer           JobHandler
	scheduler          EmbeddingScheduler
	chunkWords         int
	chunkOverlap       int
	embeddingBatchSize int
}

func NewProcessor(
	db *postgres.Client,
	downloader *pdf.Downloader,
	parser *pdf.GrobidClient,
	embedder *embedding.Generator,
	analyzer JobHandler,
	scheduler EmbeddingScheduler,
	chunkWords int,
	chunkOverlap int,
	embeddingBatchSize int,
) *Processor {
	if chunkWords <= 0 {
		chunkWords = 350
	}
	if embeddingBatchSize <= 0 {
		embeddingBatchSize = 10
	}
	return &Processor{
		db:                 db,
		downloader:         downloader,
		parser:             parser,
		embedder:           embedder,
		analyzer:           analyzer,
		scheduler:          scheduler,
		chunkWords:         chunkWords,
		chunkOverlap:       chunkOverlap,
		embeddingBatchSize: embeddingBatchSize,
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
	case TypeEmbeddingBatch:
		return p.handleEmbeddingBatch(ctx, job)
	default:
		return fmt.Errorf("unknown job type: %s", job.Type)
	}
}

func (p *Processor) handlePaperAnalysis(ctx context.Context, job Job) error {
	if p.analyzer == nil {
		return fmt.Errorf("paper analysis handler not configured")
	}

	return p.analyzer(ctx, job)
}

func (p *Processor) handlePDFDownload(ctx context.Context, job Job) error {
	paperID := job.GetString("paper_id")
	topicID := job.GetString("topic_id")
	pdfURL := job.GetString("pdf_url")

	if topicID == "" || paperID == "" || pdfURL == "" {
		return fmt.Errorf("missing topic_id, paper_id, or pdf_url in job payload")
	}

	logger.Info().
		Str("job_id", job.ID).
		Str("paper_id", paperID).
		Msg("Downloading PDF")

	filename, data, err := p.downloader.Download(ctx, pdfURL)
	if err != nil {
		return fmt.Errorf("PDF download failed: %w", err)
	}

	tei, err := p.parser.Parse(ctx, filename, data)
	if err != nil {
		return fmt.Errorf("PDF parse failed: %w", err)
	}
	chunks := pdf.ChunkByParagraphsWithOverlap(p.parser.ExtractText(tei), p.chunkWords, p.chunkOverlap)
	if len(chunks) == 0 {
		return fmt.Errorf("PDF parse produced no indexable text")
	}

	topicUUID, err := uuid.Parse(topicID)
	if err != nil {
		return fmt.Errorf("invalid topic_id: %w", err)
	}
	paperUUID, err := uuid.Parse(paperID)
	if err != nil {
		return fmt.Errorf("invalid paper_id: %w", err)
	}
	previous, err := p.db.Queries().GetPaperChunks(ctx, postgres.GetPaperChunksParams{
		TopicID: topicUUID,
		PaperID: paperUUID,
	})
	if err != nil {
		return fmt.Errorf("load existing paper chunks: %w", err)
	}
	for _, chunk := range previous {
		if chunk.ChunkType != "pdf" || chunk.ChunkIndex < int32(len(chunks)) {
			continue
		}
		if err := p.embedder.DeleteEmbedding(ctx, embedding.PaperEmbedding{
			TopicID: topicID, PaperID: paperID, ChunkType: chunk.ChunkType, ChunkIndex: int(chunk.ChunkIndex),
		}); err != nil {
			return fmt.Errorf("delete stale chunk embedding %s: %w", chunk.ID, err)
		}
	}

	persisted := make([]*postgres.PaperChunk, 0, len(chunks))
	err = p.db.WithTx(ctx, func(q *postgres.Queries) error {
		for _, chunk := range chunks {
			hash := fmt.Sprintf("%x", sha256.Sum256([]byte(chunk.Text)))
			stored, err := q.UpsertPaperChunk(ctx, postgres.UpsertPaperChunkParams{
				TopicID: topicUUID, PaperID: paperUUID, ChunkType: "pdf", ChunkIndex: int32(chunk.Index),
				Text: chunk.Text, ContentHash: hash, Source: "grobid",
			})
			if err != nil {
				return fmt.Errorf("upsert chunk %d: %w", chunk.Index, err)
			}
			persisted = append(persisted, stored)
		}
		_, err := q.DeleteStalePaperChunks(ctx, postgres.DeleteStalePaperChunksParams{
			TopicID: topicUUID, PaperID: paperUUID, ChunkType: "pdf", ChunkIndex: int32(len(chunks)),
		})
		return err
	})
	if err != nil {
		return fmt.Errorf("persist PDF chunks: %w", err)
	}
	if _, err := p.db.Queries().UpdatePaperPDFStatus(ctx, postgres.UpdatePaperPDFStatusParams{
		ID: paperUUID, PdfDownloaded: pgtype.Bool{Bool: true, Valid: true}, PdfParsed: pgtype.Bool{Bool: true, Valid: true},
	}); err != nil {
		return fmt.Errorf("update PDF status: %w", err)
	}

	if p.scheduler == nil {
		return fmt.Errorf("embedding scheduler not configured")
	}
	chunksToEmbed := make([]embeddingChunk, 0, len(persisted))
	for _, chunk := range persisted {
		if chunk.EmbeddingStatus == "completed" {
			continue
		}
		chunksToEmbed = append(chunksToEmbed, embeddingChunk{
			ChunkID: chunk.ID.String(), TopicID: topicID, PaperID: paperID,
			Text: chunk.Text, ChunkType: chunk.ChunkType, ChunkIndex: int(chunk.ChunkIndex),
		})
	}
	for start := 0; start < len(chunksToEmbed); start += p.embeddingBatchSize {
		end := start + p.embeddingBatchSize
		if end > len(chunksToEmbed) {
			end = len(chunksToEmbed)
		}
		payloadChunks := make([]map[string]interface{}, 0, end-start)
		for _, chunk := range chunksToEmbed[start:end] {
			payloadChunks = append(payloadChunks, chunk.payload())
		}
		batchJob := NewJob(TypeEmbeddingBatch, map[string]interface{}{
			"topic_id": topicID,
			"paper_id": paperID,
			"chunks":   payloadChunks,
		})
		if err := p.scheduler.SubmitEmbeddings(ctx, job, []Job{batchJob}); err != nil {
			return fmt.Errorf("submit chunk embeddings: %w", err)
		}
	}

	return nil
}

func (p *Processor) handleEmbedding(ctx context.Context, job Job) error {
	return p.embedChunks(ctx, []embeddingChunk{embeddingChunkFromPayload(job.Payload)})
}

func (p *Processor) handleEmbeddingBatch(ctx context.Context, job Job) error {
	items := job.GetMaps("chunks")
	if len(items) == 0 {
		return fmt.Errorf("embedding batch is missing chunks")
	}
	chunks := make([]embeddingChunk, 0, len(items))
	for _, item := range items {
		chunks = append(chunks, embeddingChunkFromPayload(item))
	}
	return p.embedChunks(ctx, chunks)
}

type embeddingChunk struct {
	ChunkID    string
	PaperID    string
	TopicID    string
	Text       string
	ChunkType  string
	ChunkIndex int
}

func (c embeddingChunk) payload() map[string]interface{} {
	return map[string]interface{}{
		"chunk_id": c.ChunkID, "paper_id": c.PaperID, "topic_id": c.TopicID,
		"text": c.Text, "chunk_type": c.ChunkType, "chunk_index": c.ChunkIndex,
	}
}

func embeddingChunkFromPayload(payload map[string]interface{}) embeddingChunk {
	job := Job{Payload: payload}
	return embeddingChunk{
		ChunkID: job.GetString("chunk_id"), PaperID: job.GetString("paper_id"), TopicID: job.GetString("topic_id"),
		Text: job.GetString("text"), ChunkType: job.GetString("chunk_type"), ChunkIndex: job.GetInt("chunk_index"),
	}
}

func (p *Processor) embedChunks(ctx context.Context, chunks []embeddingChunk) error {
	if len(chunks) == 0 {
		return fmt.Errorf("embedding batch is empty")
	}
	texts := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		if chunk.PaperID == "" || chunk.TopicID == "" || chunk.ChunkID == "" || chunk.Text == "" {
			return fmt.Errorf("embedding batch contains a chunk with missing required fields")
		}
		texts = append(texts, chunk.Text)
	}

	logger.Info().Int("chunks", len(chunks)).Msg("Generating chunk embedding batch")
	vectors, err := p.embedder.GenerateBatch(ctx, texts)
	if err != nil {
		return fmt.Errorf("generate embedding batch: %w", err)
	}
	if len(vectors) != len(chunks) {
		return fmt.Errorf("embedding batch returned %d vectors for %d chunks", len(vectors), len(chunks))
	}

	embeddings := make([]embedding.PaperEmbedding, 0, len(chunks))
	for index, chunk := range chunks {
		embeddings = append(embeddings, embedding.PaperEmbedding{
			ChunkID: chunk.ChunkID, PaperID: chunk.PaperID, TopicID: chunk.TopicID,
			ChunkType: chunk.ChunkType, ChunkIndex: chunk.ChunkIndex, Text: chunk.Text, Vector: vectors[index],
		})
	}
	if err := p.embedder.StoreEmbeddings(ctx, embeddings); err != nil {
		return fmt.Errorf("store embedding batch: %w", err)
	}

	return p.db.WithTx(ctx, func(q *postgres.Queries) error {
		for _, chunk := range chunks {
			topicID, err := uuid.Parse(chunk.TopicID)
			if err != nil {
				return fmt.Errorf("invalid topic_id: %w", err)
			}
			chunkID, err := uuid.Parse(chunk.ChunkID)
			if err != nil {
				return fmt.Errorf("invalid chunk_id: %w", err)
			}
			if _, err := q.UpdatePaperChunkEmbeddingStatus(ctx, postgres.UpdatePaperChunkEmbeddingStatusParams{
				TopicID: topicID, ID: chunkID, EmbeddingStatus: "completed",
			}); err != nil {
				return fmt.Errorf("mark chunk embedding completed: %w", err)
			}
		}
		return nil
	})
}

func (p *Processor) CreateHandler() JobHandler {
	return p.HandleJob
}
