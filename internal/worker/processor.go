package worker

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

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

type DocumentParser interface {
	Parse(ctx context.Context, filename string, data []byte) (pdf.Document, error)
	Provider() string
	Version() string
}

type Processor struct {
	db                 *postgres.Client
	downloader         *pdf.Downloader
	parser             DocumentParser
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
	parser DocumentParser,
	embedder *embedding.Generator,
	analyzer JobHandler,
	scheduler EmbeddingScheduler,
	chunkWords int,
	chunkOverlap int,
	embeddingBatchSize int,
) (*Processor, error) {
	if db == nil || downloader == nil || parser == nil || embedder == nil || analyzer == nil || scheduler == nil {
		return nil, fmt.Errorf("processor requires database, downloader, parser, embedder, analyzer, and scheduler")
	}
	if chunkWords <= 0 || chunkOverlap < 0 || chunkOverlap >= chunkWords {
		return nil, fmt.Errorf("invalid chunk size/overlap %d/%d", chunkWords, chunkOverlap)
	}
	if embeddingBatchSize <= 0 {
		return nil, fmt.Errorf("embedding batch size must be positive")
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
	}, nil
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

	logger.From(ctx).Info().
		Str("job_id", job.ID).
		Str("paper_id", paperID).
		Msg("Downloading PDF")

	filename, data, err := p.downloader.Download(ctx, pdfURL)
	if err != nil {
		return fmt.Errorf("PDF download failed: %w", err)
	}

	pdfHash := fmt.Sprintf("%x", sha256.Sum256(data))
	parseStarted := time.Now()
	document, err := p.parser.Parse(ctx, filename, data)
	if err != nil {
		recordErr := p.recordDocument(ctx, paperID, pdfHash, "failed", time.Since(parseStarted), document.Warnings, err, document.Markdown, document.JSON)
		return fmt.Errorf("PDF parse failed: %w", errors.Join(err, recordErr))
	}
	markdown := document.Markdown
	chunks := pdf.ChunkMarkdown(markdown, p.chunkWords, p.chunkOverlap)
	if len(chunks) == 0 {
		emptyErr := fmt.Errorf("PDF parse produced no indexable text")
		recordErr := p.recordDocument(ctx, paperID, pdfHash, "failed", time.Since(parseStarted), document.Warnings, emptyErr, markdown, document.JSON)
		return errors.Join(emptyErr, recordErr)
	}
	if err := p.recordDocument(ctx, paperID, pdfHash, "completed", time.Since(parseStarted), document.Warnings, nil, markdown, document.JSON); err != nil {
		return err
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

	persisted := make([]*postgres.PaperChunk, 0, len(chunks))
	err = p.db.WithTx(ctx, func(q *postgres.Queries) error {
		newHashes := make(map[int32]string, len(chunks))
		for _, chunk := range chunks {
			newHashes[int32(chunk.Index)] = fmt.Sprintf("%x", sha256.Sum256([]byte(chunk.Text)))
		}
		for _, old := range previous {
			if old.ChunkType != "pdf" || !old.QdrantPointID.Valid || !old.QdrantCollection.Valid {
				continue
			}
			newHash, retained := newHashes[old.ChunkIndex]
			if retained && newHash == old.ContentHash {
				continue
			}
			if _, err := q.CreateEmbeddingCleanupTask(ctx, postgres.CreateEmbeddingCleanupTaskParams{
				CollectionName: old.QdrantCollection.String, PointID: uuid.UUID(old.QdrantPointID.Bytes),
				TopicID: topicUUID, PaperID: paperUUID, ChunkID: pgtype.UUID{Bytes: old.ID, Valid: true},
				Reason: "document content replaced",
			}); err != nil {
				return fmt.Errorf("schedule stale embedding cleanup for chunk %s: %w", old.ID, err)
			}
		}
		for _, chunk := range chunks {
			hash := fmt.Sprintf("%x", sha256.Sum256([]byte(chunk.Text)))
			stored, err := q.UpsertPaperChunk(ctx, postgres.UpsertPaperChunkParams{
				TopicID: topicUUID, PaperID: paperUUID, ChunkType: "pdf", ChunkIndex: int32(chunk.Index),
				Text: chunk.Text, ContentHash: hash, Source: "pdf_parser",
				SectionHeading: pgtype.Text{String: chunk.Heading, Valid: chunk.Heading != ""},
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
		if currentChunkEmbedding(chunk, p.embedder.Identity(), p.embedder.CollectionName()) {
			continue
		}
		chunksToEmbed = append(chunksToEmbed, embeddingChunk{
			ChunkID: chunk.ID.String(), TopicID: topicID, PaperID: paperID,
			Text: chunk.Text, ContentHash: chunk.ContentHash, SectionHeading: chunk.SectionHeading.String,
			ChunkType: chunk.ChunkType, ChunkIndex: int(chunk.ChunkIndex),
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
	ChunkID        string
	PaperID        string
	TopicID        string
	Text           string
	ContentHash    string
	SectionHeading string
	ChunkType      string
	ChunkIndex     int
}

func (c embeddingChunk) payload() map[string]interface{} {
	return map[string]interface{}{
		"chunk_id": c.ChunkID, "paper_id": c.PaperID, "topic_id": c.TopicID,
		"text": c.Text, "chunk_type": c.ChunkType, "chunk_index": c.ChunkIndex,
		"content_hash":    c.ContentHash,
		"section_heading": c.SectionHeading,
	}
}

func embeddingChunkFromPayload(payload map[string]interface{}) embeddingChunk {
	job := Job{Payload: payload}
	return embeddingChunk{
		ChunkID: job.GetString("chunk_id"), PaperID: job.GetString("paper_id"), TopicID: job.GetString("topic_id"),
		Text: job.GetString("text"), ContentHash: job.GetString("content_hash"), SectionHeading: job.GetString("section_heading"),
		ChunkType: job.GetString("chunk_type"), ChunkIndex: job.GetInt("chunk_index"),
	}
}

func (p *Processor) embedChunks(ctx context.Context, chunks []embeddingChunk) error {
	if len(chunks) == 0 {
		return fmt.Errorf("embedding batch is empty")
	}
	texts := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		if chunk.PaperID == "" || chunk.TopicID == "" || chunk.ChunkID == "" || chunk.Text == "" || chunk.ContentHash == "" {
			return fmt.Errorf("embedding batch contains a chunk with missing required fields")
		}
		texts = append(texts, chunk.Text)
	}

	logger.From(ctx).Info().Int("chunks", len(chunks)).Msg("Generating chunk embedding batch")
	vectors, err := p.embedder.GenerateBatch(ctx, texts)
	if err != nil {
		return fmt.Errorf("generate embedding batch: %w", err)
	}
	if len(vectors) != len(chunks) {
		return fmt.Errorf("embedding batch returned %d vectors for %d chunks", len(vectors), len(chunks))
	}

	embeddings := make([]embedding.PaperEmbedding, 0, len(chunks))
	identity := p.embedder.Identity()
	collection := p.embedder.CollectionName()
	for index, chunk := range chunks {
		embeddings = append(embeddings, embedding.PaperEmbedding{
			ChunkID: chunk.ChunkID, PaperID: chunk.PaperID, TopicID: chunk.TopicID,
			ChunkType: chunk.ChunkType, ChunkIndex: chunk.ChunkIndex, Text: chunk.Text,
			ContentHash: chunk.ContentHash, SectionHeading: chunk.SectionHeading, Identity: identity, Vector: vectors[index],
		})
	}
	if err := p.db.WithTx(ctx, func(q *postgres.Queries) error {
		for index, chunk := range chunks {
			topicID, chunkID, err := parseChunkIDs(chunk)
			if err != nil {
				return err
			}
			pointID, err := uuid.Parse(embedding.EmbeddingPointID(embeddings[index]))
			if err != nil {
				return fmt.Errorf("parse deterministic point ID: %w", err)
			}
			if _, err := q.MarkPaperChunkEmbeddingIndexing(ctx, postgres.MarkPaperChunkEmbeddingIndexingParams{
				TopicID: topicID, ID: chunkID,
				EmbeddingProvider:           pgtype.Text{String: identity.Provider, Valid: true},
				EmbeddingModel:              pgtype.Text{String: identity.Model, Valid: true},
				EmbeddingDimensions:         pgtype.Int4{Int32: int32(identity.Dimensions), Valid: true},
				EmbeddingInstructionVersion: pgtype.Text{String: identity.InstructionVersion, Valid: true},
				EmbeddingIndexingVersion:    pgtype.Text{String: identity.IndexingVersion, Valid: true},
				QdrantCollection:            pgtype.Text{String: collection, Valid: true},
				QdrantPointID:               pgtype.UUID{Bytes: pointID, Valid: true},
			}); err != nil {
				return fmt.Errorf("mark chunk %s indexing: %w", chunk.ChunkID, err)
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if err := p.embedder.StoreEmbeddings(ctx, embeddings); err != nil {
		return fmt.Errorf("store embedding batch: %w", err)
	}

	if err := p.db.WithTx(ctx, func(q *postgres.Queries) error {
		for index, chunk := range chunks {
			topicID, chunkID, err := parseChunkIDs(chunk)
			if err != nil {
				return err
			}
			pointID, err := uuid.Parse(embedding.EmbeddingPointID(embeddings[index]))
			if err != nil {
				return fmt.Errorf("parse deterministic point ID: %w", err)
			}
			if _, err := q.CompletePaperChunkEmbedding(ctx, postgres.CompletePaperChunkEmbeddingParams{
				TopicID: topicID, ID: chunkID, QdrantPointID: pgtype.UUID{Bytes: pointID, Valid: true},
			}); err != nil {
				return fmt.Errorf("mark chunk embedding completed: %w", err)
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if _, err := p.ReconcileEmbeddingCleanup(ctx, 100); err != nil {
		logger.From(ctx).Warn().Err(err).Msg("Embedding cleanup remains retryable after replacement commit")
	}
	return nil
}

func parseChunkIDs(chunk embeddingChunk) (uuid.UUID, uuid.UUID, error) {
	topicID, err := uuid.Parse(chunk.TopicID)
	if err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("invalid topic_id: %w", err)
	}
	chunkID, err := uuid.Parse(chunk.ChunkID)
	if err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("invalid chunk_id: %w", err)
	}
	return topicID, chunkID, nil
}

func currentChunkEmbedding(chunk *postgres.PaperChunk, identity embedding.Identity, collection string) bool {
	return chunk.EmbeddingStatus == "completed" && chunk.EmbeddedContentHash.Valid && chunk.EmbeddedContentHash.String == chunk.ContentHash &&
		chunk.EmbeddingProvider.Valid && chunk.EmbeddingProvider.String == identity.Provider &&
		chunk.EmbeddingModel.Valid && chunk.EmbeddingModel.String == identity.Model &&
		chunk.EmbeddingDimensions.Valid && int(chunk.EmbeddingDimensions.Int32) == identity.Dimensions &&
		chunk.EmbeddingInstructionVersion.Valid && chunk.EmbeddingInstructionVersion.String == identity.InstructionVersion &&
		chunk.EmbeddingIndexingVersion.Valid && chunk.EmbeddingIndexingVersion.String == identity.IndexingVersion &&
		chunk.QdrantCollection.Valid && chunk.QdrantCollection.String == collection && chunk.QdrantPointID.Valid
}

func (p *Processor) recordDocument(ctx context.Context, paperID, pdfHash, status string, duration time.Duration, warnings []string, cause error, markdown string, parserJSON []byte) error {
	id, err := uuid.Parse(paperID)
	if err != nil {
		return fmt.Errorf("invalid paper_id: %w", err)
	}
	warningJSON, err := json.Marshal(warnings)
	if err != nil {
		return fmt.Errorf("encode parser warnings: %w", err)
	}
	errorMessage := pgtype.Text{}
	if cause != nil {
		errorMessage = pgtype.Text{String: cause.Error(), Valid: true}
	}
	_, err = p.db.Queries().UpsertPaperDocument(ctx, postgres.UpsertPaperDocumentParams{
		PaperID: id, PdfHash: pdfHash, ParserProvider: p.parser.Provider(), ParserVersion: p.parser.Version(), Status: status,
		DurationMs: duration.Milliseconds(), Warnings: warningJSON, ErrorMessage: errorMessage,
		Markdown: pgtype.Text{String: markdown, Valid: markdown != ""}, ParserJson: parserJSON,
	})
	if err != nil {
		return fmt.Errorf("record PDF parser result: %w", err)
	}
	return nil
}

type CleanupResult struct {
	Completed int
	Pending   int
}

func (p *Processor) ReconcileEmbeddingCleanup(ctx context.Context, limit int32) (CleanupResult, error) {
	if limit < 1 {
		limit = 100
	}
	tasks, err := p.db.Queries().ListPendingEmbeddingCleanupTasks(ctx, limit)
	if err != nil {
		return CleanupResult{}, fmt.Errorf("list pending embedding cleanup tasks: %w", err)
	}
	return reconcileCleanupTasks(ctx, tasks, p.embedder.DeletePoint,
		func(ctx context.Context, id uuid.UUID) error {
			_, err := p.db.Queries().CompleteEmbeddingCleanupTask(ctx, id)
			return err
		},
		func(ctx context.Context, id uuid.UUID, cause error) error {
			_, err := p.db.Queries().FailEmbeddingCleanupTask(ctx, postgres.FailEmbeddingCleanupTaskParams{
				ID: id, ErrorMessage: pgtype.Text{String: cause.Error(), Valid: true},
			})
			return err
		})
}

func reconcileCleanupTasks(
	ctx context.Context,
	tasks []*postgres.EmbeddingCleanupTask,
	deletePoint func(context.Context, string, string) error,
	complete func(context.Context, uuid.UUID) error,
	fail func(context.Context, uuid.UUID, error) error,
) (CleanupResult, error) {
	result := CleanupResult{}
	var persistenceErrors []error
	for _, task := range tasks {
		if err := deletePoint(ctx, task.CollectionName, task.PointID.String()); err != nil {
			result.Pending++
			if recordErr := fail(ctx, task.ID, err); recordErr != nil {
				persistenceErrors = append(persistenceErrors, fmt.Errorf("record cleanup failure for %s: %w", task.ID, recordErr))
			}
			continue
		}
		if err := complete(ctx, task.ID); err != nil {
			result.Pending++
			persistenceErrors = append(persistenceErrors, fmt.Errorf("complete cleanup task %s: %w", task.ID, err))
			continue
		}
		result.Completed++
	}
	return result, errors.Join(persistenceErrors...)
}

func (p *Processor) CreateHandler() JobHandler {
	return p.HandleJob
}
