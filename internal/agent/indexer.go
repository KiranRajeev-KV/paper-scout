package agent

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/paper-scout/internal/logger"
	"github.com/paper-scout/internal/storage/postgres"
	"github.com/paper-scout/internal/tools/embedding"
	"github.com/paper-scout/internal/worker"
	"github.com/rs/zerolog"
)

// Indexer coordinates document-indexing jobs without making workers wait on
// child jobs. Its state exists only for the active batch; all artifacts live in
// PostgreSQL and Qdrant and can be replayed safely after a restart.
type Indexer struct {
	postgres *postgres.Client
	pool     *worker.Pool
	vectors  *embedding.Generator

	mu         sync.Mutex
	batches    map[string]*indexBatch
	jobToBatch map[string]string
}

type indexBatch struct {
	topicID   string
	total     int
	completed int
	failures  []ItemFailure
	done      chan struct{}
	log       zerolog.Logger
}

func NewIndexer(pg *postgres.Client, pool *worker.Pool, vectors ...*embedding.Generator) *Indexer {
	var vectorService *embedding.Generator
	if len(vectors) > 0 {
		vectorService = vectors[0]
	}
	return &Indexer{
		postgres:   pg,
		pool:       pool,
		vectors:    vectorService,
		batches:    make(map[string]*indexBatch),
		jobToBatch: make(map[string]string),
	}
}

func (i *Indexer) Index(ctx context.Context, topicID string, papers []RankedPaper) error {
	if len(papers) == 0 {
		return nil
	}

	batchID := uuid.NewString()
	batch := &indexBatch{topicID: topicID, done: make(chan struct{}), log: *logger.From(ctx)}
	i.mu.Lock()
	i.batches[batchID] = batch
	i.mu.Unlock()

	for _, paper := range papers {
		if paper.PDFURL == "" {
			continue
		}
		indexed, err := i.isFullyIndexed(ctx, topicID, paper.ID)
		if err != nil {
			i.cancelBatch(batchID)
			return fmt.Errorf("check document index for paper %s: %w", paper.ID, err)
		}
		if indexed {
			continue
		}
		job := worker.NewJob(worker.TypePDFDownload, map[string]interface{}{
			"topic_id": topicID,
			"paper_id": paper.ID,
			"pdf_url":  paper.PDFURL,
		})
		if err := i.registerAndSubmit(ctx, batchID, job); err != nil {
			i.cancelBatch(batchID)
			return fmt.Errorf("submit PDF indexing job for paper %s: %w", paper.ID, err)
		}
	}

	i.mu.Lock()
	batch = i.batches[batchID]
	if batch != nil && batch.total == 0 {
		close(batch.done)
	}
	i.mu.Unlock()

	return i.wait(ctx, batchID)
}

// SubmitEmbeddings is called by the PDF worker after chunks have been committed.
// Every child is registered before it is submitted, so a fast completion cannot
// race the batch accounting.
func (i *Indexer) SubmitEmbeddings(ctx context.Context, parent worker.Job, jobs []worker.Job) error {
	if len(jobs) == 0 {
		return nil
	}
	i.mu.Lock()
	batchID, ok := i.jobToBatch[parent.ID]
	i.mu.Unlock()
	if !ok {
		return fmt.Errorf("indexing batch not found for PDF job %s", parent.ID)
	}

	for _, job := range jobs {
		if err := i.register(batchID, job); err != nil {
			return err
		}
		go i.submitAsync(job)
	}
	return nil
}

func (i *Indexer) registerAndSubmit(ctx context.Context, batchID string, job worker.Job) error {
	if err := i.register(batchID, job); err != nil {
		return err
	}
	if err := i.pool.Submit(job); err != nil {
		i.complete(job, err)
		return err
	}
	return nil
}

func (i *Indexer) register(batchID string, job worker.Job) error {
	i.mu.Lock()
	batch, ok := i.batches[batchID]
	if !ok {
		i.mu.Unlock()
		return fmt.Errorf("indexing batch %s is no longer active", batchID)
	}
	batch.total++
	i.jobToBatch[job.ID] = batchID
	i.mu.Unlock()
	return nil
}

func (i *Indexer) submitAsync(job worker.Job) {
	if err := i.pool.Submit(job); err != nil {
		i.complete(job, err)
	}
}

func (i *Indexer) HandleJobCompletion(ctx context.Context, job worker.Job, err error, terminal bool) {
	if !terminal || (job.Type != worker.TypePDFDownload && job.Type != worker.TypeEmbedding && job.Type != worker.TypeEmbeddingBatch) {
		return
	}
	if err != nil && (job.Type == worker.TypeEmbedding || job.Type == worker.TypeEmbeddingBatch) {
		if recordErr := i.recordEmbeddingFailure(ctx, job, err); recordErr != nil {
			err = errors.Join(err, recordErr)
		}
	}
	i.complete(job, err)
}

func (i *Indexer) complete(job worker.Job, err error) {
	i.mu.Lock()
	batchID, ok := i.jobToBatch[job.ID]
	if !ok {
		i.mu.Unlock()
		return
	}
	delete(i.jobToBatch, job.ID)
	batch, ok := i.batches[batchID]
	if !ok {
		i.mu.Unlock()
		return
	}
	batch.completed++
	if err != nil {
		identifier := job.GetString("paper_id")
		if identifier == "" {
			identifier = job.GetString("chunk_id")
		}
		batch.failures = append(batch.failures, ItemFailure{
			Kind:       string(job.Type),
			Identifier: identifier,
			Err:        err,
		})
	}
	if batch.completed >= batch.total {
		close(batch.done)
	}
	i.mu.Unlock()
}

func (i *Indexer) wait(ctx context.Context, batchID string) error {
	i.mu.Lock()
	batch := i.batches[batchID]
	i.mu.Unlock()
	if batch == nil {
		return fmt.Errorf("indexing batch %s not found", batchID)
	}
	defer i.releaseBatch(batchID)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-batch.done:
		batch.log.Info().Str("topic_id", batch.topicID).Int("failed", len(batch.failures)).Msg("Document indexing batch completed")
		return newBatchError("document indexing", batch.total, batch.failures)
	}
}

func (i *Indexer) releaseBatch(batchID string) {
	i.mu.Lock()
	delete(i.batches, batchID)
	i.mu.Unlock()
}

func (i *Indexer) cancelBatch(batchID string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	delete(i.batches, batchID)
	for jobID, candidate := range i.jobToBatch {
		if candidate == batchID {
			delete(i.jobToBatch, jobID)
		}
	}
}

func (i *Indexer) recordEmbeddingFailure(ctx context.Context, job worker.Job, cause error) error {
	if i.postgres == nil {
		return nil
	}
	topicID, err := uuid.Parse(job.GetString("topic_id"))
	if err != nil {
		return fmt.Errorf("parse failed embedding topic ID: %w", err)
	}
	chunkIDs := []string{job.GetString("chunk_id")}
	if job.Type == worker.TypeEmbeddingBatch {
		chunkIDs = chunkIDs[:0]
		for _, chunk := range job.GetMaps("chunks") {
			if chunkID, ok := chunk["chunk_id"].(string); ok {
				chunkIDs = append(chunkIDs, chunkID)
			}
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var failures []error
	for _, chunkID := range chunkIDs {
		parsedChunkID, err := uuid.Parse(chunkID)
		if err != nil {
			failures = append(failures, fmt.Errorf("parse failed embedding chunk ID %q: %w", chunkID, err))
			continue
		}
		_, updateErr := i.postgres.Queries().UpdatePaperChunkEmbeddingStatus(ctx, postgres.UpdatePaperChunkEmbeddingStatusParams{
			TopicID:         topicID,
			ID:              parsedChunkID,
			EmbeddingStatus: "failed",
			ErrorMessage:    pgtype.Text{String: cause.Error(), Valid: true},
		})
		if updateErr != nil {
			logger.From(ctx).Warn().Err(updateErr).Str("chunk_id", parsedChunkID.String()).Msg("Failed to record terminal chunk embedding failure")
			failures = append(failures, fmt.Errorf("record failed embedding chunk %s: %w", parsedChunkID, updateErr))
		}
	}
	return errors.Join(failures...)
}

func (i *Indexer) isFullyIndexed(ctx context.Context, topicID, paperID string) (bool, error) {
	if i.postgres == nil {
		return false, nil
	}
	topicUUID, err := uuid.Parse(topicID)
	if err != nil {
		return false, err
	}
	paperUUID, err := uuid.Parse(paperID)
	if err != nil {
		return false, err
	}
	chunks, err := i.postgres.Queries().GetPaperChunks(ctx, postgres.GetPaperChunksParams{
		TopicID: topicUUID,
		PaperID: paperUUID,
	})
	if err != nil {
		return false, err
	}
	if len(chunks) == 0 {
		return false, nil
	}
	identity := embedding.Identity{}
	collection := ""
	if i.vectors != nil {
		identity = i.vectors.Identity()
		collection = i.vectors.CollectionName()
	}
	pointIDs := make([]string, 0, len(chunks))
	for _, chunk := range chunks {
		if chunk.EmbeddingStatus != "completed" || !chunk.EmbeddedContentHash.Valid || chunk.EmbeddedContentHash.String != chunk.ContentHash ||
			i.vectors != nil && (!chunk.EmbeddingProvider.Valid || chunk.EmbeddingProvider.String != identity.Provider ||
				!chunk.EmbeddingModel.Valid || chunk.EmbeddingModel.String != identity.Model ||
				!chunk.EmbeddingDimensions.Valid || int(chunk.EmbeddingDimensions.Int32) != identity.Dimensions ||
				!chunk.EmbeddingInstructionVersion.Valid || chunk.EmbeddingInstructionVersion.String != identity.InstructionVersion ||
				!chunk.EmbeddingIndexingVersion.Valid || chunk.EmbeddingIndexingVersion.String != identity.IndexingVersion ||
				!chunk.QdrantCollection.Valid || chunk.QdrantCollection.String != collection) || !chunk.QdrantPointID.Valid {
			return false, nil
		}
		pointIDs = append(pointIDs, uuid.UUID(chunk.QdrantPointID.Bytes).String())
	}
	if i.vectors != nil {
		existing, err := i.vectors.ExistingPoints(ctx, pointIDs)
		if err != nil {
			return false, fmt.Errorf("verify Qdrant points: %w", err)
		}
		if len(existing) != len(pointIDs) {
			for _, chunk := range chunks {
				pointID := uuid.UUID(chunk.QdrantPointID.Bytes).String()
				if _, ok := existing[pointID]; ok {
					continue
				}
				_, updateErr := i.postgres.Queries().UpdatePaperChunkEmbeddingStatus(ctx, postgres.UpdatePaperChunkEmbeddingStatusParams{
					TopicID: topicUUID, ID: chunk.ID, EmbeddingStatus: "pending",
					ErrorMessage: pgtype.Text{String: "Qdrant point missing; reindex scheduled", Valid: true},
				})
				if updateErr != nil {
					return false, fmt.Errorf("reset missing Qdrant point %s: %w", pointID, updateErr)
				}
			}
			return false, nil
		}
	}
	return true, nil
}
