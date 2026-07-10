package agent

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/paper-scout/internal/logger"
	"github.com/paper-scout/internal/storage/postgres"
	"github.com/paper-scout/internal/worker"
)

// Indexer coordinates document-indexing jobs without making workers wait on
// child jobs. Its state exists only for the active batch; all artifacts live in
// PostgreSQL and Qdrant and can be replayed safely after a restart.
type Indexer struct {
	postgres *postgres.Client
	pool     *worker.Pool

	mu         sync.Mutex
	batches    map[string]*indexBatch
	jobToBatch map[string]string
}

type indexBatch struct {
	topicID   string
	total     int
	completed int
	failures  int
	done      chan struct{}
}

func NewIndexer(pg *postgres.Client, pool *worker.Pool) *Indexer {
	return &Indexer{
		postgres:   pg,
		pool:       pool,
		batches:    make(map[string]*indexBatch),
		jobToBatch: make(map[string]string),
	}
}

func (i *Indexer) Index(ctx context.Context, topicID string, papers []RankedPaper) error {
	if len(papers) == 0 {
		return nil
	}

	batchID := uuid.NewString()
	batch := &indexBatch{topicID: topicID, done: make(chan struct{})}
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
		delete(i.batches, batchID)
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

func (i *Indexer) HandleJobCompletion(job worker.Job, err error, terminal bool) {
	if !terminal || (job.Type != worker.TypePDFDownload && job.Type != worker.TypeEmbedding && job.Type != worker.TypeEmbeddingBatch) {
		return
	}
	if err != nil && (job.Type == worker.TypeEmbedding || job.Type == worker.TypeEmbeddingBatch) {
		i.recordEmbeddingFailure(job, err)
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
		batch.failures++
	}
	if batch.completed >= batch.total {
		delete(i.batches, batchID)
		close(batch.done)
	}
	i.mu.Unlock()
}

func (i *Indexer) wait(ctx context.Context, batchID string) error {
	i.mu.Lock()
	batch := i.batches[batchID]
	i.mu.Unlock()
	if batch == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-batch.done:
		logger.Info().Str("topic_id", batch.topicID).Int("failed", batch.failures).Msg("Document indexing batch completed")
		return nil
	}
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

func (i *Indexer) recordEmbeddingFailure(job worker.Job, cause error) {
	if i.postgres == nil {
		return
	}
	topicID, err := uuid.Parse(job.GetString("topic_id"))
	if err != nil {
		return
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
	for _, chunkID := range chunkIDs {
		parsedChunkID, err := uuid.Parse(chunkID)
		if err != nil {
			continue
		}
		_, updateErr := i.postgres.Queries().UpdatePaperChunkEmbeddingStatus(context.Background(), postgres.UpdatePaperChunkEmbeddingStatusParams{
			TopicID:         topicID,
			ID:              parsedChunkID,
			EmbeddingStatus: "failed",
			ErrorMessage:    pgtype.Text{String: cause.Error(), Valid: true},
		})
		if updateErr != nil {
			logger.Warn().Err(updateErr).Str("chunk_id", parsedChunkID.String()).Msg("Failed to record terminal chunk embedding failure")
		}
	}
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
	for _, chunk := range chunks {
		if chunk.EmbeddingStatus != "completed" {
			return false, nil
		}
	}
	return true, nil
}
