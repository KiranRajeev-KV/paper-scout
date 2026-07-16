// Package abstractindex maintains durable embeddings for paper abstracts.
package abstractindex

import (
	"context"
	"crypto/sha256"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/paper-scout/internal/logger"
	"github.com/paper-scout/internal/storage/postgres"
	"github.com/paper-scout/internal/tools/embedding"
	"github.com/paper-scout/internal/worker"
)

const abstractChunkType = "abstract"

// CleanupReconciler completes deferred deletion of replaced embedding points.
type CleanupReconciler interface {
	ReconcileEmbeddingCleanup(context.Context, int32) (worker.CleanupResult, error)
}

// Service owns durable abstract chunks and their Qdrant embedding lifecycle.
type Service struct {
	postgres *postgres.Client
	embedder *embedding.Generator
	cleanup  CleanupReconciler
}

// NewService constructs an abstract-index service with its required storage and vector dependencies.
func NewService(pg *postgres.Client, embedder *embedding.Generator, cleanup CleanupReconciler) (*Service, error) {
	if pg == nil || embedder == nil || cleanup == nil {
		return nil, fmt.Errorf("abstract index requires postgres, embedding generator, and cleanup reconciler")
	}
	return &Service{postgres: pg, embedder: embedder, cleanup: cleanup}, nil
}

// Ensure makes every abstract-bearing paper queryable in the active embedding collection.
func (s *Service) Ensure(ctx context.Context, topicID string, papers []*postgres.Paper) (map[string]*postgres.Paper, error) {
	topicUUID, err := uuid.Parse(topicID)
	if err != nil {
		return nil, fmt.Errorf("invalid topic ID: %w", err)
	}
	queryable := make(map[string]*postgres.Paper)
	persisted := make([]*postgres.PaperChunk, 0, len(papers))
	previous := make(map[uuid.UUID]*postgres.PaperChunk, len(papers))
	for _, paper := range papers {
		chunks, err := s.postgres.Queries().GetPaperChunks(ctx, postgres.GetPaperChunksParams{TopicID: topicUUID, PaperID: paper.ID})
		if err != nil {
			return nil, fmt.Errorf("load existing abstract chunk for paper %s: %w", paper.ID, err)
		}
		for _, chunk := range chunks {
			if chunk.ChunkType == abstractChunkType && chunk.ChunkIndex == 0 {
				previous[paper.ID] = chunk
				break
			}
		}
	}
	if err := s.postgres.WithTx(ctx, func(q *postgres.Queries) error {
		for _, paper := range papers {
			abstract := textValue(paper.Abstract)
			if abstract == "" {
				continue
			}
			hash := fmt.Sprintf("%x", sha256.Sum256([]byte(abstract)))
			if old := previous[paper.ID]; old != nil && old.ContentHash != hash && old.QdrantPointID.Valid && old.QdrantCollection.Valid {
				if _, err := q.CreateEmbeddingCleanupTask(ctx, postgres.CreateEmbeddingCleanupTaskParams{
					CollectionName: old.QdrantCollection.String, PointID: uuid.UUID(old.QdrantPointID.Bytes),
					TopicID: topicUUID, PaperID: paper.ID, ChunkID: pgtype.UUID{Bytes: old.ID, Valid: true},
					Reason: "abstract content replaced",
				}); err != nil {
					return fmt.Errorf("schedule old abstract cleanup for paper %s: %w", paper.ID, err)
				}
			}
			chunk, err := q.UpsertPaperChunk(ctx, postgres.UpsertPaperChunkParams{
				TopicID: topicUUID, PaperID: paper.ID, ChunkType: abstractChunkType, ChunkIndex: 0,
				Text: abstract, ContentHash: hash, Source: "paper_metadata", SectionHeading: pgtype.Text{},
			})
			if err != nil {
				return fmt.Errorf("persist abstract chunk for paper %s: %w", paper.ID, err)
			}
			queryable[paper.ID.String()] = paper
			persisted = append(persisted, chunk)
		}
		return nil
	}); err != nil {
		return nil, err
	}

	identity := s.embedder.Identity()
	collection := s.embedder.CollectionName()
	stale := make([]*postgres.PaperChunk, 0, len(persisted))
	texts := make([]string, 0, len(persisted))
	candidates := make(map[string]*postgres.PaperChunk, len(persisted))
	for _, chunk := range persisted {
		if currentEmbedding(chunk, identity, collection) {
			candidates[uuid.UUID(chunk.QdrantPointID.Bytes).String()] = chunk
			continue
		}
		stale = append(stale, chunk)
		texts = append(texts, chunk.Text)
	}
	if len(candidates) > 0 {
		pointIDs := make([]string, 0, len(candidates))
		for pointID := range candidates {
			pointIDs = append(pointIDs, pointID)
		}
		existing, err := s.embedder.ExistingPoints(ctx, pointIDs)
		if err != nil {
			return nil, fmt.Errorf("verify abstract vectors in Qdrant: %w", err)
		}
		for pointID, chunk := range candidates {
			if _, ok := existing[pointID]; ok {
				continue
			}
			if _, err := s.postgres.Queries().UpdatePaperChunkEmbeddingStatus(ctx, postgres.UpdatePaperChunkEmbeddingStatusParams{
				TopicID: chunk.TopicID, ID: chunk.ID, EmbeddingStatus: "pending",
				ErrorMessage: pgtype.Text{String: "Qdrant point missing; reindex scheduled", Valid: true},
			}); err != nil {
				return nil, fmt.Errorf("reset missing abstract vector %s: %w", pointID, err)
			}
			stale = append(stale, chunk)
			texts = append(texts, chunk.Text)
		}
	}
	if len(stale) == 0 {
		s.reconcileCleanup(ctx)
		return queryable, nil
	}
	vectors, err := s.embedder.GenerateBatch(ctx, texts)
	if err != nil {
		return nil, fmt.Errorf("generate abstract embeddings: %w", err)
	}
	embeddings := make([]embedding.PaperEmbedding, len(stale))
	for index, chunk := range stale {
		embeddings[index] = embedding.PaperEmbedding{
			ChunkID: chunk.ID.String(), PaperID: chunk.PaperID.String(), TopicID: chunk.TopicID.String(),
			ChunkType: chunk.ChunkType, ChunkIndex: int(chunk.ChunkIndex), Text: chunk.Text,
			ContentHash: chunk.ContentHash, SectionHeading: chunk.SectionHeading.String, Identity: identity, Vector: vectors[index],
		}
	}
	if err := s.markIndexing(ctx, stale, embeddings, identity, collection); err != nil {
		return nil, err
	}
	if err := s.embedder.StoreEmbeddings(ctx, embeddings); err != nil {
		return nil, fmt.Errorf("store abstract embeddings: %w", err)
	}
	if err := s.completeEmbeddings(ctx, stale, embeddings); err != nil {
		return nil, err
	}
	s.reconcileCleanup(ctx)
	return queryable, nil
}

func (s *Service) markIndexing(ctx context.Context, stale []*postgres.PaperChunk, embeddings []embedding.PaperEmbedding, identity embedding.Identity, collection string) error {
	return s.postgres.WithTx(ctx, func(q *postgres.Queries) error {
		for index, chunk := range stale {
			pointID, err := uuid.Parse(embedding.EmbeddingPointID(embeddings[index]))
			if err != nil {
				return fmt.Errorf("parse deterministic point ID: %w", err)
			}
			if _, err := q.MarkPaperChunkEmbeddingIndexing(ctx, postgres.MarkPaperChunkEmbeddingIndexingParams{
				TopicID: chunk.TopicID, ID: chunk.ID, EmbeddingProvider: text(identity.Provider), EmbeddingModel: text(identity.Model),
				EmbeddingDimensions:         pgtype.Int4{Int32: int32(identity.Dimensions), Valid: true},
				EmbeddingInstructionVersion: text(identity.InstructionVersion), EmbeddingIndexingVersion: text(identity.IndexingVersion),
				QdrantCollection: text(collection), QdrantPointID: pgtype.UUID{Bytes: pointID, Valid: true},
			}); err != nil {
				return fmt.Errorf("mark abstract chunk %s indexing: %w", chunk.ID, err)
			}
		}
		return nil
	})
}

func (s *Service) completeEmbeddings(ctx context.Context, stale []*postgres.PaperChunk, embeddings []embedding.PaperEmbedding) error {
	return s.postgres.WithTx(ctx, func(q *postgres.Queries) error {
		for index, chunk := range stale {
			pointID, err := uuid.Parse(embedding.EmbeddingPointID(embeddings[index]))
			if err != nil {
				return fmt.Errorf("parse deterministic point ID: %w", err)
			}
			if _, err := q.CompletePaperChunkEmbedding(ctx, postgres.CompletePaperChunkEmbeddingParams{
				TopicID: chunk.TopicID, ID: chunk.ID, QdrantPointID: pgtype.UUID{Bytes: pointID, Valid: true},
			}); err != nil {
				return fmt.Errorf("complete abstract embedding %s: %w", chunk.ID, err)
			}
		}
		return nil
	})
}

func (s *Service) reconcileCleanup(ctx context.Context) {
	result, err := s.cleanup.ReconcileEmbeddingCleanup(ctx, 100)
	if err != nil {
		logger.From(ctx).Warn().Err(err).Int("pending", result.Pending).Msg("Embedding cleanup remains retryable after abstract indexing")
		return
	}
	if result.Completed > 0 || result.Pending > 0 {
		logger.From(ctx).Info().Int("completed", result.Completed).Int("pending", result.Pending).Msg("Reconciled embedding cleanup after abstract indexing")
	}
}

func currentEmbedding(chunk *postgres.PaperChunk, identity embedding.Identity, collection string) bool {
	return chunk.EmbeddingStatus == "completed" && chunk.EmbeddedContentHash.Valid && chunk.EmbeddedContentHash.String == chunk.ContentHash &&
		chunk.EmbeddingProvider.Valid && chunk.EmbeddingProvider.String == identity.Provider && chunk.EmbeddingModel.Valid && chunk.EmbeddingModel.String == identity.Model &&
		chunk.EmbeddingDimensions.Valid && int(chunk.EmbeddingDimensions.Int32) == identity.Dimensions &&
		chunk.EmbeddingInstructionVersion.Valid && chunk.EmbeddingInstructionVersion.String == identity.InstructionVersion &&
		chunk.EmbeddingIndexingVersion.Valid && chunk.EmbeddingIndexingVersion.String == identity.IndexingVersion &&
		chunk.QdrantCollection.Valid && chunk.QdrantCollection.String == collection && chunk.QdrantPointID.Valid
}

func text(value string) pgtype.Text { return pgtype.Text{String: value, Valid: true} }

func textValue(value pgtype.Text) string {
	if !value.Valid {
		return ""
	}
	return value.String
}
