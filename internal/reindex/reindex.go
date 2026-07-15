// Package reindex rebuilds and atomically activates embedding generations.
package reindex

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/paper-scout/internal/storage/postgres"
	qdrantstore "github.com/paper-scout/internal/storage/qdrant"
	"github.com/paper-scout/internal/tools/embedding"
	"github.com/qdrant/go-client/qdrant"
)

var errChunkSnapshotChanged = errors.New("durable chunk snapshot changed during reindex")

// Runner rebuilds one physical collection from durable chunks before activation.
type Runner struct {
	db         *postgres.Client
	vectors    *qdrantstore.Client
	embeddings *embedding.Generator
	batchSize  int
	progress   io.Writer
}

func NewRunner(db *postgres.Client, vectors *qdrantstore.Client, embeddings *embedding.Generator, batchSize int, progress io.Writer) (*Runner, error) {
	if db == nil || vectors == nil || embeddings == nil {
		return nil, fmt.Errorf("reindex requires PostgreSQL, Qdrant, and an embedding provider")
	}
	if batchSize < 1 {
		return nil, fmt.Errorf("reindex batch size must be positive")
	}
	if progress == nil {
		progress = io.Discard
	}
	return &Runner{db: db, vectors: vectors, embeddings: embeddings, batchSize: batchSize, progress: progress}, nil
}

func (r *Runner) Run(ctx context.Context) error {
	identity := r.embeddings.Identity()
	collection := r.vectors.PhysicalCollectionName()
	generation, err := r.db.Queries().CreateEmbeddingGeneration(ctx, postgres.CreateEmbeddingGenerationParams{
		Provider: identity.Provider, Model: identity.Model, Dimensions: int32(identity.Dimensions),
		InstructionVersion: identity.InstructionVersion, IndexingVersion: identity.IndexingVersion,
		CollectionName: collection,
	})
	if err != nil {
		return fmt.Errorf("create embedding generation ledger: %w", err)
	}
	chunks, err := r.db.Queries().ListPaperChunksForReindex(ctx)
	if err != nil {
		return fmt.Errorf("list durable chunks: %w", err)
	}
	if len(chunks) == 0 {
		return fmt.Errorf("no durable chunks are available to reindex")
	}
	expected := make(map[string]struct{}, len(chunks))
	for start := 0; start < len(chunks); start += r.batchSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := start + r.batchSize
		if end > len(chunks) {
			end = len(chunks)
		}
		texts := make([]string, 0, end-start)
		for _, chunk := range chunks[start:end] {
			texts = append(texts, chunk.Text)
		}
		vectors, err := r.embeddings.GenerateBatch(ctx, texts)
		if err != nil {
			return fmt.Errorf("embed chunks %d-%d: %w", start+1, end, err)
		}
		items := make([]embedding.PaperEmbedding, 0, end-start)
		for index, chunk := range chunks[start:end] {
			item := embedding.PaperEmbedding{
				ChunkID: chunk.ID.String(), PaperID: chunk.PaperID.String(), TopicID: chunk.TopicID.String(),
				ChunkType: chunk.ChunkType, ChunkIndex: int(chunk.ChunkIndex), Text: chunk.Text,
				ContentHash: chunk.ContentHash, SectionHeading: chunk.SectionHeading.String, Identity: identity, Vector: vectors[index],
			}
			expected[embedding.EmbeddingPointID(item)] = struct{}{}
			items = append(items, item)
		}
		if err := r.embeddings.StoreEmbeddings(ctx, items); err != nil {
			return fmt.Errorf("write chunks %d-%d: %w", start+1, end, err)
		}
		if _, err := r.db.Queries().UpdateEmbeddingGenerationProgress(ctx, postgres.UpdateEmbeddingGenerationProgressParams{
			ID: generation.ID, IndexedChunks: int64(end),
		}); err != nil {
			return fmt.Errorf("record reindex progress: %w", err)
		}
		fmt.Fprintf(r.progress, "indexed %d/%d chunks\n", end, len(chunks))
	}
	if err := r.removeUnexpectedPoints(ctx, expected); err != nil {
		return err
	}
	count, err := r.vectors.Count(ctx, nil)
	if err != nil {
		return fmt.Errorf("count rebuilt Qdrant points: %w", err)
	}
	if count != uint64(len(chunks)) {
		return fmt.Errorf("rebuilt collection contains %d points; expected %d; activation refused", count, len(chunks))
	}
	if err := r.db.WithTx(ctx, func(q *postgres.Queries) error {
		if err := q.LockPaperChunksForEmbeddingActivation(ctx); err != nil {
			return fmt.Errorf("lock durable chunks for activation: %w", err)
		}
		current, err := q.ListPaperChunksForReindex(ctx)
		if err != nil {
			return fmt.Errorf("revalidate durable chunks before activation: %w", err)
		}
		if err := validateChunkSnapshot(chunks, current); err != nil {
			return err
		}
		return activateGeneration(ctx, r.vectors.Activate, func(ctx context.Context) error {
			if err := q.RetireActiveEmbeddingGenerations(ctx, generation.ID); err != nil {
				return err
			}
			if _, err := q.ActivateEmbeddingGeneration(ctx, generation.ID); err != nil {
				return err
			}
			for _, chunk := range chunks {
				item := embedding.PaperEmbedding{
					ChunkID: chunk.ID.String(), PaperID: chunk.PaperID.String(), TopicID: chunk.TopicID.String(),
					ChunkType: chunk.ChunkType, ChunkIndex: int(chunk.ChunkIndex), Text: chunk.Text,
					ContentHash: chunk.ContentHash, SectionHeading: chunk.SectionHeading.String, Identity: identity,
				}
				pointID, err := uuid.Parse(embedding.EmbeddingPointID(item))
				if err != nil {
					return fmt.Errorf("parse deterministic point ID for chunk %s: %w", chunk.ID, err)
				}
				if _, err := q.MarkPaperChunkForActiveGeneration(ctx, postgres.MarkPaperChunkForActiveGenerationParams{
					TopicID: chunk.TopicID, ID: chunk.ID, ContentHash: chunk.ContentHash,
					EmbeddingProvider:           pgtype.Text{String: identity.Provider, Valid: true},
					EmbeddingModel:              pgtype.Text{String: identity.Model, Valid: true},
					EmbeddingDimensions:         pgtype.Int4{Int32: int32(identity.Dimensions), Valid: true},
					EmbeddingInstructionVersion: pgtype.Text{String: identity.InstructionVersion, Valid: true},
					EmbeddingIndexingVersion:    pgtype.Text{String: identity.IndexingVersion, Valid: true},
					QdrantCollection:            pgtype.Text{String: collection, Valid: true},
					QdrantPointID:               pgtype.UUID{Bytes: pointID, Valid: true},
				}); err != nil {
					return fmt.Errorf("activate snapshotted chunk %s: %w", chunk.ID, err)
				}
			}
			return nil
		})
	}); err != nil {
		return err
	}
	fmt.Fprintf(r.progress, "activated %s with %d points\n", collection, count)
	return nil
}

func validateChunkSnapshot(indexed, current []*postgres.PaperChunk) error {
	if len(indexed) != len(current) {
		return fmt.Errorf("%w: indexed %d chunks, found %d at activation", errChunkSnapshotChanged, len(indexed), len(current))
	}
	currentByID := make(map[uuid.UUID]*postgres.PaperChunk, len(current))
	for _, chunk := range current {
		currentByID[chunk.ID] = chunk
	}
	for _, chunk := range indexed {
		latest, ok := currentByID[chunk.ID]
		if !ok || !sameIndexedChunk(chunk, latest) {
			return fmt.Errorf("%w: chunk %s no longer matches", errChunkSnapshotChanged, chunk.ID)
		}
	}
	return nil
}

func sameIndexedChunk(indexed, current *postgres.PaperChunk) bool {
	return indexed.ID == current.ID && indexed.TopicID == current.TopicID && indexed.PaperID == current.PaperID &&
		indexed.ChunkType == current.ChunkType && indexed.ChunkIndex == current.ChunkIndex &&
		indexed.Text == current.Text && indexed.ContentHash == current.ContentHash &&
		indexed.SectionHeading == current.SectionHeading
}

func (r *Runner) removeUnexpectedPoints(ctx context.Context, expected map[string]struct{}) error {
	ids, err := r.vectors.ListPointIDs(ctx, 256)
	if err != nil {
		return fmt.Errorf("list rebuilt Qdrant points: %w", err)
	}
	stale := make([]*qdrant.PointId, 0)
	for _, id := range ids {
		value := id.GetUuid()
		if _, ok := expected[value]; !ok {
			stale = append(stale, id)
		}
	}
	if len(stale) == 0 {
		return nil
	}
	if err := r.vectors.DeleteFromCollection(ctx, r.vectors.PhysicalCollectionName(), stale); err != nil {
		return fmt.Errorf("remove %d stale points from inactive generation: %w", len(stale), err)
	}
	return nil
}

func activateGeneration(ctx context.Context, activateAlias func(context.Context) error, persistActivation func(context.Context) error) error {
	if err := activateAlias(ctx); err != nil {
		return fmt.Errorf("activate Qdrant alias: %w", err)
	}
	if err := persistActivation(ctx); err != nil {
		return fmt.Errorf("persist activated embedding generation: %w", err)
	}
	return nil
}
