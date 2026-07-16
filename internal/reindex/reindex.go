// Package reindex rebuilds and atomically activates embedding generations.
package reindex

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

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

// NewRunner creates a reindex runner with the required durable and vector dependencies.
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

// Run rebuilds and recoverably activates one new embedding generation.
func (r *Runner) Run(ctx context.Context) error {
	reconciler, err := NewReconciler(r.db, r.vectors)
	if err != nil {
		return err
	}
	if err := reconciler.Reconcile(ctx); err != nil {
		return fmt.Errorf("reconcile existing embedding activation: %w", err)
	}

	identity := r.embeddings.Identity()
	chunks, err := r.db.Queries().ListPaperChunksForReindex(ctx)
	if err != nil {
		return fmt.Errorf("list durable chunks: %w", err)
	}
	if len(chunks) == 0 {
		return fmt.Errorf("no durable chunks are available to reindex")
	}

	generationID := uuid.New()
	collection := r.vectors.CollectionNameForGeneration(generationID)
	generation, err := r.db.Queries().CreateEmbeddingGeneration(ctx, postgres.CreateEmbeddingGenerationParams{
		ID:       generationID,
		Provider: identity.Provider, Model: identity.Model, Dimensions: int32(identity.Dimensions),
		InstructionVersion: identity.InstructionVersion, IndexingVersion: identity.IndexingVersion,
		CollectionName: collection,
	})
	if err != nil {
		return fmt.Errorf("create embedding generation ledger: %w", err)
	}
	if _, err := r.vectors.PrepareGeneration(ctx, generation.ID); err != nil {
		r.failGeneration(ctx, generation.ID, err)
		return fmt.Errorf("prepare Qdrant generation: %w", err)
	}
	expected := make(map[string]struct{}, len(chunks))
	for start := 0; start < len(chunks); start += r.batchSize {
		if err := ctx.Err(); err != nil {
			r.failGeneration(ctx, generation.ID, err)
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
			r.failGeneration(ctx, generation.ID, err)
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
			r.failGeneration(ctx, generation.ID, err)
			return fmt.Errorf("write chunks %d-%d: %w", start+1, end, err)
		}
		if _, err := r.db.Queries().UpdateEmbeddingGenerationProgress(ctx, postgres.UpdateEmbeddingGenerationProgressParams{
			ID: generation.ID, IndexedChunks: int64(end),
		}); err != nil {
			r.failGeneration(ctx, generation.ID, err)
			return fmt.Errorf("record reindex progress: %w", err)
		}
		fmt.Fprintf(r.progress, "indexed %d/%d chunks\n", end, len(chunks))
	}
	if err := r.removeUnexpectedPoints(ctx, expected); err != nil {
		r.failGeneration(ctx, generation.ID, err)
		return err
	}
	count, err := r.vectors.Count(ctx, nil)
	if err != nil {
		r.failGeneration(ctx, generation.ID, err)
		return fmt.Errorf("count rebuilt Qdrant points: %w", err)
	}
	if count != uint64(len(chunks)) {
		err := fmt.Errorf("rebuilt collection contains %d points; expected %d; activation refused", count, len(chunks))
		r.failGeneration(ctx, generation.ID, err)
		return err
	}
	snapshotDigest := chunkSnapshotDigest(chunks)
	if _, err := r.db.Queries().MarkEmbeddingGenerationReady(ctx, postgres.MarkEmbeddingGenerationReadyParams{ID: generation.ID, IndexedChunks: int64(count)}); err != nil {
		r.failGeneration(ctx, generation.ID, err)
		return fmt.Errorf("mark embedding generation ready: %w", err)
	}
	previous, err := r.vectors.AliasTarget(ctx)
	if err != nil {
		r.failGeneration(ctx, generation.ID, err)
		return fmt.Errorf("inspect Qdrant alias before activation: %w", err)
	}
	intent, err := r.db.Queries().CreateEmbeddingActivationIntent(ctx, postgres.CreateEmbeddingActivationIntentParams{
		GenerationID: generation.ID, TargetCollection: collection, AliasName: r.vectors.AliasName(),
		PreviousCollection:  pgtype.Text{String: previous, Valid: previous != ""},
		ExpectedPointCount:  pgtype.Int8{Int64: int64(count), Valid: true},
		ChunkSnapshotDigest: pgtype.Text{String: snapshotDigest, Valid: true},
	})
	if err != nil {
		r.failGeneration(ctx, generation.ID, err)
		return fmt.Errorf("persist embedding activation intent: %w", err)
	}
	aliasSwitched := false
	if err := finalizeDurableActivation(ctx, r.db, intent, generation, func(activationCtx context.Context) error {
		if err := r.vectors.ActivateExpected(activationCtx, previous, collection); err != nil {
			return fmt.Errorf("activate Qdrant alias: %w", err)
		}
		aliasSwitched = true
		return nil
	}); err != nil {
		restored := !aliasSwitched
		if aliasSwitched {
			if restoreErr := r.restoreAlias(ctx, collection, previous); restoreErr != nil {
				err = errors.Join(err, fmt.Errorf("restore previous Qdrant alias: %w", restoreErr))
			} else {
				restored = true
			}
		}
		if errors.Is(err, errChunkSnapshotChanged) && restored {
			r.failGeneration(ctx, generation.ID, err)
			r.supersedeIntent(ctx, intent.ID, err)
			return fmt.Errorf("activation snapshot changed; target generation was not retained: %w", err)
		}
		r.failIntent(ctx, intent.ID, err)
		return fmt.Errorf("finalize embedding generation: %w", err)
	}
	if _, err := r.db.Queries().CompleteEmbeddingActivationIntent(ctx, intent.ID); err != nil {
		return fmt.Errorf("complete embedding activation intent: %w", err)
	}
	fmt.Fprintf(r.progress, "activated %s with %d points\n", collection, count)
	return nil
}

func (r *Runner) failGeneration(ctx context.Context, generationID uuid.UUID, cause error) {
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_, _ = r.db.Queries().FailEmbeddingGeneration(persistCtx, postgres.FailEmbeddingGenerationParams{
		ID: generationID, ErrorMessage: pgtype.Text{String: cause.Error(), Valid: true},
	})
}

func (r *Runner) failIntent(ctx context.Context, intentID uuid.UUID, cause error) {
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_, _ = r.db.Queries().FailEmbeddingActivationIntent(persistCtx, postgres.FailEmbeddingActivationIntentParams{
		ID: intentID, LastError: pgtype.Text{String: cause.Error(), Valid: true},
	})
}

func (r *Runner) supersedeIntent(ctx context.Context, intentID uuid.UUID, cause error) {
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_, _ = r.db.Queries().SupersedeEmbeddingActivationIntent(persistCtx, postgres.SupersedeEmbeddingActivationIntentParams{
		ID: intentID, LastError: pgtype.Text{String: cause.Error(), Valid: true},
	})
}

func (r *Runner) restoreAlias(ctx context.Context, target, previous string) error {
	restoreCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return r.vectors.RestoreExpected(restoreCtx, target, previous)
}

func finalizeDurableActivation(ctx context.Context, db *postgres.Client, intent *postgres.EmbeddingActivationIntent, generation *postgres.EmbeddingGeneration, activate func(context.Context) error) error {
	identity := embedding.Identity{Provider: generation.Provider, Model: generation.Model, Dimensions: int(generation.Dimensions), InstructionVersion: generation.InstructionVersion, IndexingVersion: generation.IndexingVersion}
	return db.WithTx(ctx, func(q *postgres.Queries) error {
		if err := q.LockPaperChunksForEmbeddingActivation(ctx); err != nil {
			return fmt.Errorf("lock durable chunks for activation: %w", err)
		}
		current, err := q.ListPaperChunksForReindex(ctx)
		if err != nil {
			return fmt.Errorf("revalidate durable chunks before activation: %w", err)
		}
		if err := validateChunkSnapshotDigest(intent.ChunkSnapshotDigest.String, current); err != nil {
			return err
		}
		if err := activate(ctx); err != nil {
			return err
		}
		if _, err := q.MarkEmbeddingActivationAliasSwitched(ctx, intent.ID); err != nil {
			return fmt.Errorf("record Qdrant alias switch: %w", err)
		}
		return finalizeGeneration(ctx, q, generation, current, identity, intent.TargetCollection)
	})
}

func finalizeGeneration(ctx context.Context, q *postgres.Queries, generation *postgres.EmbeddingGeneration, chunks []*postgres.PaperChunk, identity embedding.Identity, collection string) error {
	if err := q.RetireActiveEmbeddingGenerations(ctx, generation.ID); err != nil {
		return err
	}
	if _, err := q.ActivateEmbeddingGeneration(ctx, generation.ID); err != nil {
		return err
	}
	for _, chunk := range chunks {
		item := embedding.PaperEmbedding{ChunkID: chunk.ID.String(), PaperID: chunk.PaperID.String(), TopicID: chunk.TopicID.String(), ChunkType: chunk.ChunkType, ChunkIndex: int(chunk.ChunkIndex), Text: chunk.Text, ContentHash: chunk.ContentHash, SectionHeading: chunk.SectionHeading.String, Identity: identity}
		pointID, err := uuid.Parse(embedding.EmbeddingPointID(item))
		if err != nil {
			return fmt.Errorf("parse deterministic point ID for chunk %s: %w", chunk.ID, err)
		}
		if _, err := q.MarkPaperChunkForActiveGeneration(ctx, postgres.MarkPaperChunkForActiveGenerationParams{
			TopicID: chunk.TopicID, ID: chunk.ID, ContentHash: chunk.ContentHash,
			EmbeddingProvider: pgtype.Text{String: identity.Provider, Valid: true}, EmbeddingModel: pgtype.Text{String: identity.Model, Valid: true}, EmbeddingDimensions: pgtype.Int4{Int32: int32(identity.Dimensions), Valid: true},
			EmbeddingInstructionVersion: pgtype.Text{String: identity.InstructionVersion, Valid: true}, EmbeddingIndexingVersion: pgtype.Text{String: identity.IndexingVersion, Valid: true}, QdrantCollection: pgtype.Text{String: collection, Valid: true}, QdrantPointID: pgtype.UUID{Bytes: pointID, Valid: true},
		}); err != nil {
			return fmt.Errorf("activate snapshotted chunk %s: %w", chunk.ID, err)
		}
	}
	return nil
}

func validateChunkSnapshotDigest(expected string, current []*postgres.PaperChunk) error {
	if actual := chunkSnapshotDigest(current); actual != expected {
		return fmt.Errorf("%w: expected digest %s, found %s", errChunkSnapshotChanged, expected, actual)
	}
	return nil
}

func chunkSnapshotDigest(chunks []*postgres.PaperChunk) string {
	ordered := append([]*postgres.PaperChunk(nil), chunks...)
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].ID.String() < ordered[j].ID.String()
	})
	hash := sha256.New()
	for _, chunk := range ordered {
		fmt.Fprintf(hash, "%s\x00%s\x00%s\x00%s\x00%d\x00%s\x00%s\x00%s\x00%t\x00",
			chunk.ID, chunk.TopicID, chunk.PaperID, chunk.ChunkType, chunk.ChunkIndex,
			chunk.Text, chunk.ContentHash, chunk.SectionHeading.String, chunk.SectionHeading.Valid)
	}
	return hex.EncodeToString(hash.Sum(nil))
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
