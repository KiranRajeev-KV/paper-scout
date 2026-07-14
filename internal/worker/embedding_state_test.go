package worker

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/paper-scout/internal/storage/postgres"
	"github.com/paper-scout/internal/tools/embedding"
)

// Protects current chunk embedding requires exact identity.
func TestCurrentChunkEmbeddingRequiresExactIdentity(t *testing.T) {
	identity := embedding.Identity{Provider: "ollama", Model: "qwen3-embedding:8b", Dimensions: 4096, InstructionVersion: "qwen-v1", IndexingVersion: "v1"}
	chunk := &postgres.PaperChunk{
		ContentHash: "hash", EmbeddingStatus: "completed",
		EmbeddedContentHash:         pgtype.Text{String: "hash", Valid: true},
		EmbeddingProvider:           pgtype.Text{String: identity.Provider, Valid: true},
		EmbeddingModel:              pgtype.Text{String: identity.Model, Valid: true},
		EmbeddingDimensions:         pgtype.Int4{Int32: int32(identity.Dimensions), Valid: true},
		EmbeddingInstructionVersion: pgtype.Text{String: identity.InstructionVersion, Valid: true},
		EmbeddingIndexingVersion:    pgtype.Text{String: identity.IndexingVersion, Valid: true},
		QdrantCollection:            pgtype.Text{String: "generation-a", Valid: true},
		QdrantPointID:               pgtype.UUID{Bytes: uuid.New(), Valid: true},
	}
	if !currentChunkEmbedding(chunk, identity, "generation-a") {
		t.Fatal("matching chunk was treated as stale")
	}
	changed := identity
	changed.InstructionVersion = "qwen-v2"
	if currentChunkEmbedding(chunk, changed, "generation-a") {
		t.Fatal("instruction-version mismatch was treated as current")
	}
	if currentChunkEmbedding(chunk, identity, "generation-b") {
		t.Fatal("collection mismatch was treated as current")
	}
}

// Protects cleanup failure is recorded without invalidating replacement.
func TestCleanupFailureIsRecordedWithoutInvalidatingReplacement(t *testing.T) {
	task := &postgres.EmbeddingCleanupTask{ID: uuid.New(), PointID: uuid.New(), CollectionName: "old-generation"}
	deleteErr := errors.New("Qdrant unavailable")
	var failed uuid.UUID
	result, err := reconcileCleanupTasks(context.Background(), []*postgres.EmbeddingCleanupTask{task},
		func(context.Context, string, string) error { return deleteErr },
		func(context.Context, uuid.UUID) error { t.Fatal("failed cleanup was marked complete"); return nil },
		func(_ context.Context, id uuid.UUID, cause error) error {
			failed = id
			if !errors.Is(cause, deleteErr) {
				t.Fatalf("cause = %v, want delete error", cause)
			}
			return nil
		},
	)
	if err != nil {
		t.Fatalf("reconcileCleanupTasks returned persistence error: %v", err)
	}
	if result.Completed != 0 || result.Pending != 1 || failed != task.ID {
		t.Fatalf("result/failed = %+v/%s, want one recorded retry", result, failed)
	}
}
