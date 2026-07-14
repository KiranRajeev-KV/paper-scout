package reindex

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/paper-scout/internal/storage/postgres"
)

// Protects activation persists only after alias switch.
func TestActivationPersistsOnlyAfterAliasSwitch(t *testing.T) {
	aliasErr := errors.New("alias switch failed")
	persisted := false
	err := activateGeneration(context.Background(), func(context.Context) error { return aliasErr }, func(context.Context) error { persisted = true; return nil })
	if !errors.Is(err, aliasErr) || persisted {
		t.Fatalf("activation error/persisted = %v/%v, want alias error and no durable activation", err, persisted)
	}
}

// Protects activation accepts an unchanged indexed snapshot.
func TestValidateChunkSnapshotAcceptsUnchangedRows(t *testing.T) {
	indexed := testReindexChunk()
	current := *indexed
	if err := validateChunkSnapshot([]*postgres.PaperChunk{indexed}, []*postgres.PaperChunk{&current}); err != nil {
		t.Fatalf("validateChunkSnapshot returned error: %v", err)
	}
}

// Protects activation rejects rows inserted or updated after indexing.
func TestValidateChunkSnapshotRejectsConcurrentChanges(t *testing.T) {
	indexed := testReindexChunk()
	inserted := testReindexChunk()
	updated := *indexed
	updated.Text = "changed text"
	updated.ContentHash = "changed-hash"

	for name, current := range map[string][]*postgres.PaperChunk{
		"inserted row": {indexed, inserted},
		"updated row":  {&updated},
	} {
		t.Run(name, func(t *testing.T) {
			err := validateChunkSnapshot([]*postgres.PaperChunk{indexed}, current)
			if !errors.Is(err, errChunkSnapshotChanged) {
				t.Fatalf("validateChunkSnapshot error = %v, want snapshot change", err)
			}
		})
	}
}

// Protects activation retry completes durable state.
func TestActivationRetryCompletesDurableState(t *testing.T) {
	aliasCalls := 0
	persistCalls := 0
	persistErr := errors.New("database interrupted")
	activate := func(context.Context) error { aliasCalls++; return nil }
	persist := func(context.Context) error {
		persistCalls++
		if persistCalls == 1 {
			return persistErr
		}
		return nil
	}
	if err := activateGeneration(context.Background(), activate, persist); !errors.Is(err, persistErr) {
		t.Fatalf("first activation error = %v, want database interruption", err)
	}
	if err := activateGeneration(context.Background(), activate, persist); err != nil {
		t.Fatalf("retry activation returned error: %v", err)
	}
	if aliasCalls != 2 || persistCalls != 2 {
		t.Fatalf("activation calls = %d/%d, want idempotent retry of both boundaries", aliasCalls, persistCalls)
	}
}

func testReindexChunk() *postgres.PaperChunk {
	return &postgres.PaperChunk{
		ID:             uuid.New(),
		TopicID:        uuid.New(),
		PaperID:        uuid.New(),
		ChunkType:      "pdf",
		ChunkIndex:     1,
		Text:           "indexed text",
		ContentHash:    "content-hash",
		SectionHeading: pgtype.Text{String: "Methods", Valid: true},
	}
}
