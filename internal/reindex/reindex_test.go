package reindex

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/paper-scout/internal/storage/postgres"
)

type fakeActivationStore struct {
	intent                    *postgres.EmbeddingActivationIntent
	generation                *postgres.EmbeddingGeneration
	pendingErr                error
	finalizeErr               error
	completed                 int
	failed                    []error
	finalized                 int
	superseded                int
	failedGenerations         int
	finalizeBeforeActivateErr error
}

func (s *fakeActivationStore) PendingActivation(context.Context) (*postgres.EmbeddingActivationIntent, error) {
	return s.intent, s.pendingErr
}

func (s *fakeActivationStore) EmbeddingGeneration(context.Context, uuid.UUID) (*postgres.EmbeddingGeneration, error) {
	return s.generation, nil
}

func (s *fakeActivationStore) CompleteActivation(context.Context, uuid.UUID) error {
	s.completed++
	s.pendingErr = pgx.ErrNoRows
	return nil
}

func (s *fakeActivationStore) FailActivation(_ context.Context, _ uuid.UUID, cause error) error {
	s.failed = append(s.failed, cause)
	return nil
}

func (s *fakeActivationStore) SupersedeActivation(context.Context, uuid.UUID, error) error {
	s.superseded++
	return nil
}

func (s *fakeActivationStore) FailGeneration(context.Context, uuid.UUID, error) error {
	s.failedGenerations++
	return nil
}

func (s *fakeActivationStore) FinalizeActivation(ctx context.Context, _ *postgres.EmbeddingActivationIntent, _ *postgres.EmbeddingGeneration, activate func(context.Context) error) error {
	s.finalized++
	if s.finalizeBeforeActivateErr != nil {
		return s.finalizeBeforeActivateErr
	}
	if err := activate(ctx); err != nil {
		return err
	}
	return s.finalizeErr
}

type fakeActivationCollections struct {
	alias           string
	validateErr     error
	activationErr   error
	validationCalls int
	switches        int
	restores        int
}

func (c *fakeActivationCollections) ValidateCollection(context.Context, string, uint64) error {
	c.validationCalls++
	return c.validateErr
}

func (c *fakeActivationCollections) AliasName() string {
	return "current"
}

func (c *fakeActivationCollections) AliasTarget(context.Context) (string, error) {
	return c.alias, nil
}

func (c *fakeActivationCollections) ActivateExpected(_ context.Context, previous, target string) error {
	if c.activationErr != nil {
		return c.activationErr
	}
	if c.alias == target {
		return nil
	}
	if c.alias != previous {
		return fmt.Errorf("unexpected alias target %q", c.alias)
	}
	c.alias = target
	c.switches++
	return nil
}

func (c *fakeActivationCollections) RestoreExpected(_ context.Context, target, previous string) error {
	if c.alias == previous {
		return nil
	}
	if c.alias != target {
		return fmt.Errorf("unexpected alias target %q", c.alias)
	}
	c.alias = previous
	c.restores++
	return nil
}

// Protects persisted activation intent precedes every alias mutation.
func TestReconciliationSwitchesOnlyPersistedIntent(t *testing.T) {
	store, vectors := testActivationDependencies("previous")
	reconciler := &Reconciler{store: store, vectors: vectors}
	if err := reconciler.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile returned error: %v", err)
	}
	if vectors.switches != 1 || store.finalized != 1 || store.completed != 1 {
		t.Fatalf("switch/finalize/complete = %d/%d/%d, want 1/1/1", vectors.switches, store.finalized, store.completed)
	}
}

// Protects alias failures leave a durable retryable activation intent.
func TestReconciliationAliasFailureLeavesRetryableIntent(t *testing.T) {
	store, vectors := testActivationDependencies("previous")
	vectors.activationErr = errors.New("Qdrant unavailable")
	err := (&Reconciler{store: store, vectors: vectors}).Reconcile(context.Background())
	if !errors.Is(err, vectors.activationErr) {
		t.Fatalf("Reconcile error = %v, want alias failure", err)
	}
	if len(store.failed) != 1 || store.finalized != 1 || store.completed != 0 {
		t.Fatalf("failures/finalized/completed = %d/%d/%d, want 1/1/0", len(store.failed), store.finalized, store.completed)
	}
}

// Protects startup reconciliation completes an activation interrupted before alias switch.
func TestStartupReconciliationCompletesPendingAliasSwitch(t *testing.T) {
	store, vectors := testActivationDependencies("previous")
	if err := (&Reconciler{store: store, vectors: vectors}).Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile returned error: %v", err)
	}
	if vectors.alias != store.intent.TargetCollection || vectors.switches != 1 {
		t.Fatalf("alias/switches = %q/%d, want %q/1", vectors.alias, vectors.switches, store.intent.TargetCollection)
	}
}

// Protects a returned finalization failure restores the previous alias before retrying.
func TestReconciliationRestoresAliasAfterFinalizationFailure(t *testing.T) {
	store, vectors := testActivationDependencies("previous")
	store.finalizeErr = errors.New("PostgreSQL interrupted")
	reconciler := &Reconciler{store: store, vectors: vectors}
	if err := reconciler.Reconcile(context.Background()); !errors.Is(err, store.finalizeErr) {
		t.Fatalf("first Reconcile error = %v, want finalization failure", err)
	}
	store.finalizeErr = nil
	if err := reconciler.Reconcile(context.Background()); err != nil {
		t.Fatalf("retry Reconcile returned error: %v", err)
	}
	if vectors.switches != 2 || vectors.restores != 1 || store.finalized != 2 || store.completed != 1 {
		t.Fatalf("switches/restores/finalized/completed = %d/%d/%d/%d, want 2/1/2/1", vectors.switches, vectors.restores, store.finalized, store.completed)
	}
}

// Protects reconciliation finalizes an alias switched before PostgreSQL completion.
func TestReconciliationFinalizesAlreadySwitchedAlias(t *testing.T) {
	store, vectors := testActivationDependencies("target")
	if err := (&Reconciler{store: store, vectors: vectors}).Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile returned error: %v", err)
	}
	if vectors.switches != 0 || store.finalized != 1 || store.completed != 1 {
		t.Fatalf("switches/finalized/completed = %d/%d/%d, want 0/1/1", vectors.switches, store.finalized, store.completed)
	}
}

// Protects reconciliation is idempotent after PostgreSQL finalization completes.
func TestReconciliationIsIdempotentAfterFinalization(t *testing.T) {
	store, vectors := testActivationDependencies("target")
	store.generation.Status = "active"
	reconciler := &Reconciler{store: store, vectors: vectors}
	if err := reconciler.Reconcile(context.Background()); err != nil {
		t.Fatalf("first Reconcile returned error: %v", err)
	}
	if err := reconciler.Reconcile(context.Background()); err != nil {
		t.Fatalf("second Reconcile returned error: %v", err)
	}
	if vectors.validationCalls != 0 || vectors.switches != 0 || store.finalized != 0 || store.completed != 1 {
		t.Fatalf("validation/switches/finalized/completed = %d/%d/%d/%d, want 0/0/0/1", vectors.validationCalls, vectors.switches, store.finalized, store.completed)
	}
}

// Protects unexpected alias targets fail closed without alias mutation.
func TestReconciliationRejectsUnexpectedAliasTarget(t *testing.T) {
	store, vectors := testActivationDependencies("unrecognized")
	err := (&Reconciler{store: store, vectors: vectors}).Reconcile(context.Background())
	if err == nil || vectors.switches != 0 || store.finalized != 1 || len(store.failed) != 1 {
		t.Fatalf("error/switches/finalized/failures = %v/%d/%d/%d, want error/0/1/1", err, vectors.switches, store.finalized, len(store.failed))
	}
}

// Protects invalid target collections cannot be activated.
func TestReconciliationRejectsInvalidTargetCollection(t *testing.T) {
	for name, cause := range map[string]error{
		"missing collection":   errors.New("collection not found"),
		"schema mismatch":      errors.New("wrong vector dimensions"),
		"point-count mismatch": errors.New("expected 2 points"),
	} {
		t.Run(name, func(t *testing.T) {
			store, vectors := testActivationDependencies("previous")
			vectors.validateErr = cause
			err := (&Reconciler{store: store, vectors: vectors}).Reconcile(context.Background())
			if !errors.Is(err, cause) || vectors.switches != 0 || len(store.failed) != 1 {
				t.Fatalf("error/switches/failures = %v/%d/%d, want validation error/0/1", err, vectors.switches, len(store.failed))
			}
		})
	}
}

// Protects cancellation records the recoverable activation failure without mutation.
func TestReconciliationCancellationFailsWithoutAliasMutation(t *testing.T) {
	store, vectors := testActivationDependencies("previous")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	vectors.validateErr = ctx.Err()
	err := (&Reconciler{store: store, vectors: vectors}).Reconcile(ctx)
	if !errors.Is(err, context.Canceled) || vectors.switches != 0 || len(store.failed) != 1 {
		t.Fatalf("error/switches/failures = %v/%d/%d, want canceled/0/1", err, vectors.switches, len(store.failed))
	}
}

// Protects snapshot validation failures leave the previous alias active.
func TestReconciliationSnapshotFailureKeepsPreviousAlias(t *testing.T) {
	store, vectors := testActivationDependencies("previous")
	store.finalizeBeforeActivateErr = errChunkSnapshotChanged
	err := (&Reconciler{store: store, vectors: vectors}).Reconcile(context.Background())
	if !errors.Is(err, errChunkSnapshotChanged) {
		t.Fatalf("Reconcile error = %v, want snapshot failure", err)
	}
	if vectors.alias != "previous" || vectors.switches != 0 || vectors.restores != 0 || store.superseded != 1 || store.failedGenerations != 1 {
		t.Fatalf("alias/switches/restores/superseded/failed generations = %q/%d/%d/%d/%d, want previous/0/0/1/1", vectors.alias, vectors.switches, vectors.restores, store.superseded, store.failedGenerations)
	}
}

// Protects snapshot failures after a switched alias restore the previous generation.
func TestReconciliationSnapshotFailureRestoresSwitchedAlias(t *testing.T) {
	store, vectors := testActivationDependencies("target")
	store.finalizeBeforeActivateErr = errChunkSnapshotChanged
	err := (&Reconciler{store: store, vectors: vectors}).Reconcile(context.Background())
	if !errors.Is(err, errChunkSnapshotChanged) {
		t.Fatalf("Reconcile error = %v, want snapshot failure", err)
	}
	if vectors.alias != "previous" || vectors.restores != 1 || store.superseded != 1 {
		t.Fatalf("alias/restores/superseded = %q/%d/%d, want previous/1/1", vectors.alias, vectors.restores, store.superseded)
	}
}

// Protects activation rejects chunk snapshots changed after vector construction.
func TestValidateChunkSnapshotDigestRejectsConcurrentChanges(t *testing.T) {
	indexed := testReindexChunk()
	updated := *indexed
	updated.Text = "changed text"
	if err := validateChunkSnapshotDigest(chunkSnapshotDigest([]*postgres.PaperChunk{indexed}), []*postgres.PaperChunk{&updated}); !errors.Is(err, errChunkSnapshotChanged) {
		t.Fatalf("validateChunkSnapshotDigest error = %v, want snapshot change", err)
	}
}

func testActivationDependencies(alias string) (*fakeActivationStore, *fakeActivationCollections) {
	generationID := uuid.New()
	return &fakeActivationStore{
		intent: &postgres.EmbeddingActivationIntent{
			ID: uuid.New(), GenerationID: generationID, TargetCollection: "target", AliasName: "current",
			PreviousCollection:  pgtype.Text{String: "previous", Valid: true},
			ExpectedPointCount:  pgtype.Int8{Int64: 2, Valid: true},
			ChunkSnapshotDigest: pgtype.Text{String: "snapshot", Valid: true},
		},
		generation: &postgres.EmbeddingGeneration{ID: generationID, Status: "ready"},
	}, &fakeActivationCollections{alias: alias}
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
