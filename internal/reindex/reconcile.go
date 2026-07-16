package reindex

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/paper-scout/internal/logger"
	"github.com/paper-scout/internal/storage/postgres"
	qdrantstore "github.com/paper-scout/internal/storage/qdrant"
)

type activationStore interface {
	PendingActivation(context.Context) (*postgres.EmbeddingActivationIntent, error)
	EmbeddingGeneration(context.Context, uuid.UUID) (*postgres.EmbeddingGeneration, error)
	CompleteActivation(context.Context, uuid.UUID) error
	FailActivation(context.Context, uuid.UUID, error) error
	SupersedeActivation(context.Context, uuid.UUID, error) error
	FailGeneration(context.Context, uuid.UUID, error) error
	FinalizeActivation(context.Context, *postgres.EmbeddingActivationIntent, *postgres.EmbeddingGeneration, func(context.Context) error) error
}

type activationCollections interface {
	ValidateCollection(context.Context, string, uint64) error
	AliasName() string
	AliasTarget(context.Context) (string, error)
	ActivateExpected(context.Context, string, string) error
	RestoreExpected(context.Context, string, string) error
}

type postgresActivationStore struct {
	db *postgres.Client
}

func (s postgresActivationStore) PendingActivation(ctx context.Context) (*postgres.EmbeddingActivationIntent, error) {
	return s.db.Queries().GetPendingEmbeddingActivationIntent(ctx)
}

func (s postgresActivationStore) EmbeddingGeneration(ctx context.Context, generationID uuid.UUID) (*postgres.EmbeddingGeneration, error) {
	return s.db.Queries().GetEmbeddingGeneration(ctx, generationID)
}

func (s postgresActivationStore) CompleteActivation(ctx context.Context, intentID uuid.UUID) error {
	_, err := s.db.Queries().CompleteEmbeddingActivationIntent(ctx, intentID)
	return err
}

func (s postgresActivationStore) FailActivation(ctx context.Context, intentID uuid.UUID, cause error) error {
	_, err := s.db.Queries().FailEmbeddingActivationIntent(ctx, postgres.FailEmbeddingActivationIntentParams{
		ID: intentID, LastError: pgtype.Text{String: cause.Error(), Valid: true},
	})
	return err
}

func (s postgresActivationStore) SupersedeActivation(ctx context.Context, intentID uuid.UUID, cause error) error {
	_, err := s.db.Queries().SupersedeEmbeddingActivationIntent(ctx, postgres.SupersedeEmbeddingActivationIntentParams{ID: intentID, LastError: pgtype.Text{String: cause.Error(), Valid: true}})
	return err
}

func (s postgresActivationStore) FailGeneration(ctx context.Context, generationID uuid.UUID, cause error) error {
	_, err := s.db.Queries().FailEmbeddingGeneration(ctx, postgres.FailEmbeddingGenerationParams{ID: generationID, ErrorMessage: pgtype.Text{String: cause.Error(), Valid: true}})
	return err
}

func (s postgresActivationStore) FinalizeActivation(ctx context.Context, intent *postgres.EmbeddingActivationIntent, generation *postgres.EmbeddingGeneration, activate func(context.Context) error) error {
	return finalizeDurableActivation(ctx, s.db, intent, generation, activate)
}

// Reconciler completes or safely retries one durable Qdrant activation intent.
type Reconciler struct {
	store   activationStore
	vectors activationCollections
}

// NewReconciler creates a reconciler with explicit PostgreSQL and Qdrant ownership.
func NewReconciler(db *postgres.Client, vectors *qdrantstore.Client) (*Reconciler, error) {
	if db == nil || vectors == nil {
		return nil, fmt.Errorf("activation reconciler requires PostgreSQL and Qdrant")
	}
	return &Reconciler{store: postgresActivationStore{db: db}, vectors: vectors}, nil
}

// Reconcile repairs the pending activation, returning nil when no intent exists.
func (r *Reconciler) Reconcile(ctx context.Context) error {
	intent, err := r.store.PendingActivation(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("load pending embedding activation: %w", err)
	}
	if !intent.ExpectedPointCount.Valid || !intent.ChunkSnapshotDigest.Valid {
		err := fmt.Errorf("activation intent %s lacks immutable target validation; run a new reindex", intent.ID)
		r.failIntent(ctx, intent.ID, err)
		return err
	}
	if intent.AliasName != r.vectors.AliasName() {
		err := fmt.Errorf("activation intent %s expects alias %q, but Qdrant client manages %q", intent.ID, intent.AliasName, r.vectors.AliasName())
		r.failIntent(ctx, intent.ID, err)
		return err
	}
	generation, err := r.store.EmbeddingGeneration(ctx, intent.GenerationID)
	if err != nil {
		return fmt.Errorf("load activation generation: %w", err)
	}
	current, err := r.vectors.AliasTarget(ctx)
	if err != nil {
		return fmt.Errorf("inspect Qdrant alias during reconciliation: %w", err)
	}
	if generation.Status == "active" && current == intent.TargetCollection {
		if err := r.store.CompleteActivation(ctx, intent.ID); err != nil {
			return fmt.Errorf("complete already-finalized activation intent: %w", err)
		}
		return nil
	}
	if generation.Status != "ready" {
		err := fmt.Errorf("activation generation %s is %q; expected ready", generation.ID, generation.Status)
		r.failIntent(ctx, intent.ID, err)
		return err
	}
	if err := r.vectors.ValidateCollection(ctx, intent.TargetCollection, uint64(intent.ExpectedPointCount.Int64)); err != nil {
		r.failIntent(ctx, intent.ID, err)
		return fmt.Errorf("target collection is not activatable: %w", err)
	}
	previous := ""
	if intent.PreviousCollection.Valid {
		previous = intent.PreviousCollection.String
	}
	aliasSwitched := false
	if err := r.store.FinalizeActivation(ctx, intent, generation, func(activationCtx context.Context) error {
		if current == intent.TargetCollection {
			return nil
		}
		if err := r.vectors.ActivateExpected(activationCtx, previous, intent.TargetCollection); err != nil {
			return fmt.Errorf("reconcile Qdrant alias from %q: %w", current, err)
		}
		aliasSwitched = true
		return nil
	}); err != nil {
		if errors.Is(err, errChunkSnapshotChanged) {
			if current == intent.TargetCollection {
				if restoreErr := r.restoreAlias(ctx, intent.TargetCollection, previous); restoreErr != nil {
					err = errors.Join(err, fmt.Errorf("restore previous Qdrant alias: %w", restoreErr))
				} else {
					r.supersedeIntent(ctx, intent, err)
					return fmt.Errorf("activation snapshot changed; restored previous alias: %w", err)
				}
			} else {
				r.supersedeIntent(ctx, intent, err)
				return fmt.Errorf("activation snapshot changed before alias switch: %w", err)
			}
		}
		if aliasSwitched {
			if restoreErr := r.restoreAlias(ctx, intent.TargetCollection, previous); restoreErr != nil {
				err = errors.Join(err, fmt.Errorf("restore previous Qdrant alias: %w", restoreErr))
			}
		}
		r.failIntent(ctx, intent.ID, err)
		return fmt.Errorf("finalize reconciled embedding activation: %w", err)
	}
	if err := r.store.CompleteActivation(ctx, intent.ID); err != nil {
		return fmt.Errorf("complete reconciled embedding activation: %w", err)
	}
	return nil
}

func (r *Reconciler) restoreAlias(ctx context.Context, target, previous string) error {
	restoreCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return r.vectors.RestoreExpected(restoreCtx, target, previous)
}

func (r *Reconciler) supersedeIntent(ctx context.Context, intent *postgres.EmbeddingActivationIntent, cause error) {
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := r.store.FailGeneration(persistCtx, intent.GenerationID, cause); err != nil {
		logger.From(ctx).Warn().Err(err).Str("generation_id", intent.GenerationID.String()).Msg("Failed to mark superseded embedding generation")
	}
	if err := r.store.SupersedeActivation(persistCtx, intent.ID, cause); err != nil {
		logger.From(ctx).Warn().Err(err).Str("intent_id", intent.ID.String()).Msg("Failed to supersede activation intent")
	}
}

func (r *Reconciler) failIntent(ctx context.Context, intentID uuid.UUID, cause error) {
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_ = r.store.FailActivation(persistCtx, intentID, cause)
}
