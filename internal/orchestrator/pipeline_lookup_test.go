package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/paper-scout/internal/storage/postgres"
)

// Protects get pipeline falls back to durable topic.
func TestGetPipelineFallsBackToDurableTopic(t *testing.T) {
	topicID := uuid.New()
	runID := uuid.New()
	o := &Orchestrator{pipelines: make(map[string]*Pipeline)}
	o.loadStateFn = func(context.Context, string) (*Pipeline, error) {
		return nil, ErrStateNotFound
	}
	o.getResearchTopicFn = func(_ context.Context, id uuid.UUID) (*postgres.ResearchTopic, error) {
		if id != topicID {
			t.Fatalf("topic ID = %s, want %s", id, topicID)
		}
		return &postgres.ResearchTopic{
			ID:           topicID,
			RunID:        runID,
			Topic:        "durable topic",
			Status:       "processing",
			CurrentStage: string(StageRanking),
			Progress:     0.25,
			CreatedAt:    pgtype.Timestamptz{Time: time.Unix(10, 0), Valid: true},
			UpdatedAt:    pgtype.Timestamptz{Time: time.Unix(20, 0), Valid: true},
		}, nil
	}

	pipeline, err := o.GetPipeline(context.Background(), topicID.String())
	if err != nil {
		t.Fatalf("GetPipeline returned error: %v", err)
	}
	if pipeline.RunID != runID.String() || pipeline.Stage != StageRanking || pipeline.Topic != "durable topic" {
		t.Fatalf("pipeline = %+v, want durable topic state", pipeline)
	}
}

// Protects get pipeline distinguishes invalid missing and infrastructure errors.
func TestGetPipelineDistinguishesInvalidMissingAndInfrastructureErrors(t *testing.T) {
	o := &Orchestrator{pipelines: make(map[string]*Pipeline)}
	if _, err := o.GetPipeline(context.Background(), "not-a-uuid"); !errors.Is(err, ErrInvalidTopicID) {
		t.Fatalf("invalid ID error = %v, want ErrInvalidTopicID", err)
	}

	topicID := uuid.New()
	o.getResearchTopicFn = func(context.Context, uuid.UUID) (*postgres.ResearchTopic, error) {
		return nil, pgx.ErrNoRows
	}
	if _, err := o.GetPipeline(context.Background(), topicID.String()); !errors.Is(err, ErrPipelineNotFound) {
		t.Fatalf("missing topic error = %v, want ErrPipelineNotFound", err)
	}

	databaseErr := errors.New("database unavailable")
	o.getResearchTopicFn = func(context.Context, uuid.UUID) (*postgres.ResearchTopic, error) {
		return nil, databaseErr
	}
	if _, err := o.GetPipeline(context.Background(), topicID.String()); !errors.Is(err, databaseErr) {
		t.Fatalf("infrastructure error = %v, want wrapped database error", err)
	}
}
