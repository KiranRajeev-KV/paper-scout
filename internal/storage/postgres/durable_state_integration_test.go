package postgres

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestDurablePipelineFailureStateIsAtomic(t *testing.T) {
	dsn := os.Getenv("PAPER_SCOUT_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("PAPER_SCOUT_TEST_POSTGRES_DSN is not set")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to postgres: %v", err)
	}
	defer pool.Close()

	queries := New(pool)
	topic, err := queries.CreateResearchTopic(ctx, CreateResearchTopicParams{
		Topic:  "durable failure integration test",
		Status: "pending",
	})
	if err != nil {
		t.Fatalf("create topic: %v", err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM research_topics WHERE id = $1", topic.ID)
	})

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}
	qtx := New(tx)
	if _, err := qtx.StartPipelineStage(ctx, StartPipelineStageParams{
		RunID:   topic.RunID,
		TopicID: topic.ID,
		Stage:   "ranking",
	}); err != nil {
		t.Fatalf("start stage: %v", err)
	}
	if _, err := qtx.FailPipelineStage(ctx, FailPipelineStageParams{
		RunID:        topic.RunID,
		Stage:        "ranking",
		ErrorMessage: pgtype.Text{String: "gemini unavailable", Valid: true},
	}); err != nil {
		t.Fatalf("fail stage: %v", err)
	}
	if _, err := qtx.UpdateResearchTopicState(ctx, UpdateResearchTopicStateParams{
		ID:           topic.ID,
		Status:       "failed",
		CurrentStage: "ranking",
		Progress:     0,
		ErrorMessage: pgtype.Text{String: "gemini unavailable", Valid: true},
	}); err != nil {
		t.Fatalf("update topic state: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit transaction: %v", err)
	}

	persisted, err := queries.GetResearchTopic(ctx, topic.ID)
	if err != nil {
		t.Fatalf("load persisted topic: %v", err)
	}
	if persisted.Status != "failed" || persisted.CurrentStage != "ranking" || persisted.Progress != 0 {
		t.Fatalf("unexpected persisted topic state: %+v", persisted)
	}
	if !persisted.ErrorMessage.Valid || persisted.ErrorMessage.String != "gemini unavailable" {
		t.Fatalf("unexpected persisted error: %+v", persisted.ErrorMessage)
	}

	checkpoint, err := queries.GetPipelineStage(ctx, GetPipelineStageParams{
		RunID: topic.RunID,
		Stage: "ranking",
	})
	if err != nil {
		t.Fatalf("load persisted checkpoint: %v", err)
	}
	if checkpoint.Status != "failed" || !checkpoint.ErrorMessage.Valid {
		t.Fatalf("unexpected persisted checkpoint: %+v", checkpoint)
	}

	rollbackTopic, err := queries.CreateResearchTopic(ctx, CreateResearchTopicParams{
		Topic:  "durable rollback integration test",
		Status: "pending",
	})
	if err != nil {
		t.Fatalf("create rollback topic: %v", err)
	}
	rollbackTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin rollback transaction: %v", err)
	}
	rollbackQueries := New(rollbackTx)
	if _, err := rollbackQueries.UpdateResearchTopicState(ctx, UpdateResearchTopicStateParams{
		ID:           rollbackTopic.ID,
		Status:       "failed",
		CurrentStage: "ranking",
		Progress:     0,
		ErrorMessage: pgtype.Text{String: "must roll back", Valid: true},
	}); err != nil {
		t.Fatalf("update rollback topic: %v", err)
	}
	if err := rollbackTx.Rollback(ctx); err != nil {
		t.Fatalf("rollback transaction: %v", err)
	}
	defer func() {
		_, _ = pool.Exec(ctx, "DELETE FROM research_topics WHERE id = $1", rollbackTopic.ID)
	}()
	if _, err := queries.GetResearchTopic(ctx, rollbackTopic.ID); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("rollback topic lookup error = %v, want pgx.ErrNoRows", err)
	}

}
