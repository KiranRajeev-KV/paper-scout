package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/paper-scout/internal/logger"
	"github.com/paper-scout/internal/storage/postgres"
)

// RunCoordinator owns the admission and launch of new research runs.
type RunCoordinator struct {
	postgres *postgres.Client
	logs     *logger.Manager
	state    *PipelineStateService
	runs     *RunManager
	runner   *PipelineRunner
}

// NewRunCoordinator constructs a coordinator for new research runs.
func NewRunCoordinator(pg *postgres.Client, logs *logger.Manager, state *PipelineStateService, runs *RunManager, runner *PipelineRunner) (*RunCoordinator, error) {
	if pg == nil || logs == nil || state == nil || runs == nil || runner == nil {
		return nil, fmt.Errorf("run coordinator requires postgres, logs, state, run manager, and runner")
	}
	return &RunCoordinator{postgres: pg, logs: logs, state: state, runs: runs, runner: runner}, nil
}

// Start creates durable run state, opens its log, and launches its pipeline.
func (c *RunCoordinator) Start(ctx context.Context, topic string) (*Pipeline, error) {
	topicRecord, err := c.postgres.Queries().CreateResearchTopic(ctx, postgres.CreateResearchTopicParams{Topic: topic, Status: "pending"})
	if err != nil {
		return nil, fmt.Errorf("create research topic: %w", err)
	}
	pipeline := &Pipeline{TopicID: topicRecord.ID.String(), RunID: topicRecord.RunID.String(), Topic: topic, Status: "pending", Stage: StagePending, StartedAt: time.Now(), UpdatedAt: time.Now()}
	if err := c.logs.StartRun(pipeline.RunID, pipeline.TopicID); err != nil {
		if persistErr := c.persistRunLogFailure(ctx, topicRecord.ID, pipeline, err); persistErr != nil {
			return nil, errors.Join(fmt.Errorf("create run log: %w", err), fmt.Errorf("persist run log failure: %w", persistErr))
		}
		return nil, fmt.Errorf("create run log: %w", err)
	}
	c.state.Remember(pipeline)
	if err := c.state.Save(ctx, pipeline); err != nil {
		c.runner.fail(ctx, pipeline, StagePending, fmt.Errorf("persist initial live pipeline state: %w", err))
		return nil, fmt.Errorf("start research pipeline: %w", err)
	}
	c.runs.Launch(pipeline)
	return clonePipeline(pipeline), nil
}

func (c *RunCoordinator) persistRunLogFailure(ctx context.Context, topicID uuid.UUID, pipeline *Pipeline, cause error) error {
	pipeline.Status, pipeline.Stage, pipeline.Error = "failed", StageFailed, cause.Error()
	_, err := c.postgres.Queries().UpdateResearchTopicState(ctx, postgres.UpdateResearchTopicStateParams{
		ID: topicID, Status: "failed", CurrentStage: string(StageFailed), ErrorMessage: pgtype.Text{String: cause.Error(), Valid: true},
	})
	if err != nil {
		logger.From(ctx).Error().Err(errors.Join(cause, err)).Msg("Failed to persist run log creation failure")
	}
	return err
}
