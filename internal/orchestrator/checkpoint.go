package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/paper-scout/internal/logger"
	"github.com/paper-scout/internal/storage/postgres"
)

// CheckpointService owns durable pipeline-stage transitions.
type CheckpointService struct {
	postgres *postgres.Client
}

// NewCheckpointService constructs a checkpoint service backed by PostgreSQL.
func NewCheckpointService(pg *postgres.Client) (*CheckpointService, error) {
	if pg == nil {
		return nil, fmt.Errorf("checkpoint service requires postgres")
	}
	return &CheckpointService{postgres: pg}, nil
}

func (s *CheckpointService) stageCompleted(ctx context.Context, p *Pipeline, stage Stage, output interface{}) (bool, error) {
	runID, err := uuid.Parse(p.RunID)
	if err != nil {
		return false, fmt.Errorf("invalid pipeline run ID %q: %w", p.RunID, err)
	}
	checkpoint, err := s.postgres.Queries().GetPipelineStage(ctx, postgres.GetPipelineStageParams{
		RunID: runID,
		Stage: string(stage),
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("load %s checkpoint: %w", stage, err)
	}
	if checkpoint.Status != "completed" {
		return false, nil
	}
	if output == nil || len(checkpoint.Output) == 0 {
		return true, nil
	}
	if err := json.Unmarshal(checkpoint.Output, output); err != nil {
		return false, fmt.Errorf("decode %s checkpoint: %w", stage, err)
	}
	return true, nil
}

func (s *CheckpointService) startStage(ctx context.Context, p *Pipeline, stage Stage) error {
	runID, topicID, err := pipelineIDs(p)
	if err != nil {
		return err
	}
	err = s.postgres.WithTx(ctx, func(q *postgres.Queries) error {
		if _, err := q.StartPipelineStage(ctx, postgres.StartPipelineStageParams{
			RunID:   runID,
			TopicID: topicID,
			Stage:   string(stage),
		}); err != nil {
			return fmt.Errorf("start %s checkpoint: %w", stage, err)
		}
		return updateTopicState(ctx, q, p)
	})
	return err
}

func (s *CheckpointService) completeStage(ctx context.Context, p *Pipeline, stage Stage, output interface{}) error {
	runID, _, err := pipelineIDs(p)
	if err != nil {
		return err
	}
	data, err := json.Marshal(output)
	if err != nil {
		return fmt.Errorf("encode %s checkpoint: %w", stage, err)
	}
	err = s.postgres.WithTx(ctx, func(q *postgres.Queries) error {
		if _, err := q.CompletePipelineStage(ctx, postgres.CompletePipelineStageParams{
			RunID:  runID,
			Stage:  string(stage),
			Output: data,
		}); err != nil {
			return fmt.Errorf("complete %s checkpoint: %w", stage, err)
		}
		return updateTopicState(ctx, q, p)
	})
	return err
}

func (s *CheckpointService) failStage(ctx context.Context, p *Pipeline, stage Stage, stageErr error) error {
	runID, topicID, err := pipelineIDs(p)
	if err != nil {
		return err
	}
	err = s.postgres.WithTx(ctx, func(q *postgres.Queries) error {
		if _, err := q.StartPipelineStage(ctx, postgres.StartPipelineStageParams{
			RunID:   runID,
			TopicID: topicID,
			Stage:   string(stage),
		}); err != nil {
			return fmt.Errorf("start failed %s checkpoint: %w", stage, err)
		}
		if _, err := q.FailPipelineStage(ctx, postgres.FailPipelineStageParams{
			RunID:        runID,
			Stage:        string(stage),
			ErrorMessage: pgtype.Text{String: stageErr.Error(), Valid: true},
		}); err != nil {
			return fmt.Errorf("fail %s checkpoint: %w", stage, err)
		}
		return updateTopicState(ctx, q, p)
	})
	if err != nil {
		logger.From(ctx).Warn().Err(err).Str("stage", string(stage)).Msg("Failed to persist pipeline stage failure")
	}
	return err
}

func (s *CheckpointService) persistTerminalState(ctx context.Context, p *Pipeline) error {
	return s.postgres.WithTx(ctx, func(q *postgres.Queries) error {
		return updateTopicState(ctx, q, p)
	})
}

func updateTopicState(ctx context.Context, q *postgres.Queries, p *Pipeline) error {
	id, err := uuid.Parse(p.TopicID)
	if err != nil {
		return fmt.Errorf("invalid pipeline topic ID %q: %w", p.TopicID, err)
	}
	_, err = q.UpdateResearchTopicState(ctx, postgres.UpdateResearchTopicStateParams{
		ID:           id,
		Status:       p.Status,
		CurrentStage: string(p.Stage),
		Progress:     p.Progress,
		ErrorMessage: pgtype.Text{String: p.Error, Valid: p.Error != ""},
	})
	return err
}

func pipelineIDs(p *Pipeline) (uuid.UUID, uuid.UUID, error) {
	runID, err := uuid.Parse(p.RunID)
	if err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("invalid pipeline run ID %q: %w", p.RunID, err)
	}
	topicID, err := uuid.Parse(p.TopicID)
	if err != nil {
		return uuid.Nil, uuid.Nil, fmt.Errorf("invalid pipeline topic ID %q: %w", p.TopicID, err)
	}
	return runID, topicID, nil
}
