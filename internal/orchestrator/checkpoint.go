package orchestrator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/paper-scout/internal/logger"
	"github.com/paper-scout/internal/storage/postgres"
)

func (o *Orchestrator) stageCompleted(ctx context.Context, p *Pipeline, stage Stage, output interface{}) (bool, error) {
	checkpoint, err := o.postgres.Queries().GetPipelineStage(ctx, postgres.GetPipelineStageParams{
		RunID: parseUUID(p.RunID),
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

func (o *Orchestrator) startStage(ctx context.Context, p *Pipeline, stage Stage) error {
	err := o.postgres.WithTx(ctx, func(q *postgres.Queries) error {
		if _, err := q.StartPipelineStage(ctx, postgres.StartPipelineStageParams{
			RunID:   parseUUID(p.RunID),
			TopicID: parseUUID(p.TopicID),
			Stage:   string(stage),
		}); err != nil {
			return fmt.Errorf("start %s checkpoint: %w", stage, err)
		}
		return updateTopicState(ctx, q, p)
	})
	return err
}

func (o *Orchestrator) completeStage(ctx context.Context, p *Pipeline, stage Stage, output interface{}) error {
	data, err := json.Marshal(output)
	if err != nil {
		return fmt.Errorf("encode %s checkpoint: %w", stage, err)
	}
	err = o.postgres.WithTx(ctx, func(q *postgres.Queries) error {
		if _, err := q.CompletePipelineStage(ctx, postgres.CompletePipelineStageParams{
			RunID:  parseUUID(p.RunID),
			Stage:  string(stage),
			Output: data,
		}); err != nil {
			return fmt.Errorf("complete %s checkpoint: %w", stage, err)
		}
		return updateTopicState(ctx, q, p)
	})
	return err
}

func (o *Orchestrator) failStage(ctx context.Context, p *Pipeline, stage Stage, stageErr error) error {
	err := o.postgres.WithTx(ctx, func(q *postgres.Queries) error {
		if _, err := q.StartPipelineStage(ctx, postgres.StartPipelineStageParams{
			RunID:   parseUUID(p.RunID),
			TopicID: parseUUID(p.TopicID),
			Stage:   string(stage),
		}); err != nil {
			return fmt.Errorf("start failed %s checkpoint: %w", stage, err)
		}
		if _, err := q.FailPipelineStage(ctx, postgres.FailPipelineStageParams{
			RunID:        parseUUID(p.RunID),
			Stage:        string(stage),
			ErrorMessage: pgtype.Text{String: stageErr.Error(), Valid: true},
		}); err != nil {
			return fmt.Errorf("fail %s checkpoint: %w", stage, err)
		}
		return updateTopicState(ctx, q, p)
	})
	if err != nil {
		logger.Warn().Err(err).Str("stage", string(stage)).Msg("Failed to persist pipeline stage failure")
	}
	return err
}

func (o *Orchestrator) persistTerminalState(ctx context.Context, p *Pipeline) error {
	return o.postgres.WithTx(ctx, func(q *postgres.Queries) error {
		return updateTopicState(ctx, q, p)
	})
}

func updateTopicState(ctx context.Context, q *postgres.Queries, p *Pipeline) error {
	_, err := q.UpdateResearchTopicState(ctx, postgres.UpdateResearchTopicStateParams{
		ID:           parseUUID(p.TopicID),
		Status:       p.Status,
		CurrentStage: string(p.Stage),
		Progress:     p.Progress,
		ErrorMessage: pgtype.Text{String: p.Error, Valid: p.Error != ""},
	})
	return err
}
