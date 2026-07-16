package orchestrator

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/paper-scout/internal/logger"
	"github.com/paper-scout/internal/storage/postgres"
)

// RecoveryService owns recovery of durable and live pipeline snapshots.
type RecoveryService struct {
	topics        recoveryStore
	listSnapshots func(context.Context) ([]*Pipeline, error)
	state         *PipelineStateService
	logs          *logger.Manager
	launch        func(*Pipeline)
}

type recoveryStore interface {
	GetResearchTopic(context.Context, uuid.UUID) (*postgres.ResearchTopic, error)
	GetPipelineStages(context.Context, uuid.UUID) ([]*postgres.PipelineStageCheckpoint, error)
	ListRecoverableResearchTopics(context.Context) ([]*postgres.ResearchTopic, error)
	UpdateResearchTopicState(context.Context, postgres.UpdateResearchTopicStateParams) (*postgres.ResearchTopic, error)
}

// NewRecoveryService constructs the startup pipeline recovery service.
func NewRecoveryService(pg *postgres.Client, snapshots *StateManager, state *PipelineStateService, logs *logger.Manager, runs *RunManager) (*RecoveryService, error) {
	if pg == nil || snapshots == nil || state == nil || logs == nil || runs == nil {
		return nil, fmt.Errorf("recovery service requires postgres, snapshots, state, logs, and run manager")
	}
	return &RecoveryService{topics: pg.Queries(), listSnapshots: snapshots.ListRecoverable, state: state, logs: logs, launch: runs.Launch}, nil
}

// Recover reopens unfinished runs from Redis snapshots and PostgreSQL checkpoints.
func (s *RecoveryService) Recover(ctx context.Context) {
	recoverable := make(map[string]*Pipeline)
	redisPipelines, err := s.listSnapshots(ctx)
	if err != nil {
		logger.From(ctx).Warn().Err(err).Msg("Failed to scan Redis pipeline state; falling back to Postgres")
	} else {
		for _, pipeline := range redisPipelines {
			topicID, parseErr := uuid.Parse(pipeline.TopicID)
			if parseErr != nil {
				logger.From(ctx).Warn().Err(parseErr).Str("topic_id", pipeline.TopicID).Msg("Skipping Redis recovery candidate with invalid topic ID")
				continue
			}
			topic, loadErr := s.topics.GetResearchTopic(ctx, topicID)
			if loadErr != nil {
				logger.From(ctx).Warn().Err(loadErr).Str("topic_id", pipeline.TopicID).Msg("Skipping Redis recovery candidate without durable topic state")
				continue
			}
			if topic.Status == "completed" || topic.Status == "failed" {
				continue
			}
			recoverable[pipeline.TopicID] = pipelineFromTopic(topic)
		}
	}
	durableTopics, err := s.topics.ListRecoverableResearchTopics(ctx)
	if err != nil {
		logger.From(ctx).Warn().Err(err).Msg("Failed to scan durable recoverable topics")
	} else {
		for _, topic := range durableTopics {
			pipeline := pipelineFromTopic(topic)
			checkpoints, checkpointErr := s.topics.GetPipelineStages(ctx, topic.RunID)
			if checkpointErr != nil {
				logger.From(ctx).Warn().Err(checkpointErr).Str("topic_id", pipeline.TopicID).Msg("Failed to load durable pipeline checkpoints")
			} else if len(checkpoints) > 0 {
				latest := checkpoints[len(checkpoints)-1]
				pipeline.Stage = Stage(latest.Stage)
				if latest.UpdatedAt.Valid {
					pipeline.UpdatedAt = latest.UpdatedAt.Time
				}
			}
			recoverable[pipeline.TopicID] = pipeline
		}
	}
	for _, pipeline := range recoverable {
		s.recoverOne(ctx, pipeline)
	}
}

func (s *RecoveryService) recoverOne(ctx context.Context, pipeline *Pipeline) {
	if pipeline.RunID == "" {
		topicID, err := uuid.Parse(pipeline.TopicID)
		if err != nil {
			logger.From(ctx).Warn().Err(err).Str("topic_id", pipeline.TopicID).Msg("Invalid topic ID during recovery")
			return
		}
		topic, err := s.topics.GetResearchTopic(ctx, topicID)
		if err != nil {
			logger.From(ctx).Warn().Err(err).Str("topic_id", pipeline.TopicID).Msg("Failed to resolve run ID during recovery")
			return
		}
		pipeline.RunID = topic.RunID.String()
	}
	if err := s.logs.StartRun(pipeline.RunID, pipeline.TopicID); err != nil {
		logger.From(ctx).Error().Err(err).Str("topic_id", pipeline.TopicID).Msg("Failed to reopen run log; recovery skipped")
		s.failRecovery(ctx, pipeline, err)
		return
	}
	s.state.Remember(pipeline)
	logger.From(s.logs.ContextForTopic(ctx, pipeline.TopicID)).Info().Str("stage", string(pipeline.Stage)).Msg("Recovering persisted pipeline")
	s.launch(pipeline)
}

func (s *RecoveryService) failRecovery(ctx context.Context, pipeline *Pipeline, cause error) {
	pipeline.Status, pipeline.Stage, pipeline.Error = "failed", StageFailed, cause.Error()
	topicID, err := uuid.Parse(pipeline.TopicID)
	if err != nil {
		logger.From(ctx).Error().Err(err).Str("topic_id", pipeline.TopicID).Msg("Cannot persist failed recovery state")
		return
	}
	if _, err := s.topics.UpdateResearchTopicState(ctx, postgres.UpdateResearchTopicStateParams{
		ID: topicID, Status: "failed", CurrentStage: string(StageFailed), Progress: pipeline.Progress,
		ErrorMessage: pgtype.Text{String: pipeline.Error, Valid: true},
	}); err != nil {
		logger.From(ctx).Error().Err(err).Str("topic_id", pipeline.TopicID).Msg("Failed to persist recovery log failure")
	}
	s.state.Publish(ctx, pipeline)
}
