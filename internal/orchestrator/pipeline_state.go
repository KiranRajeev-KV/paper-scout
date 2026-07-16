package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/paper-scout/internal/logger"
	"github.com/paper-scout/internal/storage/postgres"
)

// PipelineStateService owns active snapshots, Redis live state, PostgreSQL fallback, and state publication.
type PipelineStateService struct {
	snapshots *StateManager
	postgres  *postgres.Client
	sse       *SSEManager

	mu        sync.RWMutex
	pipelines map[string]*Pipeline

	loadSnapshot func(context.Context, string) (*Pipeline, error)
	loadTopic    func(context.Context, uuid.UUID) (*postgres.ResearchTopic, error)
}

// NewPipelineStateService constructs pipeline state storage with Redis snapshots and PostgreSQL fallback.
func NewPipelineStateService(snapshots *StateManager, pg *postgres.Client, sse *SSEManager) (*PipelineStateService, error) {
	if snapshots == nil || pg == nil || sse == nil {
		return nil, fmt.Errorf("pipeline state service requires snapshots, postgres, and SSE")
	}
	return &PipelineStateService{
		snapshots: snapshots, postgres: pg, sse: sse, pipelines: make(map[string]*Pipeline),
		loadSnapshot: snapshots.Load, loadTopic: pg.Queries().GetResearchTopic,
	}, nil
}

// Remember records a process-local immutable snapshot.
func (s *PipelineStateService) Remember(pipeline *Pipeline) {
	s.mu.Lock()
	s.pipelines[pipeline.TopicID] = clonePipeline(pipeline)
	s.mu.Unlock()
}

// Save persists a live Redis snapshot without publishing an event.
func (s *PipelineStateService) Save(ctx context.Context, pipeline *Pipeline) error {
	if s.snapshots == nil {
		return nil
	}
	return s.snapshots.Save(ctx, pipeline.TopicID, pipeline)
}

// Publish records state, writes a bounded live snapshot, and broadcasts the state event.
func (s *PipelineStateService) Publish(ctx context.Context, pipeline *Pipeline) {
	s.Remember(pipeline)
	stateCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := s.Save(stateCtx, pipeline); err != nil {
		logger.From(ctx).Warn().Err(err).Str("topic_id", pipeline.TopicID).Msg("Failed to save live pipeline state")
	}
	s.sse.Broadcast(statusEvent{TopicID: pipeline.TopicID, Status: pipeline.Status, Stage: string(pipeline.Stage), Progress: pipeline.Progress, Error: pipeline.Error})
}

// PublishProgress emits analysis progress without changing durable state.
func (s *PipelineStateService) PublishProgress(topicID string, progress float64) {
	s.sse.Broadcast(progressEvent{TopicID: topicID, Stage: string(StageAnalysis), Progress: progress})
}

// Get returns the local snapshot, Redis state, or authoritative PostgreSQL state in that order.
func (s *PipelineStateService) Get(ctx context.Context, topicID string) (*Pipeline, error) {
	s.mu.RLock()
	pipeline, ok := s.pipelines[topicID]
	s.mu.RUnlock()
	if ok {
		return clonePipeline(pipeline), nil
	}
	id, err := uuid.Parse(topicID)
	if err != nil {
		return nil, fmt.Errorf("%w: %q", ErrInvalidTopicID, topicID)
	}
	if s.loadSnapshot != nil {
		pipeline, stateErr := s.loadSnapshot(ctx, topicID)
		if stateErr == nil {
			s.Remember(pipeline)
			return clonePipeline(pipeline), nil
		}
		if !errors.Is(stateErr, ErrStateNotFound) {
			logger.From(ctx).Warn().Err(stateErr).Str("topic_id", topicID).Msg("Failed to load Redis pipeline state; falling back to Postgres")
		}
	}
	if s.loadTopic == nil {
		return nil, fmt.Errorf("load durable pipeline %s: postgres is not configured", topicID)
	}
	topic, err := s.loadTopic(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: %s", ErrPipelineNotFound, topicID)
		}
		return nil, fmt.Errorf("load durable pipeline %s: %w", topicID, err)
	}
	pipeline = pipelineFromTopic(topic)
	s.Remember(pipeline)
	return clonePipeline(pipeline), nil
}

func pipelineFromTopic(topic *postgres.ResearchTopic) *Pipeline {
	pipeline := &Pipeline{TopicID: topic.ID.String(), RunID: topic.RunID.String(), Topic: topic.Topic, Status: topic.Status,
		Stage: Stage(topic.CurrentStage), Progress: topic.Progress, StartedAt: topic.CreatedAt.Time, UpdatedAt: topic.UpdatedAt.Time}
	if pipeline.Stage == "" {
		pipeline.Stage = StagePending
	}
	if topic.ErrorMessage.Valid {
		pipeline.Error = topic.ErrorMessage.String
	}
	return pipeline
}
