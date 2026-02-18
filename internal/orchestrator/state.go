package orchestrator

import (
	"context"
	"time"

	"github.com/research-agent/internal/logger"
	"github.com/research-agent/internal/storage/redis"
)

type StateManager struct {
	redis *redis.Client
}

func NewStateManager(client *redis.Client) *StateManager {
	return &StateManager{redis: client}
}

func (s *StateManager) Save(ctx context.Context, topicID string, pipeline *Pipeline) error {
	key := "pipeline:" + topicID

	state := &PipelineState{
		TopicID:   pipeline.TopicID,
		Topic:     pipeline.Topic,
		Status:    pipeline.Status,
		Stage:     string(pipeline.Stage),
		Progress:  pipeline.Progress,
		StartedAt: pipeline.StartedAt,
		UpdatedAt: pipeline.UpdatedAt,
		Error:     pipeline.Error,
	}

	if err := s.redis.SetJSON(ctx, key, state, 24*time.Hour); err != nil {
		logger.Warn().Err(err).Str("topic_id", topicID).Msg("Failed to save pipeline state")
		return err
	}

	return nil
}

func (s *StateManager) Load(ctx context.Context, topicID string) (*Pipeline, error) {
	key := "pipeline:" + topicID

	var state PipelineState
	if err := s.redis.GetJSON(ctx, key, &state); err != nil {
		return nil, err
	}

	return &Pipeline{
		TopicID:   state.TopicID,
		Topic:     state.Topic,
		Status:    state.Status,
		Stage:     Stage(state.Stage),
		Progress:  state.Progress,
		StartedAt: state.StartedAt,
		UpdatedAt: state.UpdatedAt,
		Error:     state.Error,
	}, nil
}

func (s *StateManager) Delete(ctx context.Context, topicID string) error {
	key := "pipeline:" + topicID
	return s.redis.Del(ctx, key)
}

type PipelineState struct {
	TopicID   string    `json:"topic_id"`
	Topic     string    `json:"topic"`
	Status    string    `json:"status"`
	Stage     string    `json:"stage"`
	Progress  float64   `json:"progress"`
	StartedAt time.Time `json:"started_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Error     string    `json:"error,omitempty"`
}
