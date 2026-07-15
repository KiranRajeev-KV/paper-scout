package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/paper-scout/internal/logger"
	"github.com/paper-scout/internal/storage/redis"
	goredis "github.com/redis/go-redis/v9"
)

var ErrStateNotFound = errors.New("pipeline state not found")

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
		RunID:     pipeline.RunID,
		Topic:     pipeline.Topic,
		Status:    pipeline.Status,
		Stage:     string(pipeline.Stage),
		Progress:  pipeline.Progress,
		StartedAt: pipeline.StartedAt,
		UpdatedAt: pipeline.UpdatedAt,
		Error:     pipeline.Error,
	}

	if err := s.redis.SetJSON(ctx, key, state, 24*time.Hour); err != nil {
		logger.From(ctx).Warn().Err(err).Str("topic_id", topicID).Msg("Failed to save pipeline state")
		return err
	}

	return nil
}

func (s *StateManager) Load(ctx context.Context, topicID string) (*Pipeline, error) {
	key := "pipeline:" + topicID

	var state PipelineState
	if err := s.redis.GetJSON(ctx, key, &state); err != nil {
		if errors.Is(err, goredis.Nil) {
			return nil, ErrStateNotFound
		}
		return nil, err
	}

	return &Pipeline{
		TopicID:   state.TopicID,
		RunID:     state.RunID,
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

func (s *StateManager) ListRecoverable(ctx context.Context) ([]*Pipeline, error) {
	var (
		cursor uint64
		result []*Pipeline
	)

	for {
		keys, next, err := s.redis.Scan(ctx, cursor, "pipeline:*", 100)
		if err != nil {
			return nil, fmt.Errorf("failed to scan pipeline state: %w", err)
		}

		for _, key := range keys {
			var state PipelineState
			if err := s.redis.GetJSON(ctx, key, &state); err != nil {
				logger.From(ctx).Warn().Err(err).Str("key", key).Msg("Failed to load pipeline state during recovery scan")
				continue
			}

			if state.Status == "completed" || state.Status == "failed" {
				continue
			}

			result = append(result, &Pipeline{
				TopicID:   state.TopicID,
				RunID:     state.RunID,
				Topic:     state.Topic,
				Status:    state.Status,
				Stage:     Stage(state.Stage),
				Progress:  state.Progress,
				StartedAt: state.StartedAt,
				UpdatedAt: state.UpdatedAt,
				Error:     state.Error,
			})
		}

		if next == 0 {
			break
		}
		cursor = next
	}

	return result, nil
}

type PipelineState struct {
	TopicID   string    `json:"topic_id"`
	RunID     string    `json:"run_id"`
	Topic     string    `json:"topic"`
	Status    string    `json:"status"`
	Stage     string    `json:"stage"`
	Progress  float64   `json:"progress"`
	StartedAt time.Time `json:"started_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Error     string    `json:"error,omitempty"`
}
