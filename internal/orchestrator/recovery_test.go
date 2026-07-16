package orchestrator

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/paper-scout/internal/logger"
	"github.com/paper-scout/internal/storage/postgres"
)

type recoveryStoreFake struct {
	topics map[uuid.UUID]*postgres.ResearchTopic
}

func (f *recoveryStoreFake) GetResearchTopic(_ context.Context, id uuid.UUID) (*postgres.ResearchTopic, error) {
	return f.topics[id], nil
}
func (*recoveryStoreFake) GetPipelineStages(context.Context, uuid.UUID) ([]*postgres.PipelineStageCheckpoint, error) {
	return nil, nil
}
func (*recoveryStoreFake) ListRecoverableResearchTopics(context.Context) ([]*postgres.ResearchTopic, error) {
	return nil, nil
}
func (*recoveryStoreFake) UpdateResearchTopicState(context.Context, postgres.UpdateResearchTopicStateParams) (*postgres.ResearchTopic, error) {
	return nil, nil
}

// Protects recovery from relaunching terminal PostgreSQL topics from stale Redis state.
func TestRecoverySkipsTerminalRedisSnapshot(t *testing.T) {
	topicID, runID := uuid.New(), uuid.New()
	store := &recoveryStoreFake{topics: map[uuid.UUID]*postgres.ResearchTopic{topicID: {ID: topicID, RunID: runID, Status: "failed"}}}
	launched := 0
	service := &RecoveryService{topics: store, listSnapshots: func(context.Context) ([]*Pipeline, error) {
		return []*Pipeline{{TopicID: topicID.String(), RunID: runID.String(), Status: "processing", Stage: StageAnalysis}}, nil
	}, launch: func(*Pipeline) { launched++ }}
	service.Recover(context.Background())
	if launched != 0 {
		t.Fatalf("launched %d stale terminal runs, want 0", launched)
	}
}

// Protects recovery uses durable topic fields for a Redis recovery candidate.
func TestRecoveryUsesDurableStateForRedisCandidate(t *testing.T) {
	topicID, runID := uuid.New(), uuid.New()
	store := &recoveryStoreFake{topics: map[uuid.UUID]*postgres.ResearchTopic{topicID: {
		ID: topicID, RunID: runID, Topic: "durable topic", Status: "processing", CurrentStage: string(StageRanking), Progress: .25,
		CreatedAt: pgtype.Timestamptz{Time: time.Unix(1, 0), Valid: true}, UpdatedAt: pgtype.Timestamptz{Time: time.Unix(2, 0), Valid: true},
	}}}
	logs, err := logger.NewManager(logger.Config{Directory: t.TempDir(), Level: "debug"})
	if err != nil {
		t.Fatalf("NewManager returned error: %v", err)
	}
	defer logs.Close()
	var launched *Pipeline
	service := &RecoveryService{topics: store, logs: logs, state: &PipelineStateService{pipelines: make(map[string]*Pipeline)}, listSnapshots: func(context.Context) ([]*Pipeline, error) {
		return []*Pipeline{{TopicID: topicID.String(), RunID: runID.String(), Topic: "stale topic", Status: "processing", Stage: StageAnalysis, Progress: .35}}, nil
	}, launch: func(pipeline *Pipeline) { launched = clonePipeline(pipeline) }}
	service.Recover(context.Background())
	if launched == nil || launched.Topic != "durable topic" || launched.Stage != StageRanking || launched.Progress != .25 {
		t.Fatalf("launched pipeline = %+v, want durable state", launched)
	}
}
