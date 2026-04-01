package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/paper-scout/internal/logger"
)

const (
	JobQueuePending    = "jobs:pending"
	JobQueueProcessing = "jobs:processing"
	JobQueueFailed     = "jobs:failed"
)

type JobType string

const (
	JobTypePaperAnalysis JobType = "paper_analysis"
	JobTypePDFDownload   JobType = "pdf_download"
	JobTypeEmbedding     JobType = "embedding"
)

type Job struct {
	ID        string                 `json:"id"`
	Type      JobType                `json:"type"`
	Payload   map[string]interface{} `json:"payload"`
	Priority  int                    `json:"priority"`
	CreatedAt time.Time              `json:"created_at"`
	Retries   int                    `json:"retries"`
	MaxRetry  int                    `json:"max_retry"`
}

type Queue struct {
	client *redis.Client
}

func NewQueue(client *redis.Client) *Queue {
	return &Queue{client: client}
}

func (q *Queue) Enqueue(ctx context.Context, job Job) error {
	job.CreatedAt = time.Now()
	if job.MaxRetry == 0 {
		job.MaxRetry = 3
	}

	data, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	if err := q.client.RPush(ctx, JobQueuePending, data).Err(); err != nil {
		return fmt.Errorf("failed to enqueue job: %w", err)
	}

	logger.Debug().
		Str("job_id", job.ID).
		Str("type", string(job.Type)).
		Msg("Job enqueued")

	return nil
}

func (q *Queue) Dequeue(ctx context.Context, timeout time.Duration) (*Job, error) {
	result, err := q.client.BRPop(ctx, timeout, JobQueuePending).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to dequeue job: %w", err)
	}

	if len(result) < 2 {
		return nil, nil
	}

	var job Job
	if err := json.Unmarshal([]byte(result[1]), &job); err != nil {
		return nil, fmt.Errorf("failed to unmarshal job: %w", err)
	}

	processingData, _ := json.Marshal(job)
	if err := q.client.SAdd(ctx, JobQueueProcessing, processingData).Err(); err != nil {
		return nil, fmt.Errorf("failed to add to processing set: %w", err)
	}

	return &job, nil
}

func (q *Queue) Complete(ctx context.Context, jobID string) error {
	return q.removeFromProcessing(ctx, jobID)
}

func (q *Queue) Fail(ctx context.Context, job Job, errMsg string) error {
	if err := q.removeFromProcessing(ctx, job.ID); err != nil {
		return err
	}

	job.Retries++
	if job.Retries < job.MaxRetry {
		logger.Warn().
			Str("job_id", job.ID).
			Int("retry", job.Retries).
			Str("error", errMsg).
			Msg("Job failed, requeueing")
		return q.Enqueue(ctx, job)
	}

	logger.Error().
		Str("job_id", job.ID).
		Int("retries", job.Retries).
		Str("error", errMsg).
		Msg("Job failed, max retries reached")

	failedData, _ := json.Marshal(map[string]interface{}{
		"job":   job,
		"error": errMsg,
		"at":    time.Now(),
	})

	return q.client.RPush(ctx, JobQueueFailed, failedData).Err()
}

func (q *Queue) removeFromProcessing(ctx context.Context, jobID string) error {
	members, err := q.client.SMembers(ctx, JobQueueProcessing).Result()
	if err != nil {
		return err
	}

	for _, member := range members {
		var job Job
		if err := json.Unmarshal([]byte(member), &job); err != nil {
			continue
		}
		if job.ID == jobID {
			return q.client.SRem(ctx, JobQueueProcessing, member).Err()
		}
	}
	return nil
}

func (q *Queue) QueueDepth(ctx context.Context) (int64, error) {
	return q.client.LLen(ctx, JobQueuePending).Result()
}

func (q *Queue) ProcessingCount(ctx context.Context) (int64, error) {
	return q.client.SCard(ctx, JobQueueProcessing).Result()
}

func (q *Queue) FailedCount(ctx context.Context) (int64, error) {
	return q.client.LLen(ctx, JobQueueFailed).Result()
}

func (q *Queue) ClearFailed(ctx context.Context) error {
	return q.client.Del(ctx, JobQueueFailed).Err()
}
