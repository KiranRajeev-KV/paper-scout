package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/paper-scout/internal/logger"
	goredis "github.com/redis/go-redis/v9"
)

const (
	JobQueueStream     = "jobs:stream"
	JobQueueGroup      = "jobs:workers"
	JobQueueDeadLetter = "jobs:failed"
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
	Timeout   time.Duration          `json:"timeout"`
	CreatedAt time.Time              `json:"created_at"`
	Retries   int                    `json:"retries"`
	MaxRetry  int                    `json:"max_retry"`
	StreamID  string                 `json:"-"`
}

type QueueOptions struct {
	Stream       string
	Group        string
	ClaimIdle    time.Duration
	ReclaimCount int64
}

type Queue struct {
	client       *goredis.Client
	stream       string
	group        string
	claimIdle    time.Duration
	reclaimCount int64
}

func NewQueue(client *goredis.Client, opts QueueOptions) *Queue {
	if opts.Stream == "" {
		opts.Stream = JobQueueStream
	}
	if opts.Group == "" {
		opts.Group = JobQueueGroup
	}
	if opts.ClaimIdle <= 0 {
		opts.ClaimIdle = 15 * time.Minute
	}
	if opts.ReclaimCount <= 0 {
		opts.ReclaimCount = 10
	}

	return &Queue{
		client:       client,
		stream:       opts.Stream,
		group:        opts.Group,
		claimIdle:    opts.ClaimIdle,
		reclaimCount: opts.ReclaimCount,
	}
}

func (q *Queue) EnsureGroup(ctx context.Context) error {
	err := q.client.XGroupCreateMkStream(ctx, q.stream, q.group, "0").Err()
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "BUSYGROUP") {
		return nil
	}
	return fmt.Errorf("failed to ensure consumer group: %w", err)
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

	if _, err := q.client.XAdd(ctx, &goredis.XAddArgs{
		Stream: q.stream,
		Values: map[string]interface{}{
			"job": string(data),
		},
	}).Result(); err != nil {
		return fmt.Errorf("failed to enqueue job: %w", err)
	}

	logger.Debug().
		Str("job_id", job.ID).
		Str("type", string(job.Type)).
		Msg("Job enqueued")

	return nil
}

func (q *Queue) Dequeue(ctx context.Context, consumer string, timeout time.Duration) (*Job, error) {
	if job, err := q.claimStale(ctx, consumer); err != nil {
		return nil, err
	} else if job != nil {
		return job, nil
	}

	streams, err := q.client.XReadGroup(ctx, &goredis.XReadGroupArgs{
		Group:    q.group,
		Consumer: consumer,
		Streams:  []string{q.stream, ">"},
		Count:    1,
		Block:    timeout,
	}).Result()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to dequeue job: %w", err)
	}

	return q.firstJob(streams)
}

func (q *Queue) Complete(ctx context.Context, streamID string) error {
	if streamID == "" {
		return fmt.Errorf("missing stream id")
	}

	pipe := q.client.TxPipeline()
	pipe.XAck(ctx, q.stream, q.group, streamID)
	pipe.XDel(ctx, q.stream, streamID)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to complete job: %w", err)
	}
	return nil
}

func (q *Queue) Fail(ctx context.Context, job Job, errMsg string) error {
	if job.StreamID == "" {
		return fmt.Errorf("missing stream id")
	}

	pipe := q.client.TxPipeline()
	pipe.XAck(ctx, q.stream, q.group, job.StreamID)
	pipe.XDel(ctx, q.stream, job.StreamID)

	job.Retries++
	if job.Retries < job.MaxRetry {
		data, err := json.Marshal(job)
		if err != nil {
			return fmt.Errorf("failed to marshal retry job: %w", err)
		}

		pipe.XAdd(ctx, &goredis.XAddArgs{
			Stream: q.stream,
			Values: map[string]interface{}{
				"job": string(data),
			},
		})

		logger.Warn().
			Str("job_id", job.ID).
			Int("retry", job.Retries).
			Str("error", errMsg).
			Msg("Job failed, requeueing")
	} else {
		failedData, _ := json.Marshal(map[string]interface{}{
			"job":   job,
			"error": errMsg,
			"at":    time.Now(),
		})
		pipe.RPush(ctx, JobQueueDeadLetter, failedData)

		logger.Error().
			Str("job_id", job.ID).
			Int("retries", job.Retries).
			Str("error", errMsg).
			Msg("Job failed, max retries reached")
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to fail job: %w", err)
	}

	return nil
}

func (q *Queue) QueueDepth(ctx context.Context) (int64, error) {
	return q.client.XLen(ctx, q.stream).Result()
}

func (q *Queue) ProcessingCount(ctx context.Context) (int64, error) {
	summary, err := q.client.XPending(ctx, q.stream, q.group).Result()
	if err != nil {
		if isNoGroupErr(err) {
			return 0, nil
		}
		return 0, err
	}
	return summary.Count, nil
}

func (q *Queue) FailedCount(ctx context.Context) (int64, error) {
	return q.client.LLen(ctx, JobQueueDeadLetter).Result()
}

func (q *Queue) ClearFailed(ctx context.Context) error {
	return q.client.Del(ctx, JobQueueDeadLetter).Err()
}

func (q *Queue) claimStale(ctx context.Context, consumer string) (*Job, error) {
	messages, _, err := q.client.XAutoClaim(ctx, &goredis.XAutoClaimArgs{
		Stream:   q.stream,
		Group:    q.group,
		Consumer: consumer,
		MinIdle:  q.claimIdle,
		Start:    "0-0",
		Count:    q.reclaimCount,
	}).Result()
	if err != nil {
		if errors.Is(err, goredis.Nil) || isNoGroupErr(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to claim stale job: %w", err)
	}
	if len(messages) == 0 {
		return nil, nil
	}
	return decodeStreamJob(messages[0])
}

func (q *Queue) firstJob(streams []goredis.XStream) (*Job, error) {
	for _, stream := range streams {
		if len(stream.Messages) == 0 {
			continue
		}
		return decodeStreamJob(stream.Messages[0])
	}
	return nil, nil
}

func decodeStreamJob(message goredis.XMessage) (*Job, error) {
	raw, ok := message.Values["job"]
	if !ok {
		return nil, fmt.Errorf("stream message missing job field")
	}

	jobJSON, ok := raw.(string)
	if !ok {
		return nil, fmt.Errorf("stream job field has unexpected type %T", raw)
	}

	var job Job
	if err := json.Unmarshal([]byte(jobJSON), &job); err != nil {
		return nil, fmt.Errorf("failed to unmarshal job: %w", err)
	}
	job.StreamID = message.ID
	return &job, nil
}

func isNoGroupErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "NOGROUP")
}
