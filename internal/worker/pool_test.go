package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	redispkg "github.com/paper-scout/internal/storage/redis"
)

type fakeRedisQueue struct {
	failResult redispkg.FailResult
	failJob    redispkg.Job
	failErr    error
}

func (f *fakeRedisQueue) EnsureGroup(context.Context) error { return nil }

func (f *fakeRedisQueue) Enqueue(context.Context, redispkg.Job) error { return nil }

func (f *fakeRedisQueue) Dequeue(context.Context, string, time.Duration) (*redispkg.Job, error) {
	return nil, nil
}

func (f *fakeRedisQueue) Complete(context.Context, string) error { return nil }

func (f *fakeRedisQueue) Fail(_ context.Context, job redispkg.Job, _ string) (redispkg.FailResult, error) {
	f.failJob = job
	return f.failResult, f.failErr
}

func (f *fakeRedisQueue) QueueDepth(context.Context) (int64, error) { return 0, nil }

func TestRedisPoolFinalFailureCompletesBatch(t *testing.T) {
	queue := &fakeRedisQueue{failErr: errors.New("redis unavailable")}
	pool := NewRedisPool(1, queue)
	pool.handler = func(context.Context, Job) error {
		return errors.New("permanent failure")
	}

	var calls int
	var terminal bool
	pool.hook = func(_ Job, err error, gotTerminal bool) {
		calls++
		terminal = gotTerminal
		if err == nil {
			t.Error("completion hook received nil error")
		}
	}

	job := Job{
		ID:       "job-1",
		Type:     TypePaperAnalysis,
		Timeout:  time.Second,
		Retries:  2,
		MaxRetry: 3,
	}
	redisJob := &redispkg.Job{
		ID:       job.ID,
		Type:     redispkg.JobType(job.Type),
		Retries:  job.Retries,
		MaxRetry: job.MaxRetry,
		StreamID: "1-0",
	}

	if err := pool.processJobWithRedisTracking(0, job, redisJob); err == nil {
		t.Fatal("expected processing error")
	}

	if calls != 1 {
		t.Fatalf("completion hook called %d times, want 1", calls)
	}
	if !terminal {
		t.Fatal("completion hook received terminal=false for final failure")
	}
}
