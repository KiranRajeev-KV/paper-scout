package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	redispkg "github.com/paper-scout/internal/storage/redis"
)

func TestPaperAnalysisRespectsWorkerLimit(t *testing.T) {
	const workers = 3
	const jobs = 18

	pool := NewPool(workers, jobs)
	var active atomic.Int32
	var peak atomic.Int32
	pool.SetHandler(func(context.Context, Job) error {
		current := active.Add(1)
		for {
			observed := peak.Load()
			if current <= observed || peak.CompareAndSwap(observed, current) {
				break
			}
		}
		time.Sleep(15 * time.Millisecond)
		active.Add(-1)
		return nil
	})
	pool.Start()
	defer pool.Stop()

	for i := 0; i < jobs; i++ {
		if err := pool.Submit(NewJob(TypePaperAnalysis, map[string]interface{}{"paper_id": fmt.Sprintf("paper-%d", i)})); err != nil {
			t.Fatalf("submit job %d: %v", i, err)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		processed, _, _, _ := pool.GetMetrics()
		if processed == jobs {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("processed %d jobs, want %d", processed, jobs)
		}
		time.Sleep(time.Millisecond)
	}
	if got := peak.Load(); got > workers {
		t.Fatalf("peak concurrent handlers = %d, worker limit = %d", got, workers)
	}
}

func TestConcurrentSubmitAndStop(t *testing.T) {
	pool := NewPool(2, 64)
	pool.SetHandler(func(context.Context, Job) error { return nil })
	pool.Start()

	var submitters sync.WaitGroup
	var panics atomic.Int32
	for i := 0; i < 12; i++ {
		submitters.Add(1)
		go func(workerID int) {
			defer submitters.Done()
			defer func() {
				if recover() != nil {
					panics.Add(1)
				}
			}()
			for jobID := 0; jobID < 200; jobID++ {
				_ = pool.Submit(NewJob(TypePaperAnalysis, map[string]interface{}{"id": fmt.Sprintf("%d-%d", workerID, jobID)}))
			}
		}(i)
	}

	time.Sleep(5 * time.Millisecond)
	pool.Stop()
	submitters.Wait()
	if got := panics.Load(); got != 0 {
		t.Fatalf("Submit panicked %d times during Stop", got)
	}
}

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
