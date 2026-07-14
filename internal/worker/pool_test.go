package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	redispkg "github.com/paper-scout/internal/storage/redis"
)

// Protects redis job round trip preserves semantics.
func TestRedisJobRoundTripPreservesSemantics(t *testing.T) {
	created := time.Unix(123, 456).UTC()
	original := Job{
		ID: "job-1", Type: TypeEmbeddingBatch, Payload: map[string]interface{}{"topic_id": "topic-1", "value": "payload"},
		Priority: 7, Timeout: 37 * time.Second, CreatedAt: created, Retries: 2, MaxRetry: 9,
		RunID: "run-1", TopicID: "topic-1", TraceID: "trace-1",
	}
	queued := toRedisJob(original)
	data, err := json.Marshal(queued)
	if err != nil {
		t.Fatalf("marshal Redis job: %v", err)
	}
	var decoded redispkg.Job
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal Redis job: %v", err)
	}
	restored := fromRedisJob(decoded)
	if !reflect.DeepEqual(restored, original) {
		t.Fatalf("restored job = %#v, want %#v", restored, original)
	}
}

// Protects paper analysis respects worker limit.
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
	if err := pool.Start(); err != nil {
		t.Fatal(err)
	}
	defer pool.Stop()

	for i := 0; i < jobs; i++ {
		if err := pool.Submit(NewJob(TypePaperAnalysis, map[string]interface{}{"paper_id": fmt.Sprintf("paper-%d", i)})); err != nil {
			t.Fatalf("submit job %d: %v", i, err)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		metrics := pool.GetMetrics()
		if metrics.JobsProcessed == jobs {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("processed %d jobs, want %d", metrics.JobsProcessed, jobs)
		}
		time.Sleep(time.Millisecond)
	}
	if got := peak.Load(); got > workers {
		t.Fatalf("peak concurrent handlers = %d, worker limit = %d", got, workers)
	}
}

// Protects concurrent submit and stop.
func TestConcurrentSubmitAndStop(t *testing.T) {
	pool := NewPool(2, 64)
	pool.SetHandler(func(context.Context, Job) error { return nil })
	if err := pool.Start(); err != nil {
		t.Fatal(err)
	}

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

// Protects worker metrics track active jobs.
func TestWorkerMetricsTrackActiveJobs(t *testing.T) {
	pool := NewPool(1, 1)
	started := make(chan struct{})
	release := make(chan struct{})
	pool.SetHandler(func(context.Context, Job) error {
		close(started)
		<-release
		return nil
	})
	if err := pool.Start(); err != nil {
		t.Fatal(err)
	}

	if err := pool.Submit(Job{ID: "active-job", Type: TypePaperAnalysis, Timeout: time.Second}); err != nil {
		t.Fatalf("submit job: %v", err)
	}
	<-started

	deadline := time.Now().Add(time.Second)
	for {
		active := pool.GetMetrics().ActiveWorkers
		if active == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("active jobs = %d, want 1", active)
		}
		time.Sleep(time.Millisecond)
	}

	close(release)
	deadline = time.Now().Add(time.Second)
	for {
		active := pool.GetMetrics().ActiveWorkers
		if active == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("active jobs = %d, want 0", active)
		}
		time.Sleep(time.Millisecond)
	}
	pool.Stop()
}

// Protects stop and wait honors timeout.
func TestStopAndWaitHonorsTimeout(t *testing.T) {
	pool := NewPool(1, 1)
	started := make(chan struct{})
	release := make(chan struct{})
	pool.SetHandler(func(context.Context, Job) error {
		close(started)
		<-release
		return nil
	})
	if err := pool.Start(); err != nil {
		t.Fatal(err)
	}
	if err := pool.Submit(Job{ID: "blocking-job", Type: TypePaperAnalysis, Timeout: time.Hour}); err != nil {
		t.Fatalf("submit job: %v", err)
	}
	<-started

	begin := time.Now()
	pool.StopAndWait(25 * time.Millisecond)
	if elapsed := time.Since(begin); elapsed > 500*time.Millisecond {
		t.Fatalf("StopAndWait took %s, want it to honor timeout", elapsed)
	}

	close(release)
	pool.Stop()
}

type fakeRedisQueue struct {
	failResult  redispkg.FailResult
	failJob     redispkg.Job
	failErr     error
	completeErr error
}

func (f *fakeRedisQueue) EnsureGroup(context.Context) error { return nil }

func (f *fakeRedisQueue) Enqueue(context.Context, redispkg.Job) error { return nil }

func (f *fakeRedisQueue) Dequeue(context.Context, string, time.Duration) (*redispkg.Job, error) {
	return nil, nil
}

func (f *fakeRedisQueue) Complete(context.Context, string) error { return f.completeErr }

func (f *fakeRedisQueue) Fail(_ context.Context, job redispkg.Job, _ string) (redispkg.FailResult, error) {
	f.failJob = job
	return f.failResult, f.failErr
}

func (f *fakeRedisQueue) QueueDepth(context.Context) (int64, error) { return 0, nil }

// Protects a Redis failure-write error from completing a worker batch.
func TestRedisPoolFailurePersistenceErrorDoesNotCompleteBatch(t *testing.T) {
	queue := &fakeRedisQueue{failErr: errors.New("redis unavailable")}
	pool := NewRedisPool(1, queue)
	pool.handler = func(context.Context, Job) error {
		return errors.New("permanent failure")
	}

	var calls int
	pool.hook = func(_ Job, err error, gotTerminal bool) {
		calls++
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

	if calls != 0 {
		t.Fatalf("completion hook called %d times, want 0", calls)
	}
	if got := pool.GetMetrics().JobStateWriteFailures; got != 1 {
		t.Fatalf("state write failures = %d, want 1", got)
	}
}

// Protects a Redis acknowledgement error from completing a successful worker batch.
func TestRedisPoolCompletionPersistenceErrorDoesNotCompleteBatch(t *testing.T) {
	queue := &fakeRedisQueue{completeErr: errors.New("redis unavailable")}
	pool := NewRedisPool(1, queue)
	pool.handler = func(context.Context, Job) error { return nil }
	var calls int
	pool.hook = func(Job, error, bool) { calls++ }
	job := Job{ID: "job-1", Type: TypePaperAnalysis, Timeout: time.Second, MaxRetry: 3}
	redisJob := &redispkg.Job{ID: job.ID, Type: redispkg.JobType(job.Type), StreamID: "1-0"}
	if err := pool.processJobWithRedisTracking(0, job, redisJob); err == nil {
		t.Fatal("expected Redis acknowledgement error")
	}
	if calls != 0 {
		t.Fatalf("completion hook called %d times, want 0", calls)
	}
	if got := pool.GetMetrics().JobStateWriteFailures; got != 1 {
		t.Fatalf("state write failures = %d, want 1", got)
	}
}

// Protects Redis metrics distinguish recovered attempts from terminal failures.
func TestRedisPoolMetricsTrackRecoveredAndTerminalFailures(t *testing.T) {
	queue := &fakeRedisQueue{failResult: redispkg.FailResult{Requeued: true}}
	pool := NewRedisPool(1, queue)
	job := Job{ID: "job-1", Type: TypePaperAnalysis, Timeout: time.Second, MaxRetry: 3}
	redisJob := &redispkg.Job{ID: job.ID, Type: redispkg.JobType(job.Type), MaxRetry: job.MaxRetry, StreamID: "1-0"}

	pool.handler = func(context.Context, Job) error { return errors.New("transient failure") }
	if err := pool.processJobWithRedisTracking(0, job, redisJob); err == nil {
		t.Fatal("expected failed attempt")
	}
	metrics := pool.GetMetrics()
	if metrics.JobAttemptsFailed != 1 || metrics.JobsRetried != 1 || metrics.JobsTerminallyFailed != 0 {
		t.Fatalf("metrics after retry = %#v", metrics)
	}

	pool.handler = func(context.Context, Job) error { return nil }
	if err := pool.processJobWithRedisTracking(0, job.IncrementRetry(), redisJob); err != nil {
		t.Fatalf("recovered attempt: %v", err)
	}
	metrics = pool.GetMetrics()
	if metrics.JobsProcessed != 1 || metrics.JobAttemptsFailed != 1 || metrics.JobsRetried != 1 || metrics.JobsTerminallyFailed != 0 {
		t.Fatalf("metrics after recovery = %#v", metrics)
	}

	queue.failResult = redispkg.FailResult{Terminal: true}
	pool.handler = func(context.Context, Job) error { return errors.New("permanent failure") }
	if err := pool.processJobWithRedisTracking(0, job, redisJob); err == nil {
		t.Fatal("expected terminal failure")
	}
	metrics = pool.GetMetrics()
	if metrics.JobAttemptsFailed != 2 || metrics.JobsTerminallyFailed != 1 {
		t.Fatalf("metrics after terminal failure = %#v", metrics)
	}
}

// Protects worker startup from panicking on an invalid dependency graph.
func TestPoolStartRejectsMissingHandler(t *testing.T) {
	pool := NewPool(1, 1)
	if err := pool.Start(); err == nil {
		t.Fatal("Start accepted a pool without a handler")
	}
}

// Protects processor construction from deferring missing dependencies until job execution.
func TestProcessorRejectsMissingDependencies(t *testing.T) {
	if processor, err := NewProcessor(nil, nil, nil, nil, nil, nil, 350, 50, 10); err == nil || processor != nil {
		t.Fatalf("NewProcessor() = (%v, %v), want an explicit dependency error", processor, err)
	}
}
