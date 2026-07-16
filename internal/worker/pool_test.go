package worker

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
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

type fakeRedisQueue struct {
	failResult   redispkg.FailResult
	failJob      redispkg.Job
	failErr      error
	completeErr  error
	failCtx      context.Context
	completeCtx  context.Context
	failLive     bool
	completeLive bool
}

func (f *fakeRedisQueue) EnsureGroup(context.Context) error { return nil }

func (f *fakeRedisQueue) Enqueue(context.Context, redispkg.Job) error { return nil }

func (f *fakeRedisQueue) Dequeue(context.Context, string, time.Duration) (*redispkg.Job, error) {
	return nil, nil
}

func (f *fakeRedisQueue) Complete(ctx context.Context, _ string) error {
	f.completeCtx = ctx
	f.completeLive = ctx.Err() == nil
	return f.completeErr
}

func (f *fakeRedisQueue) Fail(ctx context.Context, job redispkg.Job, _ string) (redispkg.FailResult, error) {
	f.failCtx = ctx
	f.failLive = ctx.Err() == nil
	f.failJob = job
	return f.failResult, f.failErr
}

func (f *fakeRedisQueue) QueueDepth(context.Context) (int64, error) { return 0, nil }

// Protects a Redis failure-write error from completing a worker batch.
func TestRedisPoolFailurePersistenceErrorDoesNotCompleteBatch(t *testing.T) {
	queue := &fakeRedisQueue{failErr: errors.New("redis unavailable")}
	pool, err := NewRedisPool(t.Context(), 1, queue)
	if err != nil {
		t.Fatalf("NewRedisPool() error = %v", err)
	}
	pool.handler = func(context.Context, Job) error {
		return errors.New("permanent failure")
	}

	var calls int
	pool.hook = func(_ context.Context, _ Job, err error, gotTerminal bool) {
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
	pool, err := NewRedisPool(t.Context(), 1, queue)
	if err != nil {
		t.Fatalf("NewRedisPool() error = %v", err)
	}
	pool.handler = func(context.Context, Job) error { return nil }
	var calls int
	pool.hook = func(context.Context, Job, error, bool) { calls++ }
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

// Protects terminal timeout failures from losing their Redis state transition.
func TestRedisPoolTimeoutPersistsFailureOutsideJobDeadline(t *testing.T) {
	queue := &fakeRedisQueue{failResult: redispkg.FailResult{Terminal: true}}
	pool, err := NewRedisPool(t.Context(), 1, queue)
	if err != nil {
		t.Fatalf("NewRedisPool() error = %v", err)
	}
	pool.handler = func(ctx context.Context, _ Job) error {
		<-ctx.Done()
		return ctx.Err()
	}
	var completed bool
	pool.hook = func(ctx context.Context, _ Job, err error, terminal bool) {
		completed = terminal && err != nil && ctx.Err() == nil
	}
	job := Job{ID: "job-1", Type: TypePaperAnalysis, Timeout: time.Millisecond, MaxRetry: 1}
	redisJob := &redispkg.Job{ID: job.ID, Type: redispkg.JobType(job.Type), StreamID: "1-0"}
	if err := pool.processJobWithRedisTracking(0, job, redisJob); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("processJobWithRedisTracking() error = %v, want deadline exceeded", err)
	}
	if queue.failCtx == nil || !queue.failLive {
		t.Fatalf("Fail() context = %v, want a live persistence context", queue.failCtx)
	}
	if !completed {
		t.Fatal("terminal timeout failure did not notify the completion hook with a live persistence context")
	}
}

// Protects ignored job deadlines from being acknowledged as successful work.
func TestRedisPoolTimeoutPersistsFailureWhenHandlerReturnsNil(t *testing.T) {
	queue := &fakeRedisQueue{failResult: redispkg.FailResult{Terminal: true}}
	pool, err := NewRedisPool(t.Context(), 1, queue)
	if err != nil {
		t.Fatalf("NewRedisPool() error = %v", err)
	}
	pool.handler = func(ctx context.Context, _ Job) error {
		<-ctx.Done()
		return nil
	}
	job := Job{ID: "job-1", Type: TypePaperAnalysis, Timeout: time.Millisecond, MaxRetry: 1}
	redisJob := &redispkg.Job{ID: job.ID, Type: redispkg.JobType(job.Type), StreamID: "1-0"}
	if err := pool.processJobWithRedisTracking(0, job, redisJob); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("processJobWithRedisTracking() error = %v, want deadline exceeded", err)
	}
	if queue.failCtx == nil || !queue.failLive {
		t.Fatalf("Fail() context = %v, want a live persistence context", queue.failCtx)
	}
}

// Protects successful acknowledgements from inheriting an already-expired job deadline.
func TestRedisPoolCompletionUsesPoolPersistenceContext(t *testing.T) {
	queue := &fakeRedisQueue{}
	pool, err := NewRedisPool(t.Context(), 1, queue)
	if err != nil {
		t.Fatalf("NewRedisPool() error = %v", err)
	}
	pool.handler = func(context.Context, Job) error { return nil }
	job := Job{ID: "job-1", Type: TypePaperAnalysis, Timeout: time.Second}
	redisJob := &redispkg.Job{ID: job.ID, Type: redispkg.JobType(job.Type), StreamID: "1-0"}
	if err := pool.processJobWithRedisTracking(0, job, redisJob); err != nil {
		t.Fatalf("processJobWithRedisTracking() error = %v", err)
	}
	if queue.completeCtx == nil || !queue.completeLive {
		t.Fatalf("Complete() context = %v, want a live persistence context", queue.completeCtx)
	}
}

// Protects Redis metrics distinguish recovered attempts from terminal failures.
func TestRedisPoolMetricsTrackRecoveredAndTerminalFailures(t *testing.T) {
	queue := &fakeRedisQueue{failResult: redispkg.FailResult{Requeued: true}}
	pool, err := NewRedisPool(t.Context(), 1, queue)
	if err != nil {
		t.Fatalf("NewRedisPool() error = %v", err)
	}
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

// Protects processor construction from deferring missing dependencies until job execution.
func TestProcessorRejectsMissingDependencies(t *testing.T) {
	if processor, err := NewProcessor(nil, nil, nil, nil, nil, nil, 350, 50, 10); err == nil || processor != nil {
		t.Fatalf("NewProcessor() = (%v, %v), want an explicit dependency error", processor, err)
	}
}

// Protects Redis-only pool construction from accepting missing dependencies.
func TestRedisPoolRejectsMissingDependencies(t *testing.T) {
	queue := &fakeRedisQueue{}
	if pool, err := NewRedisPool(nil, 1, queue); err == nil || pool != nil {
		t.Fatalf("NewRedisPool(nil context) = (%v, %v), want dependency error", pool, err)
	}
	if pool, err := NewRedisPool(t.Context(), 0, queue); err == nil || pool != nil {
		t.Fatalf("NewRedisPool(zero workers) = (%v, %v), want validation error", pool, err)
	}
	if pool, err := NewRedisPool(t.Context(), 1, nil); err == nil || pool != nil {
		t.Fatalf("NewRedisPool(nil queue) = (%v, %v), want dependency error", pool, err)
	}
}

type unavailableRedisQueue struct{ err error }

func (q unavailableRedisQueue) EnsureGroup(context.Context) error         { return q.err }
func (unavailableRedisQueue) Enqueue(context.Context, redispkg.Job) error { return nil }
func (unavailableRedisQueue) Dequeue(context.Context, string, time.Duration) (*redispkg.Job, error) {
	return nil, nil
}
func (unavailableRedisQueue) Complete(context.Context, string) error { return nil }
func (unavailableRedisQueue) Fail(context.Context, redispkg.Job, string) (redispkg.FailResult, error) {
	return redispkg.FailResult{}, nil
}
func (unavailableRedisQueue) QueueDepth(context.Context) (int64, error) { return 0, nil }

// Protects server startup from continuing when Redis cannot create its consumer group.
func TestRedisPoolStartFailsWhenConsumerGroupIsUnavailable(t *testing.T) {
	queue := unavailableRedisQueue{err: errors.New("Redis unavailable")}
	pool, err := NewRedisPool(t.Context(), 1, queue)
	if err != nil {
		t.Fatalf("NewRedisPool() error = %v", err)
	}
	pool.SetHandler(func(context.Context, Job) error { return nil })
	if err := pool.Start(); err == nil || !errors.Is(err, queue.err) {
		t.Fatalf("Start() error = %v, want consumer group failure", err)
	}
	if pool.IsRunning() {
		t.Fatal("pool reported running after consumer group startup failure")
	}
}

type blockingRedisQueue struct {
	dequeueStarted chan struct{}
	once           sync.Once
}

func (*blockingRedisQueue) EnsureGroup(context.Context) error           { return nil }
func (*blockingRedisQueue) Enqueue(context.Context, redispkg.Job) error { return nil }
func (q *blockingRedisQueue) Dequeue(ctx context.Context, _ string, _ time.Duration) (*redispkg.Job, error) {
	q.once.Do(func() { close(q.dequeueStarted) })
	<-ctx.Done()
	return nil, ctx.Err()
}
func (*blockingRedisQueue) Complete(context.Context, string) error { return nil }
func (*blockingRedisQueue) Fail(context.Context, redispkg.Job, string) (redispkg.FailResult, error) {
	return redispkg.FailResult{}, nil
}
func (*blockingRedisQueue) QueueDepth(context.Context) (int64, error) { return 0, nil }

// Protects Redis workers from leaking when pool shutdown cancels a blocking dequeue.
func TestRedisPoolShutdownCancelsBlockingDequeue(t *testing.T) {
	queue := &blockingRedisQueue{dequeueStarted: make(chan struct{})}
	pool, err := NewRedisPool(t.Context(), 1, queue)
	if err != nil {
		t.Fatalf("NewRedisPool() error = %v", err)
	}
	pool.SetHandler(func(context.Context, Job) error { return nil })
	if err := pool.Start(); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	select {
	case <-queue.dequeueStarted:
	case <-time.After(time.Second):
		t.Fatal("worker did not begin Redis dequeue")
	}
	pool.StopAndWait(time.Second)
	if pool.IsRunning() {
		t.Fatal("pool remained running after shutdown")
	}
}

type depthRedisQueue struct {
	depth    int64
	enqueued redispkg.Job
}

func (*depthRedisQueue) EnsureGroup(context.Context) error { return nil }
func (q *depthRedisQueue) Enqueue(_ context.Context, job redispkg.Job) error {
	q.enqueued = job
	return nil
}
func (*depthRedisQueue) Dequeue(ctx context.Context, _ string, _ time.Duration) (*redispkg.Job, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func (*depthRedisQueue) Complete(context.Context, string) error { return nil }
func (*depthRedisQueue) Fail(context.Context, redispkg.Job, string) (redispkg.FailResult, error) {
	return redispkg.FailResult{}, nil
}
func (q *depthRedisQueue) QueueDepth(context.Context) (int64, error) { return q.depth, nil }

// Protects Redis queue-depth metrics from losing newly enqueued jobs.
func TestRedisPoolSubmitRecordsRedisQueueDepth(t *testing.T) {
	queue := &depthRedisQueue{depth: 3}
	pool, err := NewRedisPool(t.Context(), 1, queue)
	if err != nil {
		t.Fatalf("NewRedisPool() error = %v", err)
	}
	pool.SetHandler(func(context.Context, Job) error { return nil })
	if err := pool.Submit(Job{ID: "job-1", Type: TypePaperAnalysis}); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	defer pool.StopAndWait(time.Second)
	if queue.enqueued.ID != "job-1" {
		t.Fatalf("enqueued job ID = %q, want job-1", queue.enqueued.ID)
	}
	if got := pool.GetMetrics().JobsQueued; got != 3 {
		t.Fatalf("queued metric = %d, want 3", got)
	}
}
