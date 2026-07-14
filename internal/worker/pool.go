package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/paper-scout/internal/logger"
	"github.com/paper-scout/internal/storage/redis"
)

type Pool struct {
	workers         int
	jobQueue        chan Job
	handler         JobHandler
	hook            CompletionHook
	wg              sync.WaitGroup
	ctx             context.Context
	cancel          context.CancelFunc
	started         bool
	mu              sync.Mutex
	metrics         *Metrics
	redisQueue      RedisQueue
	useRedis        bool
	pollTimeout     time.Duration
	consumerKey     string
	decorateContext func(context.Context, Job) context.Context
	decorateJob     func(Job) Job
}

type JobHandler func(ctx context.Context, job Job) error
type CompletionHook func(job Job, err error, terminal bool)

type RedisQueue interface {
	EnsureGroup(ctx context.Context) error
	Enqueue(ctx context.Context, job redis.Job) error
	Dequeue(ctx context.Context, consumer string, timeout time.Duration) (*redis.Job, error)
	Complete(ctx context.Context, streamID string) error
	Fail(ctx context.Context, job redis.Job, errMsg string) (redis.FailResult, error)
	QueueDepth(ctx context.Context) (int64, error)
}

type Metrics struct {
	JobsProcessed         int64
	JobAttemptsFailed     int64
	JobsRetried           int64
	JobsTerminallyFailed  int64
	JobStateWriteFailures int64
	JobsQueued            int64
	ActiveWorkers         int
	mu                    sync.RWMutex
}

type MetricsSnapshot struct {
	JobsProcessed         int64
	JobAttemptsFailed     int64
	JobsRetried           int64
	JobsTerminallyFailed  int64
	JobStateWriteFailures int64
	JobsQueued            int64
	ActiveWorkers         int
}

func (m *Metrics) RecordProcessed() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.JobsProcessed++
}

func (m *Metrics) RecordFailedAttempt() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.JobAttemptsFailed++
}

func (m *Metrics) RecordRetry() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.JobsRetried++
}

func (m *Metrics) RecordTerminalFailure() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.JobsTerminallyFailed++
}

func (m *Metrics) RecordStateWriteFailure() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.JobStateWriteFailures++
}

func (m *Metrics) SetQueued(count int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.JobsQueued = count
}

func (m *Metrics) AddActive(delta int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ActiveWorkers += delta
}

func (m *Metrics) Snapshot() MetricsSnapshot {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return MetricsSnapshot{
		JobsProcessed:         m.JobsProcessed,
		JobAttemptsFailed:     m.JobAttemptsFailed,
		JobsRetried:           m.JobsRetried,
		JobsTerminallyFailed:  m.JobsTerminallyFailed,
		JobStateWriteFailures: m.JobStateWriteFailures,
		JobsQueued:            m.JobsQueued,
		ActiveWorkers:         m.ActiveWorkers,
	}
}

func NewPool(workers int, queueSize int) *Pool {
	ctx, cancel := context.WithCancel(context.Background())
	return &Pool{
		workers:     workers,
		jobQueue:    make(chan Job, queueSize),
		ctx:         ctx,
		cancel:      cancel,
		metrics:     &Metrics{},
		useRedis:    false,
		pollTimeout: 5 * time.Second,
	}
}

func NewRedisPool(workers int, queue RedisQueue) *Pool {
	ctx, cancel := context.WithCancel(context.Background())
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "paper-scout"
	}
	return &Pool{
		workers:     workers,
		jobQueue:    nil,
		ctx:         ctx,
		cancel:      cancel,
		metrics:     &Metrics{},
		redisQueue:  queue,
		useRedis:    true,
		pollTimeout: 5 * time.Second,
		consumerKey: fmt.Sprintf("%s-%d-%d", hostname, os.Getpid(), time.Now().UnixNano()),
	}
}

func (p *Pool) SetHandler(handler JobHandler) {
	p.handler = handler
}

func (p *Pool) SetCompletionHook(hook CompletionHook) {
	p.hook = hook
}

func (p *Pool) SetContextDecorator(decorator func(context.Context, Job) context.Context) {
	p.decorateContext = decorator
}

// SetJobDecorator installs immutable correlation metadata before a job enters either queue.
func (p *Pool) SetJobDecorator(decorator func(Job) Job) {
	p.decorateJob = decorator
}

func (p *Pool) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.started {
		return nil
	}
	if p.workers < 1 {
		return fmt.Errorf("worker pool size must be positive")
	}

	if p.handler == nil {
		return fmt.Errorf("worker pool handler is not set")
	}

	if p.useRedis {
		if p.redisQueue == nil {
			return fmt.Errorf("Redis worker queue is not configured")
		}
		if err := p.redisQueue.EnsureGroup(p.ctx); err != nil {
			return fmt.Errorf("ensure Redis worker consumer group: %w", err)
		}
	}

	p.started = true

	for i := 0; i < p.workers; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}

	logger.Info().
		Int("workers", p.workers).
		Bool("use_redis", p.useRedis).
		Msg("Worker pool started")
	return nil
}

func (p *Pool) worker(id int) {
	defer p.wg.Done()

	if p.useRedis {
		p.redisWorker(id)
	} else {
		p.localWorker(id)
	}
}

func (p *Pool) localWorker(id int) {
	for {
		select {
		case <-p.ctx.Done():
			logger.Debug().Int("worker_id", id).Msg("Worker stopped")
			return
		case job, ok := <-p.jobQueue:
			if !ok {
				return
			}
			p.metrics.SetQueued(int64(len(p.jobQueue)))
			p.processJob(id, job)
		}
	}
}

func (p *Pool) redisWorker(id int) {
	consumer := fmt.Sprintf("%s-%d", p.consumerKey, id)

	for {
		select {
		case <-p.ctx.Done():
			logger.Debug().Int("worker_id", id).Msg("Worker stopped")
			return
		default:
		}

		job, err := p.redisQueue.Dequeue(p.ctx, consumer, p.pollTimeout)
		if err != nil {
			logger.Warn().Err(err).Int("worker_id", id).Msg("Failed to dequeue job")
			continue
		}

		if job == nil {
			continue
		}

		workerJob := fromRedisJob(*job)

		if workerJob.Timeout <= 0 {
			workerJob.Timeout = 10 * time.Minute
		}

		if err := p.processJobWithRedisTracking(id, workerJob, job); err != nil {
			logger.Error().Err(err).Str("job_id", workerJob.ID).Msg("Job processing error")
		}
	}
}

func (p *Pool) processJobWithRedisTracking(workerID int, job Job, redisJob *redis.Job) error {
	start := time.Now()
	p.metrics.AddActive(1)
	defer p.metrics.AddActive(-1)

	base := p.pctx(job)
	ctx, cancel := context.WithTimeout(base, job.Timeout)
	defer cancel()

	logger.From(ctx).Debug().
		Int("worker_id", workerID).
		Str("job_id", job.ID).
		Str("job_type", string(job.Type)).
		Msg("Processing job")

	err := p.handler(ctx, job)

	duration := time.Since(start)

	if err != nil {
		p.metrics.RecordFailedAttempt()
		logger.From(ctx).Error().
			Err(err).
			Str("job_id", job.ID).
			Dur("duration", duration).
			Msg("Job failed")

		failedJob := p.jobToRedisJob(job, redisJob)
		failure, failErr := p.redisQueue.Fail(p.ctx, *failedJob, err.Error())
		if failErr != nil {
			p.metrics.RecordStateWriteFailure()
			logger.From(ctx).Warn().Err(failErr).Msg("Failed to mark job as failed in Redis")
			p.refreshRedisQueueDepth()
			return errors.Join(err, fmt.Errorf("persist Redis job failure: %w", failErr))
		}
		if failure.Requeued {
			p.metrics.RecordRetry()
		}
		if failure.Terminal {
			p.metrics.RecordTerminalFailure()
		}
		logger.From(ctx).Warn().
			Str("job_id", job.ID).
			Int("attempt", job.Retries+1).
			Int("max_attempts", job.MaxRetry).
			Bool("terminal", failure.Terminal).
			Msg("Job attempt finished with failure")
		p.notifyCompletion(job, err, failure.Terminal)
		p.refreshRedisQueueDepth()
		return err
	}

	if completeErr := p.redisQueue.Complete(p.ctx, redisJob.StreamID); completeErr != nil {
		logger.From(ctx).Warn().Err(completeErr).Msg("Failed to mark job as complete in Redis")
		p.metrics.RecordStateWriteFailure()
		p.refreshRedisQueueDepth()
		return fmt.Errorf("persist Redis job completion: %w", completeErr)
	}

	p.metrics.RecordProcessed()
	logger.From(ctx).Debug().
		Str("job_id", job.ID).
		Dur("duration", duration).
		Msg("Job completed")
	p.notifyCompletion(job, nil, true)
	p.refreshRedisQueueDepth()

	return nil
}

func (p *Pool) jobToRedisJob(job Job, source *redis.Job) *redis.Job {
	converted := toRedisJob(job)
	converted.CreatedAt = source.CreatedAt
	converted.StreamID = source.StreamID
	return &converted
}

func (p *Pool) processJob(workerID int, job Job) {
	start := time.Now()
	p.metrics.AddActive(1)
	defer p.metrics.AddActive(-1)

	base := p.pctx(job)
	ctx, cancel := context.WithTimeout(base, job.Timeout)
	defer cancel()

	logger.From(ctx).Debug().
		Int("worker_id", workerID).
		Str("job_id", job.ID).
		Str("job_type", string(job.Type)).
		Msg("Processing job")

	err := p.handler(ctx, job)

	duration := time.Since(start)

	if err != nil {
		p.metrics.RecordFailedAttempt()
		p.metrics.RecordTerminalFailure()
		logger.From(ctx).Error().
			Err(err).
			Str("job_id", job.ID).
			Dur("duration", duration).
			Msg("Job failed")
	} else {
		p.metrics.RecordProcessed()
		logger.From(ctx).Debug().
			Str("job_id", job.ID).
			Dur("duration", duration).
			Msg("Job completed")
	}

	p.notifyCompletion(job, err, true)
}

func (p *Pool) pctx(job Job) context.Context {
	if p.decorateContext != nil {
		return p.decorateContext(p.ctx, job)
	}
	return p.ctx
}

func (p *Pool) notifyCompletion(job Job, err error, terminal bool) {
	if p.hook != nil {
		p.hook(job, err, terminal)
	}
}

func (p *Pool) Submit(job Job) error {
	if err := p.ctx.Err(); err != nil {
		return err
	}

	p.mu.Lock()
	started := p.started
	p.mu.Unlock()

	if !started {
		if err := p.ctx.Err(); err != nil {
			return err
		}
		if err := p.Start(); err != nil {
			return err
		}
	}
	if p.decorateJob != nil {
		job = p.decorateJob(job)
	}

	if p.useRedis {
		return p.submitToRedis(job)
	}

	return p.submitLocal(job)
}

func (p *Pool) submitLocal(job Job) error {
	select {
	case <-p.ctx.Done():
		return p.ctx.Err()
	case p.jobQueue <- job:
		p.metrics.SetQueued(int64(len(p.jobQueue)))
		return nil
	}
}

func (p *Pool) submitToRedis(job Job) error {
	redisJob := toRedisJob(job)

	if err := p.redisQueue.Enqueue(p.ctx, redisJob); err != nil {
		return err
	}

	queued, err := p.redisQueue.QueueDepth(p.ctx)
	if err == nil {
		p.metrics.SetQueued(queued)
	}

	return nil
}

func toRedisJob(job Job) redis.Job {
	return redis.Job{
		ID: job.ID, Type: redis.JobType(job.Type), Payload: job.Payload, Priority: job.Priority,
		Timeout: job.Timeout, CreatedAt: job.CreatedAt, Retries: job.Retries, MaxRetry: job.MaxRetry,
		RunID: job.RunID, TopicID: job.TopicID, TraceID: job.TraceID,
	}
}

func fromRedisJob(job redis.Job) Job {
	return Job{
		ID: job.ID, Type: Type(job.Type), Payload: job.Payload, Priority: job.Priority,
		Timeout: job.Timeout, CreatedAt: job.CreatedAt, Retries: job.Retries, MaxRetry: job.MaxRetry,
		RunID: job.RunID, TopicID: job.TopicID, TraceID: job.TraceID,
	}
}

func (p *Pool) refreshRedisQueueDepth() {
	if !p.useRedis {
		return
	}
	if queued, err := p.redisQueue.QueueDepth(p.ctx); err == nil {
		p.metrics.SetQueued(queued)
	}
}

func (p *Pool) Stop() {
	p.stop()
	p.wg.Wait()

	logger.Info().Msg("Worker pool stopped")
}

func (p *Pool) stop() {
	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return
	}
	p.started = false
	p.mu.Unlock()

	p.cancel()
}

func (p *Pool) StopAndWait(timeout time.Duration) {
	p.stop()
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	if timeout <= 0 {
		logger.Warn().Msg("Worker pool shutdown deadline reached")
		return
	}
	select {
	case <-done:
	case <-time.After(timeout):
		logger.Warn().Dur("timeout", timeout).Msg("Timed out waiting for worker pool")
	}
}

func (p *Pool) GetMetrics() MetricsSnapshot {
	return p.metrics.Snapshot()
}

func (p *Pool) QueueDepth() int {
	if p.useRedis {
		depth, err := p.redisQueue.QueueDepth(p.ctx)
		if err != nil {
			return 0
		}
		return int(depth)
	}
	return len(p.jobQueue)
}

func (p *Pool) IsRunning() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.started
}

func (p *Pool) UseRedis() bool {
	return p.useRedis
}
