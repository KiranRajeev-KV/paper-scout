package worker

import (
	"context"
	"sync"
	"time"

	"github.com/research-agent/internal/logger"
	"github.com/research-agent/internal/storage/redis"
)

type Pool struct {
	workers     int
	jobQueue    chan Job
	handler     JobHandler
	wg          sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
	started     bool
	mu          sync.Mutex
	metrics     *Metrics
	redisQueue  *redis.Queue
	useRedis    bool
	pollTimeout time.Duration
}

type JobHandler func(ctx context.Context, job Job) error

type Metrics struct {
	JobsProcessed int64
	JobsFailed    int64
	JobsQueued    int64
	ActiveWorkers int
	mu            sync.RWMutex
}

func (m *Metrics) RecordProcessed() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.JobsProcessed++
}

func (m *Metrics) RecordFailed() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.JobsFailed++
}

func (m *Metrics) SetQueued(count int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.JobsQueued = count
}

func (m *Metrics) SetActive(count int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ActiveWorkers = count
}

func (m *Metrics) Snapshot() (processed, failed, queued int64, active int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.JobsProcessed, m.JobsFailed, m.JobsQueued, m.ActiveWorkers
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

func NewRedisPool(workers int, queue *redis.Queue) *Pool {
	ctx, cancel := context.WithCancel(context.Background())
	return &Pool{
		workers:     workers,
		jobQueue:    nil,
		ctx:         ctx,
		cancel:      cancel,
		metrics:     &Metrics{},
		redisQueue:  queue,
		useRedis:    true,
		pollTimeout: 5 * time.Second,
	}
}

func (p *Pool) SetHandler(handler JobHandler) {
	p.handler = handler
}

func (p *Pool) Start() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.started {
		return
	}

	if p.handler == nil {
		panic("worker pool: handler not set")
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
}

func (p *Pool) worker(id int) {
	defer p.wg.Done()

	p.metrics.SetActive(p.workers)

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
			p.processJob(id, job)
		}
	}
}

func (p *Pool) redisWorker(id int) {
	for {
		select {
		case <-p.ctx.Done():
			logger.Debug().Int("worker_id", id).Msg("Worker stopped")
			return
		default:
		}

		job, err := p.redisQueue.Dequeue(p.ctx, p.pollTimeout)
		if err != nil {
			logger.Warn().Err(err).Int("worker_id", id).Msg("Failed to dequeue job")
			continue
		}

		if job == nil {
			continue
		}

		workerJob := Job{
			ID:      job.ID,
			Type:    Type(job.Type),
			Payload: job.Payload,
			Timeout: time.Duration(job.MaxRetry) * time.Minute,
		}

		if err := p.processJobWithRedisTracking(id, workerJob, job.ID); err != nil {
			logger.Error().Err(err).Str("job_id", workerJob.ID).Msg("Job processing error")
		}
	}
}

func (p *Pool) processJobWithRedisTracking(workerID int, job Job, redisJobID string) error {
	start := time.Now()

	ctx, cancel := context.WithTimeout(p.ctx, job.Timeout)
	defer cancel()

	logger.Debug().
		Int("worker_id", workerID).
		Str("job_id", job.ID).
		Str("job_type", string(job.Type)).
		Msg("Processing job")

	err := p.handler(ctx, job)

	duration := time.Since(start)

	if err != nil {
		p.metrics.RecordFailed()
		logger.Error().
			Err(err).
			Str("job_id", job.ID).
			Dur("duration", duration).
			Msg("Job failed")

		redisJob := p.jobToRedisJob(job, redisJobID)
		if failErr := p.redisQueue.Fail(p.ctx, *redisJob, err.Error()); failErr != nil {
			logger.Warn().Err(failErr).Msg("Failed to mark job as failed in Redis")
		}
		return err
	}

	p.metrics.RecordProcessed()
	logger.Debug().
		Str("job_id", job.ID).
		Dur("duration", duration).
		Msg("Job completed")

	if completeErr := p.redisQueue.Complete(p.ctx, redisJobID); completeErr != nil {
		logger.Warn().Err(completeErr).Msg("Failed to mark job as complete in Redis")
	}

	return nil
}

func (p *Pool) jobToRedisJob(job Job, id string) *redis.Job {
	return &redis.Job{
		ID:        id,
		Type:      redis.JobType(job.Type),
		Payload:   job.Payload,
		CreatedAt: time.Now(),
		MaxRetry:  3,
	}
}

func (p *Pool) processJob(workerID int, job Job) {
	start := time.Now()

	ctx, cancel := context.WithTimeout(p.ctx, job.Timeout)
	defer cancel()

	logger.Debug().
		Int("worker_id", workerID).
		Str("job_id", job.ID).
		Str("job_type", string(job.Type)).
		Msg("Processing job")

	err := p.handler(ctx, job)

	duration := time.Since(start)

	if err != nil {
		p.metrics.RecordFailed()
		logger.Error().
			Err(err).
			Str("job_id", job.ID).
			Dur("duration", duration).
			Msg("Job failed")
	} else {
		p.metrics.RecordProcessed()
		logger.Debug().
			Str("job_id", job.ID).
			Dur("duration", duration).
			Msg("Job completed")
	}
}

func (p *Pool) Submit(job Job) error {
	p.mu.Lock()
	started := p.started
	p.mu.Unlock()

	if !started {
		p.Start()
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
	redisJob := redis.Job{
		ID:        job.ID,
		Type:      redis.JobType(job.Type),
		Payload:   job.Payload,
		Priority:  0,
		CreatedAt: time.Now(),
		Retries:   0,
		MaxRetry:  3,
	}

	if err := p.redisQueue.Enqueue(p.ctx, redisJob); err != nil {
		return err
	}

	queued, err := p.redisQueue.QueueDepth(p.ctx)
	if err == nil {
		p.metrics.SetQueued(queued)
	}

	return nil
}

func (p *Pool) Stop() {
	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return
	}
	p.started = false
	p.mu.Unlock()

	p.cancel()

	if !p.useRedis && p.jobQueue != nil {
		close(p.jobQueue)
	}

	p.wg.Wait()

	logger.Info().Msg("Worker pool stopped")
}

func (p *Pool) StopAndWait(timeout time.Duration) {
	p.Stop()
}

func (p *Pool) GetMetrics() (processed, failed, queued int64, active int) {
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
