// Package application assembles focused services for the server process.
package application

import (
	"context"
	"fmt"
	"time"

	"github.com/paper-scout/internal/abstractindex"
	"github.com/paper-scout/internal/agent"
	"github.com/paper-scout/internal/config"
	"github.com/paper-scout/internal/httpresilience"
	"github.com/paper-scout/internal/llm"
	"github.com/paper-scout/internal/logger"
	"github.com/paper-scout/internal/orchestrator"
	"github.com/paper-scout/internal/storage/postgres"
	"github.com/paper-scout/internal/storage/qdrant"
	"github.com/paper-scout/internal/storage/redis"
	"github.com/paper-scout/internal/tools/arxiv"
	"github.com/paper-scout/internal/tools/embedding"
	"github.com/paper-scout/internal/tools/pdf"
	"github.com/paper-scout/internal/tools/semantic_scholar"
	"github.com/paper-scout/internal/worker"
)

// Dependencies contains initialized infrastructure needed by the research application.
type Dependencies struct {
	Postgres          *postgres.Client
	Redis             *redis.Client
	Qdrant            *qdrant.Client
	Generator         llm.Generator
	EmbeddingProvider embedding.Embedder
	Parser            worker.DocumentParser
	SemanticScholar   *semantic_scholar.Client
	Arxiv             *arxiv.Client
}

// NewResearchService assembles the research application without implementing pipeline business logic.
func NewResearchService(appCtx context.Context, cfg *config.Config, logs *logger.Manager, deps Dependencies) (*orchestrator.Orchestrator, error) {
	if cfg == nil || logs == nil || deps.Postgres == nil || deps.Redis == nil || deps.Qdrant == nil || deps.Generator == nil || deps.EmbeddingProvider == nil || deps.Parser == nil || deps.SemanticScholar == nil || deps.Arxiv == nil {
		return nil, fmt.Errorf("application requires configuration, logging, and all infrastructure dependencies")
	}
	if appCtx == nil {
		appCtx = context.Background()
	}
	appCtx = logs.App().WithContext(appCtx)
	appCtx, cancel := context.WithCancel(appCtx)

	downloader := pdf.NewDownloaderWithPolicyAndMaxBytes(cfg.Pipeline.PDFDownloadTimeout, httpresilience.New(appCtx, "pdf", httpresilience.Config{
		MaxRetries: cfg.Pipeline.PDFResilience.MaxRetries, BaseBackoff: cfg.Pipeline.PDFResilience.BaseBackoff,
		MaxBackoff: cfg.Pipeline.PDFResilience.MaxBackoff, FailureThreshold: cfg.Pipeline.PDFResilience.FailureThreshold,
		OpenTimeout: cfg.Pipeline.PDFResilience.OpenTimeout,
	}, cfg.Pipeline.PDFRateLimit.RequestsPerSecond, cfg.Pipeline.PDFRateLimit.Burst, nil), cfg.Pipeline.PDFMaxBytes)
	embedder := embedding.NewGenerator(deps.EmbeddingProvider, deps.Qdrant)
	queue := redis.NewQueue(deps.Redis.Client(), deps.Redis.WorkerClient(), redis.QueueOptions{ClaimIdle: cfg.Pipeline.JobTimeout + time.Minute})
	pool, err := worker.NewRedisPool(appCtx, cfg.Pipeline.WorkerPoolSize, queue)
	if err != nil {
		cancel()
		return nil, err
	}
	analyzer := agent.NewAnalyzer(deps.Generator, deps.Postgres, pool, cfg.Pipeline.JobTimeout)
	indexer := agent.NewIndexer(deps.Postgres, pool, embedder)
	processor, err := worker.NewProcessor(deps.Postgres, downloader, deps.Parser, embedder, analyzer.HandleJob, indexer, cfg.Pipeline.ChunkMaxWords, cfg.Pipeline.ChunkOverlap, cfg.Pipeline.EmbeddingBatchSize)
	if err != nil {
		cancel()
		return nil, err
	}
	abstracts, err := abstractindex.NewService(deps.Postgres, embedder, processor)
	if err != nil {
		cancel()
		return nil, err
	}
	ranker, err := agent.NewRanker(deps.Postgres, embedder, deps.Generator, abstracts)
	if err != nil {
		cancel()
		return nil, err
	}
	cleanupResult, cleanupErr := processor.ReconcileEmbeddingCleanup(appCtx, 1000)
	if cleanupErr != nil {
		logger.From(appCtx).Warn().Err(cleanupErr).Int("pending", cleanupResult.Pending).Msg("Startup embedding cleanup remains retryable")
	}
	pool.SetHandler(processor.CreateHandler())
	pool.SetCompletionHook(func(ctx context.Context, job worker.Job, err error, terminal bool) {
		indexer.HandleJobCompletion(ctx, job, err, terminal)
		analyzer.HandleJobCompletion(job, err, terminal)
	})
	pool.SetContextDecorator(func(ctx context.Context, job worker.Job) context.Context {
		return logs.ContextForTopic(ctx, job.TopicID)
	})
	pool.SetJobDecorator(func(job worker.Job) worker.Job {
		if job.RunID == "" {
			if runID, ok := logs.RunIDForTopic(job.TopicID); ok {
				job.RunID = runID
			}
		}
		if job.TraceID == "" {
			job.TraceID = job.ID
		}
		if job.Payload == nil {
			job.Payload = make(map[string]interface{})
		}
		job.Payload["run_id"], job.Payload["trace_id"] = job.RunID, job.TraceID
		return job
	})
	if err := pool.Start(); err != nil {
		cancel()
		return nil, fmt.Errorf("start worker pool: %w", err)
	}
	abort := func() {
		cancel()
		pool.StopAndWait(5 * time.Second)
	}

	sse := orchestrator.NewSSEManager(appCtx)
	snapshots := orchestrator.NewStateManager(deps.Redis)
	state, err := orchestrator.NewPipelineStateService(snapshots, deps.Postgres, sse)
	if err != nil {
		abort()
		return nil, err
	}
	checkpoints, err := orchestrator.NewCheckpointService(deps.Postgres)
	if err != nil {
		abort()
		return nil, err
	}
	reportGenerator := agent.NewReportGenerator(deps.Postgres)
	reports := orchestrator.NewReportService(reportGenerator)
	stages, err := orchestrator.NewAgentPipelineStages(cfg, deps.Postgres, agent.NewQueryExpander(deps.Generator, deps.Postgres), agent.NewPaperDiscoverer(deps.SemanticScholar, deps.Arxiv, deps.Postgres, cfg.Pipeline.MaxPapers), ranker, analyzer, indexer, agent.NewGapDetector(deps.Generator, deps.Postgres, embedder, cfg.Pipeline.MaxRetrievedChunks), agent.NewFeasibilityEvaluator(deps.Generator, deps.Postgres), reportGenerator)
	if err != nil {
		abort()
		return nil, err
	}
	runner, err := orchestrator.NewPipelineRunner(appCtx, cfg, checkpoints, stages, state, reports, logs)
	if err != nil {
		abort()
		return nil, err
	}
	runs, err := orchestrator.NewRunManager(appCtx, cancel, runner, logs, pool)
	if err != nil {
		abort()
		return nil, err
	}
	coordinator, err := orchestrator.NewRunCoordinator(deps.Postgres, logs, state, runs, runner)
	if err != nil {
		abort()
		return nil, err
	}
	recovery, err := orchestrator.NewRecoveryService(deps.Postgres, snapshots, state, logs, runs)
	if err != nil {
		abort()
		return nil, err
	}
	recovery.Recover(appCtx)
	service, err := orchestrator.NewOrchestrator(coordinator, state, reports, runs, sse)
	if err != nil {
		abort()
		return nil, err
	}
	return service, nil
}
