package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/paper-scout/internal/agent"
	"github.com/paper-scout/internal/config"
	"github.com/paper-scout/internal/httpresilience"
	"github.com/paper-scout/internal/llm"
	"github.com/paper-scout/internal/logger"
	"github.com/paper-scout/internal/storage/postgres"
	"github.com/paper-scout/internal/storage/qdrant"
	"github.com/paper-scout/internal/storage/redis"
	"github.com/paper-scout/internal/tools/arxiv"
	"github.com/paper-scout/internal/tools/embedding"
	"github.com/paper-scout/internal/tools/pdf"
	"github.com/paper-scout/internal/tools/semantic_scholar"
	"github.com/paper-scout/internal/worker"
)

type Orchestrator struct {
	appCtx    context.Context
	appCancel context.CancelFunc
	runs      sync.WaitGroup

	config   *config.Config
	logs     *logger.Manager
	postgres *postgres.Client
	redis    *redis.Client
	qdrant   *qdrant.Client

	queryExpander   *agent.QueryExpander
	paperDiscoverer *agent.PaperDiscoverer
	ranker          *agent.Ranker
	analyzer        *agent.Analyzer
	indexer         *agent.Indexer
	gapDetector     *agent.GapDetector
	feasibility     *agent.FeasibilityEvaluator
	reportGenerator *agent.ReportGenerator

	workerPool *worker.Pool
	runFn      func(context.Context, *Pipeline)

	// Test hooks keep stage-transition coverage independent of external services.
	stageCompletedFn       func(context.Context, *Pipeline, Stage, interface{}) (bool, error)
	startStageFn           func(context.Context, *Pipeline, Stage) error
	completeStageFn        func(context.Context, *Pipeline, Stage, interface{}) error
	failStageFn            func(context.Context, *Pipeline, Stage, error) error
	persistTerminalStateFn func(context.Context, *Pipeline) error
	expandFn               func(context.Context, string, string) (*agent.ExpandedQuery, error)
	discoverFn             func(context.Context, string, string, *agent.ExpandedQuery) ([]agent.DiscoveredPaper, error)
	countPapersFn          func(context.Context, string) (int64, error)
	rankFn                 func(context.Context, string, string, int) ([]agent.RankedPaper, error)
	pendingRankedPapersFn  func(context.Context, string, []agent.RankedPaper) ([]agent.RankedPaper, error)
	analyzePapersFn        func(context.Context, string, []agent.RankedPaper) error
	detectFn               func(context.Context, string, string) ([]agent.ResearchGap, error)
	evaluateFn             func(context.Context, string, []agent.ResearchGap) ([]agent.FeasibilityResult, error)
	generateReportFn       func(context.Context, string) (*agent.Report, error)
	loadStateFn            func(context.Context, string) (*Pipeline, error)
	getResearchTopicFn     func(context.Context, uuid.UUID) (*postgres.ResearchTopic, error)

	sse   *SSEManager
	state *StateManager

	mu        sync.RWMutex
	pipelines map[string]*Pipeline
	reportMu  sync.Mutex
	reports   map[string]*agent.Report
}

type Pipeline struct {
	TopicID   string
	RunID     string
	Topic     string
	Status    string
	Stage     Stage
	Progress  float64
	StartedAt time.Time
	UpdatedAt time.Time
	Error     string
}

type Stage string

var (
	ErrPipelineNotFound = errors.New("pipeline not found")
	ErrInvalidTopicID   = errors.New("invalid topic ID")
)

const (
	StagePending      Stage = "pending"
	StageQueryExpand  Stage = "query_expansion"
	StageDiscovery    Stage = "paper_discovery"
	StageRanking      Stage = "ranking"
	StageAnalysis     Stage = "paper_analysis"
	StageGapDetection Stage = "gap_detection"
	StageFeasibility  Stage = "feasibility_evaluation"
	StageReport       Stage = "report_generation"
	StageCompleted    Stage = "completed"
	StageFailed       Stage = "failed"
)

func NewOrchestrator(
	appCtx context.Context,
	cfg *config.Config,
	logManager *logger.Manager,
	pg *postgres.Client,
	redisClient *redis.Client,
	qdrantClient *qdrant.Client,
	llmClient llm.Generator,
	embeddingProvider embedding.Embedder,
	parser worker.DocumentParser,
	ssClient *semantic_scholar.Client,
	arxivClient *arxiv.Client,
) (*Orchestrator, error) {
	if cfg == nil || logManager == nil || pg == nil || redisClient == nil || qdrantClient == nil || llmClient == nil || embeddingProvider == nil || parser == nil || ssClient == nil || arxivClient == nil {
		return nil, fmt.Errorf("orchestrator requires all configured dependencies")
	}
	if appCtx == nil {
		appCtx = context.Background()
	}
	appCtx, appCancel := context.WithCancel(appCtx)
	downloader := pdf.NewDownloaderWithPolicyAndMaxBytes(cfg.Pipeline.PDFDownloadTimeout, httpresilience.New("pdf", httpresilience.Config{
		MaxRetries: cfg.Pipeline.PDFResilience.MaxRetries, BaseBackoff: cfg.Pipeline.PDFResilience.BaseBackoff,
		MaxBackoff: cfg.Pipeline.PDFResilience.MaxBackoff, FailureThreshold: cfg.Pipeline.PDFResilience.FailureThreshold,
		OpenTimeout: cfg.Pipeline.PDFResilience.OpenTimeout,
	}, cfg.Pipeline.PDFRateLimit.RequestsPerSecond, cfg.Pipeline.PDFRateLimit.Burst, nil), cfg.Pipeline.PDFMaxBytes)
	embedder := embedding.NewGenerator(embeddingProvider, qdrantClient)

	var pool *worker.Pool
	if cfg.Pipeline.UseRedisQueue {
		redisQueue := redis.NewQueue(redisClient.Client(), redisClient.WorkerClient(), redis.QueueOptions{
			ClaimIdle: cfg.Pipeline.JobTimeout + time.Minute,
		})
		pool = worker.NewRedisPool(cfg.Pipeline.WorkerPoolSize, redisQueue)
		logger.Info().Msg("Using Redis queue for worker pool")
	} else {
		pool = worker.NewPool(cfg.Pipeline.WorkerPoolSize, 100)
		logger.Info().Msg("Using local queue for worker pool")
	}

	o := &Orchestrator{
		appCtx:     appCtx,
		appCancel:  appCancel,
		config:     cfg,
		logs:       logManager,
		postgres:   pg,
		redis:      redisClient,
		qdrant:     qdrantClient,
		workerPool: pool,
		sse:        NewSSEManager(),
		state:      NewStateManager(redisClient),
		pipelines:  make(map[string]*Pipeline),
		reports:    make(map[string]*agent.Report),
	}

	o.queryExpander = agent.NewQueryExpander(llmClient, pg)
	o.paperDiscoverer = agent.NewPaperDiscoverer(ssClient, arxivClient, pg, cfg.Pipeline.MaxPapers)
	o.analyzer = agent.NewAnalyzer(llmClient, pg, pool, cfg.Pipeline.JobTimeout)
	o.indexer = agent.NewIndexer(pg, pool, embedder)
	processor, err := worker.NewProcessor(pg, downloader, parser, embedder, o.analyzer.HandleJob, o.indexer, cfg.Pipeline.ChunkMaxWords, cfg.Pipeline.ChunkOverlap, cfg.Pipeline.EmbeddingBatchSize)
	if err != nil {
		appCancel()
		return nil, err
	}
	o.ranker = agent.NewRanker(pg, embedder, llmClient, processor)
	cleanupResult, cleanupErr := processor.ReconcileEmbeddingCleanup(appCtx, 1000)
	if cleanupErr != nil {
		logger.Warn().Err(cleanupErr).Int("pending", cleanupResult.Pending).Msg("Startup embedding cleanup remains retryable")
	} else if cleanupResult.Completed > 0 || cleanupResult.Pending > 0 {
		logger.Info().Int("completed", cleanupResult.Completed).Int("pending", cleanupResult.Pending).Msg("Reconciled embedding cleanup at startup")
	}
	pool.SetHandler(processor.CreateHandler())
	pool.SetCompletionHook(func(job worker.Job, err error, terminal bool) {
		o.indexer.HandleJobCompletion(job, err, terminal)
		o.analyzer.HandleJobCompletion(job, err, terminal)
	})
	pool.SetContextDecorator(func(ctx context.Context, job worker.Job) context.Context {
		return logManager.ContextForTopic(ctx, job.TopicID)
	})
	pool.SetJobDecorator(func(job worker.Job) worker.Job {
		if job.RunID == "" {
			if runID, ok := logManager.RunIDForTopic(job.TopicID); ok {
				job.RunID = runID
			}
		}
		if job.TraceID == "" {
			job.TraceID = job.ID
		}
		if job.Payload == nil {
			job.Payload = make(map[string]interface{})
		}
		job.Payload["run_id"] = job.RunID
		job.Payload["trace_id"] = job.TraceID
		return job
	})
	if err := pool.Start(); err != nil {
		appCancel()
		return nil, fmt.Errorf("start worker pool: %w", err)
	}
	o.gapDetector = agent.NewGapDetector(llmClient, pg, embedder, cfg.Pipeline.MaxRetrievedChunks)
	o.feasibility = agent.NewFeasibilityEvaluator(llmClient, pg)
	o.reportGenerator = agent.NewReportGenerator(pg)

	o.recoverPipelines(appCtx)

	return o, nil
}

func (o *Orchestrator) StartResearch(ctx context.Context, topic string) (*Pipeline, error) {
	topicRecord, err := o.postgres.Queries().CreateResearchTopic(ctx, postgres.CreateResearchTopicParams{
		Topic:  topic,
		Status: "pending",
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create research topic: %w", err)
	}

	pipeline := &Pipeline{
		TopicID:   topicRecord.ID.String(),
		RunID:     topicRecord.RunID.String(),
		Topic:     topic,
		Status:    "pending",
		Stage:     StagePending,
		Progress:  0,
		StartedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	if err := o.logs.StartRun(pipeline.RunID, pipeline.TopicID); err != nil {
		pipeline.Status = "failed"
		pipeline.Stage = StageFailed
		pipeline.Error = err.Error()
		_, updateErr := o.postgres.Queries().UpdateResearchTopicState(ctx, postgres.UpdateResearchTopicStateParams{
			ID: topicRecord.ID, Status: "failed", CurrentStage: string(StageFailed), Progress: 0,
			ErrorMessage: pgtype.Text{String: err.Error(), Valid: true},
		})
		if updateErr != nil {
			return nil, errors.Join(fmt.Errorf("create run log: %w", err), fmt.Errorf("persist run log failure: %w", updateErr))
		}
		return nil, fmt.Errorf("create run log: %w", err)
	}

	o.mu.Lock()
	o.pipelines[pipeline.TopicID] = clonePipeline(pipeline)
	o.mu.Unlock()

	if err := o.state.Save(ctx, pipeline.TopicID, pipeline); err != nil {
		o.failPipeline(ctx, pipeline, StagePending, fmt.Errorf("persist initial live pipeline state: %w", err))
		return nil, fmt.Errorf("start research pipeline: %w", err)
	}

	o.launchPipeline(pipeline)

	return pipeline, nil
}

func (o *Orchestrator) launchPipeline(p *Pipeline) {
	ctx, cancel := context.WithCancel(o.appCtx)
	if o.logs != nil {
		ctx = o.logs.ContextForTopic(ctx, p.TopicID)
	}
	run := o.runPipelineWithContext
	if o.runFn != nil {
		run = o.runFn
	}
	o.runs.Add(1)
	go func() {
		defer o.runs.Done()
		defer cancel()
		run(ctx, p)
	}()
}

func (o *Orchestrator) runPipelineWithContext(ctx context.Context, p *Pipeline) {

	defer func() {
		if r := recover(); r != nil {
			logger.From(ctx).Error().Interface("panic", r).Str("topic_id", p.TopicID).Msg("Pipeline panicked")
			o.failPipeline(ctx, p, p.Stage, fmt.Errorf("pipeline panic: %v", r))
		}
	}()

	var expanded *agent.ExpandedQuery
	queryCompleted, err := o.isStageCompleted(ctx, p, StageQueryExpand, &expanded)
	if err != nil {
		o.failPipeline(ctx, p, StageQueryExpand, err)
		return
	}
	if !queryCompleted {
		o.updateStatus(p, StageQueryExpand, 0.05, "")
		if err := o.beginStage(ctx, p, StageQueryExpand); err != nil {
			o.failPipeline(ctx, p, StageQueryExpand, err)
			return
		}
		expanded, err = o.expand(ctx, p.TopicID, p.Topic)
		if err != nil {
			o.failPipeline(ctx, p, StageQueryExpand, err)
			return
		}
		if err := o.finishStage(ctx, p, StageQueryExpand, expanded); err != nil {
			o.failPipeline(ctx, p, StageQueryExpand, err)
			return
		}
	}

	discoveryCompleted, err := o.isStageCompleted(ctx, p, StageDiscovery, nil)
	if err != nil {
		o.failPipeline(ctx, p, StageDiscovery, err)
		return
	}
	if !discoveryCompleted {
		o.updateStatus(p, StageDiscovery, 0.15, "")
		if err := o.beginStage(ctx, p, StageDiscovery); err != nil {
			o.failPipeline(ctx, p, StageDiscovery, err)
			return
		}
		papers, err := o.discover(ctx, p.TopicID, p.Topic, expanded)
		if err != nil {
			o.failPipeline(ctx, p, StageDiscovery, err)
			return
		}
		if len(papers) < o.config.Pipeline.MinPapersForAnalysis {
			err := fmt.Errorf("not enough papers found: %d (minimum: %d)", len(papers), o.config.Pipeline.MinPapersForAnalysis)
			o.failPipeline(ctx, p, StageDiscovery, err)
			return
		}
		if err := o.finishStage(ctx, p, StageDiscovery, map[string]int{"total": len(papers), "succeeded": len(papers), "failed": 0}); err != nil {
			o.failPipeline(ctx, p, StageDiscovery, err)
			return
		}
	} else {
		count, countErr := o.countPapers(ctx, p.TopicID)
		if countErr != nil || count < int64(o.config.Pipeline.MinPapersForAnalysis) {
			err := fmt.Errorf("recovered discovery has insufficient papers: %d", count)
			if countErr != nil {
				err = countErr
			}
			o.failPipeline(ctx, p, StageDiscovery, err)
			return
		}
	}

	var ranked []agent.RankedPaper
	rankingCompleted, err := o.isStageCompleted(ctx, p, StageRanking, &ranked)
	if err != nil {
		o.failPipeline(ctx, p, StageRanking, err)
		return
	}
	if !rankingCompleted {
		o.updateStatus(p, StageRanking, 0.25, "")
		if err := o.beginStage(ctx, p, StageRanking); err != nil {
			o.failPipeline(ctx, p, StageRanking, err)
			return
		}
		ranked, err = o.rank(ctx, p.TopicID, p.Topic, o.config.Pipeline.MaxPapers)
		if err != nil {
			o.failPipeline(ctx, p, StageRanking, err)
			return
		}
		if err := o.finishStage(ctx, p, StageRanking, ranked); err != nil {
			o.failPipeline(ctx, p, StageRanking, err)
			return
		}
	}

	analysisCompleted, err := o.isStageCompleted(ctx, p, StageAnalysis, nil)
	if err != nil {
		o.failPipeline(ctx, p, StageAnalysis, err)
		return
	}
	if !analysisCompleted {
		o.updateStatus(p, StageAnalysis, 0.35, "")
		if err := o.beginStage(ctx, p, StageAnalysis); err != nil {
			o.failPipeline(ctx, p, StageAnalysis, err)
			return
		}
		pending, err := o.pendingPapers(ctx, p.TopicID, ranked)
		if err != nil {
			o.failPipeline(ctx, p, StageAnalysis, err)
			return
		}
		if err := o.analyze(ctx, p.TopicID, pending); err != nil {
			o.failPipeline(ctx, p, StageAnalysis, err)
			return
		}
		if err := o.finishStage(ctx, p, StageAnalysis, map[string]int{"total": len(pending), "succeeded": len(pending), "failed": 0}); err != nil {
			o.failPipeline(ctx, p, StageAnalysis, err)
			return
		}
	}

	var gaps []agent.ResearchGap
	gapsCompleted, err := o.isStageCompleted(ctx, p, StageGapDetection, &gaps)
	if err != nil {
		o.failPipeline(ctx, p, StageGapDetection, err)
		return
	}
	if !gapsCompleted {
		o.updateStatus(p, StageGapDetection, 0.65, "")
		if err := o.beginStage(ctx, p, StageGapDetection); err != nil {
			o.failPipeline(ctx, p, StageGapDetection, err)
			return
		}
		gaps, err = o.detect(ctx, p.TopicID, p.Topic)
		if err != nil {
			o.failPipeline(ctx, p, StageGapDetection, err)
			return
		}
		if err := o.finishStage(ctx, p, StageGapDetection, gaps); err != nil {
			o.failPipeline(ctx, p, StageGapDetection, err)
			return
		}
	}

	feasibilityCompleted, err := o.isStageCompleted(ctx, p, StageFeasibility, nil)
	if err != nil {
		o.failPipeline(ctx, p, StageFeasibility, err)
		return
	}
	if !feasibilityCompleted {
		o.updateStatus(p, StageFeasibility, 0.80, "")
		if err := o.beginStage(ctx, p, StageFeasibility); err != nil {
			o.failPipeline(ctx, p, StageFeasibility, err)
			return
		}
		results, evalErr := o.evaluate(ctx, p.TopicID, gaps)
		if evalErr != nil {
			o.failPipeline(ctx, p, StageFeasibility, evalErr)
			return
		}
		if err := o.finishStage(ctx, p, StageFeasibility, map[string]int{"total": len(gaps), "succeeded": len(results), "failed": 0}); err != nil {
			o.failPipeline(ctx, p, StageFeasibility, err)
			return
		}
	}

	reportCompleted, err := o.isStageCompleted(ctx, p, StageReport, nil)
	if err != nil {
		o.failPipeline(ctx, p, StageReport, err)
		return
	}
	if !reportCompleted {
		o.updateStatus(p, StageReport, 0.90, "")
		if err := o.beginStage(ctx, p, StageReport); err != nil {
			o.failPipeline(ctx, p, StageReport, err)
			return
		}
		report, err := o.generateReport(ctx, p.TopicID)
		if err != nil {
			o.failPipeline(ctx, p, StageReport, err)
			return
		}
		o.cacheReport(p.TopicID, report)
		if err := o.finishStage(ctx, p, StageReport, map[string]bool{"generated": true}); err != nil {
			o.failPipeline(ctx, p, StageReport, err)
			return
		}
	}

	o.setStatus(p, StageCompleted, 1.0, "")
	if err := o.persistTerminal(ctx, p); err != nil {
		o.failPipeline(ctx, p, StageReport, fmt.Errorf("persist completed pipeline state: %w", err))
		return
	}
	o.publishStatus(p)

	logger.From(ctx).Info().Str("topic_id", p.TopicID).Msg("Pipeline completed")
	if o.logs != nil {
		if err := o.logs.CloseRun(p.RunID); err != nil {
			logger.Error().Err(err).Str("run_id", p.RunID).Msg("Failed to close run log")
		}
	}
}

func (o *Orchestrator) failPipeline(ctx context.Context, p *Pipeline, stage Stage, err error) {
	if errors.Is(err, context.Canceled) && o.appCtx.Err() != nil {
		logger.From(ctx).Info().Str("stage", string(stage)).Msg("Pipeline suspended for server shutdown")
		o.publishStatus(p)
		return
	}
	o.setStatus(p, StageFailed, 0, err.Error())
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if dbErr := o.persistFailedStage(persistCtx, p, stage, err); dbErr != nil {
		logger.From(ctx).Warn().Err(dbErr).Str("topic_id", p.TopicID).Msg("Failed to persist failed topic state")
	}
	o.publishStatus(p)
	logger.From(ctx).Error().Err(err).Str("stage", string(stage)).Msg("Pipeline failed")
	if o.logs != nil {
		if closeErr := o.logs.CloseRun(p.RunID); closeErr != nil {
			logger.Error().Err(closeErr).Str("run_id", p.RunID).Msg("Failed to close run log")
		}
	}
}

func (o *Orchestrator) persistFailedStage(ctx context.Context, p *Pipeline, stage Stage, stageErr error) error {
	if o.failStageFn != nil {
		return o.failStageFn(ctx, p, stage, stageErr)
	}
	return o.failStage(ctx, p, stage, stageErr)
}

func (o *Orchestrator) isStageCompleted(ctx context.Context, p *Pipeline, stage Stage, output interface{}) (bool, error) {
	if o.stageCompletedFn != nil {
		return o.stageCompletedFn(ctx, p, stage, output)
	}
	return o.stageCompleted(ctx, p, stage, output)
}

func (o *Orchestrator) beginStage(ctx context.Context, p *Pipeline, stage Stage) error {
	if o.startStageFn != nil {
		return o.startStageFn(ctx, p, stage)
	}
	return o.startStage(ctx, p, stage)
}

func (o *Orchestrator) finishStage(ctx context.Context, p *Pipeline, stage Stage, output interface{}) error {
	if o.completeStageFn != nil {
		return o.completeStageFn(ctx, p, stage, output)
	}
	return o.completeStage(ctx, p, stage, output)
}

func (o *Orchestrator) persistTerminal(ctx context.Context, p *Pipeline) error {
	if o.persistTerminalStateFn != nil {
		return o.persistTerminalStateFn(ctx, p)
	}
	return o.persistTerminalState(ctx, p)
}

func (o *Orchestrator) expand(ctx context.Context, topicID, topic string) (*agent.ExpandedQuery, error) {
	if o.expandFn != nil {
		return o.expandFn(ctx, topicID, topic)
	}
	return o.queryExpander.Expand(ctx, topicID, topic)
}

func (o *Orchestrator) discover(ctx context.Context, topicID, topic string, expanded *agent.ExpandedQuery) ([]agent.DiscoveredPaper, error) {
	if o.discoverFn != nil {
		return o.discoverFn(ctx, topicID, topic, expanded)
	}
	return o.discoverWithRetry(ctx, topicID, topic, expanded)
}

func (o *Orchestrator) countPapers(ctx context.Context, topicID string) (int64, error) {
	if o.countPapersFn != nil {
		return o.countPapersFn(ctx, topicID)
	}
	return o.paperDiscoverer.CountPapers(ctx, topicID)
}

func (o *Orchestrator) rank(ctx context.Context, topicID, topic string, maxPapers int) ([]agent.RankedPaper, error) {
	if o.rankFn != nil {
		return o.rankFn(ctx, topicID, topic, maxPapers)
	}
	return o.ranker.Rank(ctx, topicID, topic, maxPapers)
}

func (o *Orchestrator) pendingPapers(ctx context.Context, topicID string, ranked []agent.RankedPaper) ([]agent.RankedPaper, error) {
	if o.pendingRankedPapersFn != nil {
		return o.pendingRankedPapersFn(ctx, topicID, ranked)
	}
	return o.pendingRankedPapers(ctx, topicID, ranked)
}

func (o *Orchestrator) analyze(ctx context.Context, topicID string, papers []agent.RankedPaper) error {
	if o.analyzePapersFn != nil {
		return o.analyzePapersFn(ctx, topicID, papers)
	}
	return o.analyzePapers(ctx, topicID, papers)
}

func (o *Orchestrator) detect(ctx context.Context, topicID, topic string) ([]agent.ResearchGap, error) {
	if o.detectFn != nil {
		return o.detectFn(ctx, topicID, topic)
	}
	return o.gapDetector.Detect(ctx, topicID, topic)
}

func (o *Orchestrator) evaluate(ctx context.Context, topicID string, gaps []agent.ResearchGap) ([]agent.FeasibilityResult, error) {
	if o.evaluateFn != nil {
		return o.evaluateFn(ctx, topicID, gaps)
	}
	return o.feasibility.Evaluate(ctx, topicID, gaps)
}

func (o *Orchestrator) generateReport(ctx context.Context, topicID string) (*agent.Report, error) {
	if o.generateReportFn != nil {
		return o.generateReportFn(ctx, topicID)
	}
	return o.reportGenerator.Generate(ctx, topicID)
}

func (o *Orchestrator) pendingRankedPapers(ctx context.Context, topicID string, ranked []agent.RankedPaper) ([]agent.RankedPaper, error) {
	id, err := uuid.Parse(topicID)
	if err != nil {
		return nil, fmt.Errorf("invalid topic ID %q: %w", topicID, err)
	}
	completed, err := o.postgres.Queries().GetCompletedPaperIDsByTopic(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("load completed paper analyses: %w", err)
	}
	completedIDs := make(map[string]struct{}, len(completed))
	for _, id := range completed {
		completedIDs[id.String()] = struct{}{}
	}
	pending := make([]agent.RankedPaper, 0, len(ranked))
	for _, paper := range ranked {
		if _, ok := completedIDs[paper.ID]; !ok {
			pending = append(pending, paper)
		}
	}
	return pending, nil
}

func (o *Orchestrator) analyzePapers(ctx context.Context, topicID string, papers []agent.RankedPaper) error {
	maxAnalyze := o.config.Pipeline.PapersToAnalyze
	if maxAnalyze > 0 && len(papers) > maxAnalyze {
		logger.From(ctx).Info().
			Int("discovered", len(papers)).
			Int("analyzing", maxAnalyze).
			Msg("Limiting papers to analyze")
		papers = papers[:maxAnalyze]
	}

	analysisPapers := make([]agent.AnalysisPaper, 0, len(papers))
	for _, paper := range papers {
		analysisPapers = append(analysisPapers, agent.AnalysisPaper{
			ID:       paper.ID,
			Title:    paper.Title,
			Abstract: paper.Abstract,
			PDFURL:   paper.PDFURL,
		})
	}

	total := len(analysisPapers)
	if total == 0 {
		return nil
	}

	indexCtx, cancel := context.WithTimeout(ctx, o.config.Pipeline.PDFIndexingTimeout)
	defer cancel()
	if err := o.indexer.Index(indexCtx, topicID, papers); err != nil {
		return fmt.Errorf("index paper documents: %w", err)
	}

	return o.analyzer.Analyze(ctx, topicID, analysisPapers, func(completed, total int) {
		progress := 0.35 + (float64(completed)/float64(total))*0.30
		o.sse.Broadcast(progressEvent{TopicID: topicID, Stage: "paper_analysis", Progress: progress})

		logger.From(ctx).Info().
			Str("topic_id", topicID).
			Int("completed", completed).
			Int("total", total).
			Float64("progress", float64(completed*100)/float64(total)).
			Msg("Paper analysis progress updated")
	})
}

func (o *Orchestrator) discoverWithRetry(ctx context.Context, topicID, topic string, expanded *agent.ExpandedQuery) ([]agent.DiscoveredPaper, error) {
	const maxAttempts = 3
	levels := []agent.QueryLevel{
		agent.QueryLevelFull,
		agent.QueryLevelBroad,
		agent.QueryLevelMinimal,
	}

	var lastErr error
	var allPapers []agent.DiscoveredPaper

	for attempt := 0; attempt < maxAttempts; attempt++ {
		level := levels[attempt]
		queries := expanded.GetQueriesForLevel(level, topic)
		keywords := expanded.GetKeywordsForLevel(level)

		logger.From(ctx).Info().
			Str("topic_id", topicID).
			Int("attempt", attempt+1).
			Int("level", int(level)).
			Int("queries", len(queries)).
			Int("keywords", len(keywords)).
			Msg("Attempting paper discovery")

		if attempt > 0 {
			if err := o.paperDiscoverer.ClearPapers(ctx, topicID); err != nil {
				return nil, fmt.Errorf("clear papers before discovery retry %d: %w", attempt+1, err)
			}
		}

		papers, err := o.paperDiscoverer.Discover(ctx, topicID, queries, keywords)
		if err != nil {
			lastErr = err
			logger.From(ctx).Warn().
				Err(err).
				Int("attempt", attempt+1).
				Msg("Discovery attempt failed")
			continue
		}

		allPapers = papers

		if len(papers) >= o.config.Pipeline.MinPapersForAnalysis {
			logger.From(ctx).Info().
				Int("attempt", attempt+1).
				Int("papers_found", len(papers)).
				Msg("Discovery succeeded")
			return papers, nil
		}

		logger.From(ctx).Warn().
			Int("attempt", attempt+1).
			Int("papers_found", len(papers)).
			Int("min_required", o.config.Pipeline.MinPapersForAnalysis).
			Msg("Not enough papers, retrying with broader queries")
	}

	if len(allPapers) > 0 {
		return allPapers, nil
	}

	if lastErr != nil {
		return nil, fmt.Errorf("all discovery attempts failed: %w", lastErr)
	}

	return nil, fmt.Errorf("not enough papers found after %d attempts", maxAttempts)
}

func (o *Orchestrator) updateStatus(p *Pipeline, stage Stage, progress float64, errMsg string) {
	o.setStatus(p, stage, progress, errMsg)
	o.publishStatus(p)
}

func (o *Orchestrator) setStatus(p *Pipeline, stage Stage, progress float64, errMsg string) {
	p.Stage = stage
	p.Progress = progress
	p.UpdatedAt = time.Now()

	if errMsg != "" {
		p.Error = errMsg
		p.Status = "failed"
	} else if stage == StageCompleted {
		p.Status = "completed"
	} else {
		p.Status = "processing"
	}

	if errMsg == "" {
		p.Error = ""
	}
}

func (o *Orchestrator) publishStatus(p *Pipeline) {
	ctx := o.appCtx
	if o.logs != nil {
		ctx = o.logs.ContextForTopic(ctx, p.TopicID)
	}
	o.publishPipeline(p)
	if o.state != nil {
		// A pipeline is intentionally canceled during shutdown, but Redis still
		// needs one last transient snapshot. Detach only cancellation (retaining
		// logger values) and bound the write so shutdown remains finite.
		stateCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = o.state.Save(stateCtx, p.TopicID, p)
	}

	if o.sse != nil {
		o.sse.Broadcast(statusEvent{
			TopicID:  p.TopicID,
			Status:   p.Status,
			Stage:    string(p.Stage),
			Progress: p.Progress,
			Error:    p.Error,
		})
	}

	logger.From(ctx).Debug().
		Str("topic_id", p.TopicID).
		Str("stage", string(p.Stage)).
		Float64("progress", p.Progress).
		Msg("Pipeline status updated")
}

func (o *Orchestrator) GetPipeline(ctx context.Context, topicID string) (*Pipeline, error) {
	o.mu.RLock()
	p, exists := o.pipelines[topicID]
	if exists {
		p = clonePipeline(p)
	}
	o.mu.RUnlock()

	if exists {
		return p, nil
	}

	id, err := uuid.Parse(topicID)
	if err != nil {
		return nil, fmt.Errorf("%w: %q", ErrInvalidTopicID, topicID)
	}

	loadState := o.loadStateFn
	if loadState == nil && o.state != nil {
		loadState = o.state.Load
	}
	if loadState != nil {
		pipeline, stateErr := loadState(ctx, topicID)
		if stateErr == nil {
			o.publishPipeline(pipeline)
			return clonePipeline(pipeline), nil
		}
		if !errors.Is(stateErr, ErrStateNotFound) {
			logger.Warn().Err(stateErr).Str("topic_id", topicID).Msg("Failed to load Redis pipeline state; falling back to Postgres")
		}
	}

	getTopic := o.getResearchTopicFn
	if getTopic == nil && o.postgres != nil {
		getTopic = o.postgres.Queries().GetResearchTopic
	}
	if getTopic == nil {
		return nil, fmt.Errorf("load durable pipeline %s: postgres is not configured", topicID)
	}
	topic, err := getTopic(ctx, id)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: %s", ErrPipelineNotFound, topicID)
		}
		return nil, fmt.Errorf("load durable pipeline %s: %w", topicID, err)
	}
	pipeline := pipelineFromTopic(topic)
	o.publishPipeline(pipeline)
	return clonePipeline(pipeline), nil
}

func pipelineFromTopic(topic *postgres.ResearchTopic) *Pipeline {
	pipeline := &Pipeline{
		TopicID:   topic.ID.String(),
		RunID:     topic.RunID.String(),
		Topic:     topic.Topic,
		Status:    topic.Status,
		Stage:     Stage(topic.CurrentStage),
		Progress:  topic.Progress,
		StartedAt: topic.CreatedAt.Time,
		UpdatedAt: topic.UpdatedAt.Time,
	}
	if pipeline.Stage == "" {
		pipeline.Stage = StagePending
	}
	if topic.ErrorMessage.Valid {
		pipeline.Error = topic.ErrorMessage.String
	}
	return pipeline
}

func clonePipeline(p *Pipeline) *Pipeline {
	if p == nil {
		return nil
	}
	clone := *p
	return &clone
}

func (o *Orchestrator) publishPipeline(p *Pipeline) {
	o.mu.Lock()
	o.pipelines[p.TopicID] = clonePipeline(p)
	o.mu.Unlock()
}

func (o *Orchestrator) GetReport(ctx context.Context, topicID string) (*agent.Report, error) {
	o.reportMu.Lock()
	defer o.reportMu.Unlock()
	if report, ok := o.reports[topicID]; ok {
		return report, nil
	}

	report, err := o.reportGenerator.Generate(ctx, topicID)
	if err != nil {
		return nil, err
	}
	if o.reports == nil {
		o.reports = make(map[string]*agent.Report)
	}
	o.reports[topicID] = report
	return report, nil
}

func (o *Orchestrator) cacheReport(topicID string, report *agent.Report) {
	if report == nil {
		return
	}
	o.reportMu.Lock()
	if o.reports == nil {
		o.reports = make(map[string]*agent.Report)
	}
	o.reports[topicID] = report
	o.reportMu.Unlock()
}

func (o *Orchestrator) GetSSEManager() *SSEManager {
	return o.sse
}

func (o *Orchestrator) Shutdown() {
	const shutdownTimeout = 30 * time.Second
	deadline := time.Now().Add(shutdownTimeout)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	o.appCancel()
	pipelinesDone := make(chan struct{})
	go func() {
		o.runs.Wait()
		close(pipelinesDone)
	}()
	select {
	case <-pipelinesDone:
	case <-shutdownCtx.Done():
		logger.Warn().Msg("Timed out waiting for pipelines to stop")
	}

	remaining := time.Until(deadline)
	if remaining < 0 {
		remaining = 0
	}
	o.workerPool.StopAndWait(remaining)
	logger.Info().Msg("Orchestrator shutdown complete")
}

func (o *Orchestrator) recoverPipelines(ctx context.Context) {
	recoverable := make(map[string]*Pipeline)
	redisPipelines, err := o.state.ListRecoverable(ctx)
	if err != nil {
		logger.Warn().Err(err).Msg("Failed to scan Redis pipeline state; falling back to Postgres")
	} else {
		for _, pipeline := range redisPipelines {
			recoverable[pipeline.TopicID] = pipeline
		}
	}

	durableTopics, err := o.postgres.Queries().ListRecoverableResearchTopics(ctx)
	if err != nil {
		logger.Warn().Err(err).Msg("Failed to scan durable recoverable topics")
	} else {
		for _, topic := range durableTopics {
			pipeline := &Pipeline{
				TopicID:   topic.ID.String(),
				RunID:     topic.RunID.String(),
				Topic:     topic.Topic,
				Status:    topic.Status,
				Stage:     Stage(topic.CurrentStage),
				Progress:  topic.Progress,
				StartedAt: topic.CreatedAt.Time,
				UpdatedAt: topic.UpdatedAt.Time,
			}
			if topic.ErrorMessage.Valid {
				pipeline.Error = topic.ErrorMessage.String
			}
			if pipeline.Stage == "" {
				pipeline.Stage = StagePending
			}
			checkpoints, checkpointErr := o.postgres.Queries().GetPipelineStages(ctx, topic.RunID)
			if checkpointErr != nil {
				logger.Warn().Err(checkpointErr).Str("topic_id", pipeline.TopicID).Msg("Failed to load durable pipeline checkpoints")
			} else if len(checkpoints) > 0 {
				latest := checkpoints[len(checkpoints)-1]
				pipeline.Stage = Stage(latest.Stage)
				if latest.UpdatedAt.Valid {
					pipeline.UpdatedAt = latest.UpdatedAt.Time
				}
			}
			recoverable[pipeline.TopicID] = pipeline
		}
	}

	for _, pipeline := range recoverable {
		if pipeline.RunID == "" {
			topicID, parseErr := uuid.Parse(pipeline.TopicID)
			if parseErr != nil {
				logger.Warn().Err(parseErr).Str("topic_id", pipeline.TopicID).Msg("Invalid topic ID during recovery")
				continue
			}
			topic, err := o.postgres.Queries().GetResearchTopic(ctx, topicID)
			if err != nil {
				logger.Warn().Err(err).Str("topic_id", pipeline.TopicID).Msg("Failed to resolve run ID during recovery")
				continue
			}
			pipeline.RunID = topic.RunID.String()
		}
		o.mu.Lock()
		if _, exists := o.pipelines[pipeline.TopicID]; exists {
			o.mu.Unlock()
			continue
		}
		o.pipelines[pipeline.TopicID] = clonePipeline(pipeline)
		o.mu.Unlock()
		if err := o.logs.StartRun(pipeline.RunID, pipeline.TopicID); err != nil {
			logger.Error().Err(err).Str("topic_id", pipeline.TopicID).Msg("Failed to reopen run log; recovery skipped")
			pipeline.Status, pipeline.Stage, pipeline.Error = "failed", StageFailed, err.Error()
			topicID, parseErr := uuid.Parse(pipeline.TopicID)
			if parseErr != nil {
				logger.Error().Err(parseErr).Str("topic_id", pipeline.TopicID).Msg("Cannot persist failed recovery state")
				continue
			}
			if _, updateErr := o.postgres.Queries().UpdateResearchTopicState(ctx, postgres.UpdateResearchTopicStateParams{
				ID: topicID, Status: "failed", CurrentStage: string(StageFailed), Progress: pipeline.Progress,
				ErrorMessage: pgtype.Text{String: pipeline.Error, Valid: true},
			}); updateErr != nil {
				logger.Error().Err(updateErr).Str("topic_id", pipeline.TopicID).Msg("Failed to persist recovery log failure")
			}
			o.publishPipeline(pipeline)
			if saveErr := o.state.Save(ctx, pipeline.TopicID, pipeline); saveErr != nil {
				logger.Error().Err(saveErr).Str("topic_id", pipeline.TopicID).Msg("Failed to publish recovery log failure")
			}
			continue
		}

		logger.Info().
			Str("topic_id", pipeline.TopicID).
			Str("stage", string(pipeline.Stage)).
			Str("status", pipeline.Status).
			Msg("Recovering persisted pipeline")

		o.launchPipeline(pipeline)
	}
}

type statusEvent struct {
	TopicID  string  `json:"topic_id"`
	Status   string  `json:"status"`
	Stage    string  `json:"stage"`
	Progress float64 `json:"progress"`
	Error    string  `json:"error,omitempty"`
}

type progressEvent struct {
	TopicID  string  `json:"topic_id"`
	Stage    string  `json:"stage"`
	Progress float64 `json:"progress"`
}
