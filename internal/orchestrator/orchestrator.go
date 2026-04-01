package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/paper-scout/internal/agent"
	"github.com/paper-scout/internal/config"
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
	config   *config.Config
	postgres *postgres.Client
	redis    *redis.Client
	qdrant   *qdrant.Client

	queryExpander   *agent.QueryExpander
	paperDiscoverer *agent.PaperDiscoverer
	ranker          *agent.Ranker
	analyzer        *agent.Analyzer
	gapDetector     *agent.GapDetector
	feasibility     *agent.FeasibilityEvaluator
	reportGenerator *agent.ReportGenerator

	workerPool *worker.Pool

	sse   *SSEManager
	state *StateManager

	mu        sync.RWMutex
	pipelines map[string]*Pipeline
}

type Pipeline struct {
	TopicID   string
	Topic     string
	Status    string
	Stage     Stage
	Progress  float64
	StartedAt time.Time
	UpdatedAt time.Time
	Error     string
}

type Stage string

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
	cfg *config.Config,
	pg *postgres.Client,
	redisClient *redis.Client,
	qdrantClient *qdrant.Client,
	llmClient *llm.Client,
	ssClient *semantic_scholar.Client,
	arxivClient *arxiv.Client,
) *Orchestrator {
	downloader := pdf.NewDownloader(cfg.Pipeline.PDFDownloadTimeout)
	parser := pdf.NewGrobidClient(cfg.APIs.Grobid.BaseURL, cfg.APIs.Grobid.Timeout)

	embedder := embedding.NewGenerator(llmClient, qdrantClient)

	var pool *worker.Pool
	if cfg.Pipeline.UseRedisQueue {
		redisQueue := redis.NewQueue(redisClient.Client())
		pool = worker.NewRedisPool(cfg.Pipeline.WorkerPoolSize, redisQueue)
		logger.Info().Msg("Using Redis queue for worker pool")
	} else {
		pool = worker.NewPool(cfg.Pipeline.WorkerPoolSize, 100)
		logger.Info().Msg("Using local queue for worker pool")
	}

	processor := worker.NewProcessor(pg, downloader, parser, embedder)
	pool.SetHandler(processor.CreateHandler())
	pool.Start()

	o := &Orchestrator{
		config:     cfg,
		postgres:   pg,
		redis:      redisClient,
		qdrant:     qdrantClient,
		workerPool: pool,
		sse:        NewSSEManager(),
		state:      NewStateManager(redisClient),
		pipelines:  make(map[string]*Pipeline),
	}

	o.queryExpander = agent.NewQueryExpander(llmClient, pg)
	o.paperDiscoverer = agent.NewPaperDiscoverer(ssClient, arxivClient, pg, cfg.Pipeline.MaxPapers)
	o.ranker = agent.NewRanker(pg, embedder, llmClient)
	o.analyzer = agent.NewAnalyzer(llmClient, pg, downloader, parser, pool)
	o.gapDetector = agent.NewGapDetector(llmClient, pg)
	o.feasibility = agent.NewFeasibilityEvaluator(llmClient, pg)
	o.reportGenerator = agent.NewReportGenerator(pg)

	return o
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
		Topic:     topic,
		Status:    "pending",
		Stage:     StagePending,
		Progress:  0,
		StartedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	o.mu.Lock()
	o.pipelines[pipeline.TopicID] = pipeline
	o.mu.Unlock()

	o.state.Save(ctx, pipeline.TopicID, pipeline)

	go o.runPipeline(pipeline)

	return pipeline, nil
}

func (o *Orchestrator) runPipeline(p *Pipeline) {
	ctx := context.Background()

	defer func() {
		if r := recover(); r != nil {
			logger.Error().Interface("panic", r).Str("topic_id", p.TopicID).Msg("Pipeline panicked")
			o.updateStatus(p, StageFailed, 0, fmt.Sprintf("Pipeline panic: %v", r))
		}
	}()

	o.updateStatus(p, StageQueryExpand, 0.05, "")

	expanded, err := o.queryExpander.Expand(ctx, p.TopicID, p.Topic)
	if err != nil {
		o.updateStatus(p, StageFailed, 0, err.Error())
		return
	}

	o.updateStatus(p, StageDiscovery, 0.15, "")
	papers, err := o.discoverWithRetry(ctx, p.TopicID, p.Topic, expanded)
	if err != nil {
		o.updateStatus(p, StageFailed, 0, err.Error())
		return
	}

	if len(papers) < o.config.Pipeline.MinPapersForAnalysis {
		o.updateStatus(p, StageFailed, 0, fmt.Sprintf("Not enough papers found: %d (minimum: %d)", len(papers), o.config.Pipeline.MinPapersForAnalysis))
		return
	}

	o.updateStatus(p, StageRanking, 0.25, "")
	ranked, err := o.ranker.Rank(ctx, p.TopicID, p.Topic, o.config.Pipeline.MaxPapers)
	if err != nil {
		o.updateStatus(p, StageFailed, 0, err.Error())
		return
	}

	o.updateStatus(p, StageAnalysis, 0.35, "")

	if err := o.analyzePapersSync(ctx, p.TopicID, ranked); err != nil {
		logger.Warn().Err(err).Msg("Paper analysis had errors, continuing...")
	}

	o.updateStatus(p, StageGapDetection, 0.65, "")
	gaps, err := o.gapDetector.Detect(ctx, p.TopicID, p.Topic)
	if err != nil {
		o.updateStatus(p, StageFailed, 0, err.Error())
		return
	}

	o.updateStatus(p, StageFeasibility, 0.80, "")
	_, err = o.feasibility.Evaluate(ctx, p.TopicID, gaps)
	if err != nil {
		logger.Warn().Err(err).Msg("Feasibility evaluation had errors, continuing...")
	}

	o.updateStatus(p, StageReport, 0.90, "")
	report, err := o.reportGenerator.Generate(ctx, p.TopicID)
	if err != nil {
		o.updateStatus(p, StageFailed, 0, err.Error())
		return
	}

	_ = report

	o.updateStatus(p, StageCompleted, 1.0, "")

	_, err = o.postgres.Queries().UpdateResearchTopicStatus(ctx, postgres.UpdateResearchTopicStatusParams{
		ID:     parseUUID(p.TopicID),
		Status: "completed",
	})
	if err != nil {
		logger.Warn().Err(err).Msg("Failed to update topic status")
	}

	logger.Info().Str("topic_id", p.TopicID).Msg("Pipeline completed")
}

func (o *Orchestrator) analyzePapersSync(ctx context.Context, topicID string, papers []agent.RankedPaper) error {
	total := len(papers)

	maxAnalyze := o.config.Pipeline.PapersToAnalyze
	if maxAnalyze > 0 && len(papers) > maxAnalyze {
		logger.Info().
			Int("discovered", len(papers)).
			Int("analyzing", maxAnalyze).
			Msg("Limiting papers to analyze")
		papers = papers[:maxAnalyze]
		total = maxAnalyze
	}

	for i, paper := range papers {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if i > 0 && o.config.Pipeline.AnalysisDelay > 0 {
			time.Sleep(o.config.Pipeline.AnalysisDelay)
		}

		progress := 0.35 + (float64(i)/float64(total))*0.30
		o.sse.Broadcast(progressEvent{TopicID: topicID, Stage: "paper_analysis", Progress: progress})

		analysis, err := o.analyzer.AnalyzeSync(ctx, paper.ID, paper.Abstract, "")
		if err != nil {
			if isDailyLimitExceeded(err) {
				logger.Warn().
					Err(err).
					Int("completed", i).
					Int("total", total).
					Msg("Daily LLM limit exceeded, skipping remaining papers")
				break
			}

			logger.Warn().
				Err(err).
				Str("paper_id", paper.ID).
				Str("title", paper.Title).
				Int("completed", i).
				Int("total", total).
				Msg("Failed to analyze paper")
			continue
		}

		if err := o.analyzer.StoreAnalysis(ctx, paper.ID, analysis); err != nil {
			logger.Warn().Err(err).Str("paper_id", paper.ID).Msg("Failed to store analysis")
		}

		logger.Info().
			Str("paper_id", paper.ID).
			Int("completed", i+1).
			Int("total", total).
			Float64("progress", float64(i+1)/float64(total)*100).
			Msg("Paper analysis completed")
	}
	return nil
}

func isDailyLimitExceeded(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return contains(errStr, "daily request limit exceeded") ||
		contains(errStr, "RESOURCE_EXHAUSTED") && contains(errStr, "per day")
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
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

		logger.Info().
			Str("topic_id", topicID).
			Int("attempt", attempt+1).
			Int("level", int(level)).
			Int("queries", len(queries)).
			Int("keywords", len(keywords)).
			Msg("Attempting paper discovery")

		if attempt > 0 {
			if err := o.paperDiscoverer.ClearPapers(ctx, topicID); err != nil {
				logger.Warn().Err(err).Msg("Failed to clear papers before retry")
			}
		}

		papers, err := o.paperDiscoverer.Discover(ctx, topicID, queries, keywords)
		if err != nil {
			lastErr = err
			logger.Warn().
				Err(err).
				Int("attempt", attempt+1).
				Msg("Discovery attempt failed")
			continue
		}

		allPapers = papers

		if len(papers) >= o.config.Pipeline.MinPapersForAnalysis {
			logger.Info().
				Int("attempt", attempt+1).
				Int("papers_found", len(papers)).
				Msg("Discovery succeeded")
			return papers, nil
		}

		logger.Warn().
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

	o.state.Save(context.Background(), p.TopicID, p)

	o.sse.Broadcast(statusEvent{
		TopicID:  p.TopicID,
		Status:   p.Status,
		Stage:    string(stage),
		Progress: progress,
		Error:    errMsg,
	})

	logger.Debug().
		Str("topic_id", p.TopicID).
		Str("stage", string(stage)).
		Float64("progress", progress).
		Msg("Pipeline status updated")
}

func (o *Orchestrator) GetPipeline(topicID string) (*Pipeline, error) {
	o.mu.RLock()
	p, exists := o.pipelines[topicID]
	o.mu.RUnlock()

	if exists {
		return p, nil
	}

	return o.state.Load(context.Background(), topicID)
}

func (o *Orchestrator) GetReport(ctx context.Context, topicID string) (*agent.Report, error) {
	return o.reportGenerator.Generate(ctx, topicID)
}

func (o *Orchestrator) GetSSEManager() *SSEManager {
	return o.sse
}

func (o *Orchestrator) Shutdown() {
	o.workerPool.Stop()
	logger.Info().Msg("Orchestrator shutdown complete")
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

func parseUUID(s string) uuid.UUID {
	id, _ := uuid.Parse(s)
	return id
}

func pgDate(t time.Time) pgtype.Date {
	return pgtype.Date{
		Time:  t,
		Valid: true,
	}
}
