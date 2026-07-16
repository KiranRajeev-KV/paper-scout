package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/paper-scout/internal/agent"
	"github.com/paper-scout/internal/config"
	"github.com/paper-scout/internal/logger"
	"github.com/paper-scout/internal/storage/postgres"
)

type checkpointStore interface {
	stageCompleted(context.Context, *Pipeline, Stage, interface{}) (bool, error)
	startStage(context.Context, *Pipeline, Stage) error
	completeStage(context.Context, *Pipeline, Stage, interface{}) error
	failStage(context.Context, *Pipeline, Stage, error) error
	persistTerminalState(context.Context, *Pipeline) error
}

// PipelineStages supplies the individual operations used by the ordered pipeline runner.
type PipelineStages interface {
	Expand(context.Context, string, string) (*agent.ExpandedQuery, error)
	Discover(context.Context, string, string, *agent.ExpandedQuery) ([]agent.DiscoveredPaper, error)
	CountPapers(context.Context, string) (int64, error)
	Rank(context.Context, string, string, int) ([]agent.RankedPaper, error)
	PendingPapers(context.Context, string, []agent.RankedPaper) ([]agent.RankedPaper, error)
	Analyze(context.Context, string, []agent.RankedPaper, func(int, int)) error
	Detect(context.Context, string, string) ([]agent.ResearchGap, error)
	Evaluate(context.Context, string, []agent.ResearchGap) ([]agent.FeasibilityResult, error)
	GenerateReport(context.Context, string) (*agent.Report, error)
}

// PipelineRunner owns ordered stage execution, durable checkpoints, and terminal state transitions.
type PipelineRunner struct {
	config      *config.Config
	checkpoints checkpointStore
	stages      PipelineStages
	state       *PipelineStateService
	reports     *ReportService
	logs        *logger.Manager
	appCtx      context.Context
}

// NewPipelineRunner constructs a pipeline runner with its stage and persistence dependencies.
func NewPipelineRunner(appCtx context.Context, cfg *config.Config, checkpoints checkpointStore, stages PipelineStages, state *PipelineStateService, reports *ReportService, logs *logger.Manager) (*PipelineRunner, error) {
	if appCtx == nil || cfg == nil || checkpoints == nil || stages == nil || state == nil || reports == nil || logs == nil {
		return nil, fmt.Errorf("pipeline runner requires application context, configuration, checkpoints, stages, state, reports, and logs")
	}
	return &PipelineRunner{appCtx: appCtx, config: cfg, checkpoints: checkpoints, stages: stages, state: state, reports: reports, logs: logs}, nil
}

// Run executes the first incomplete stage through a durable terminal result.
func (r *PipelineRunner) Run(ctx context.Context, pipeline *Pipeline) {
	defer func() {
		if recovered := recover(); recovered != nil {
			r.fail(ctx, pipeline, pipeline.Stage, fmt.Errorf("pipeline panic: %v", recovered))
		}
	}()
	expanded, ok := r.runQueryExpansion(ctx, pipeline)
	if !ok || !r.runDiscovery(ctx, pipeline, expanded) || !r.runRanking(ctx, pipeline) ||
		!r.runAnalysis(ctx, pipeline) || !r.runGapDetection(ctx, pipeline) || !r.runFeasibility(ctx, pipeline) || !r.runReport(ctx, pipeline) {
		return
	}
	r.setStatus(pipeline, StageCompleted, 1, "")
	if err := r.checkpoints.persistTerminalState(ctx, pipeline); err != nil {
		r.fail(ctx, pipeline, StageReport, fmt.Errorf("persist completed pipeline state: %w", err))
		return
	}
	r.state.Publish(ctx, pipeline)
	if err := r.logs.CloseRun(pipeline.RunID); err != nil {
		logger.From(ctx).Error().Err(err).Str("run_id", pipeline.RunID).Msg("Failed to close run log")
	}
}

func (r *PipelineRunner) runQueryExpansion(ctx context.Context, p *Pipeline) (*agent.ExpandedQuery, bool) {
	var expanded *agent.ExpandedQuery
	completed, err := r.checkpoints.stageCompleted(ctx, p, StageQueryExpand, &expanded)
	if err != nil {
		r.fail(ctx, p, StageQueryExpand, err)
		return nil, false
	}
	if completed {
		return expanded, true
	}
	if !r.begin(ctx, p, StageQueryExpand, .05) {
		return nil, false
	}
	expanded, err = r.stages.Expand(ctx, p.TopicID, p.Topic)
	if err != nil {
		r.fail(ctx, p, StageQueryExpand, err)
		return nil, false
	}
	if err := r.checkpoints.completeStage(ctx, p, StageQueryExpand, expanded); err != nil {
		r.fail(ctx, p, StageQueryExpand, err)
		return nil, false
	}
	return expanded, true
}

func (r *PipelineRunner) runDiscovery(ctx context.Context, p *Pipeline, expanded *agent.ExpandedQuery) bool {
	completed, err := r.checkpoints.stageCompleted(ctx, p, StageDiscovery, nil)
	if err != nil {
		r.fail(ctx, p, StageDiscovery, err)
		return false
	}
	if completed {
		count, countErr := r.stages.CountPapers(ctx, p.TopicID)
		if countErr != nil || count < int64(r.config.Pipeline.MinPapersForAnalysis) {
			if countErr != nil {
				err = countErr
			} else {
				err = fmt.Errorf("recovered discovery has insufficient papers: %d", count)
			}
			r.fail(ctx, p, StageDiscovery, err)
			return false
		}
		return true
	}
	if !r.begin(ctx, p, StageDiscovery, .15) {
		return false
	}
	papers, err := r.stages.Discover(ctx, p.TopicID, p.Topic, expanded)
	if err != nil {
		r.fail(ctx, p, StageDiscovery, err)
		return false
	}
	if len(papers) < r.config.Pipeline.MinPapersForAnalysis {
		r.fail(ctx, p, StageDiscovery, fmt.Errorf("not enough papers found: %d (minimum: %d)", len(papers), r.config.Pipeline.MinPapersForAnalysis))
		return false
	}
	if err := r.checkpoints.completeStage(ctx, p, StageDiscovery, map[string]int{"total": len(papers), "succeeded": len(papers), "failed": 0}); err != nil {
		r.fail(ctx, p, StageDiscovery, err)
		return false
	}
	return true
}

func (r *PipelineRunner) runRanking(ctx context.Context, p *Pipeline) bool {
	var ranked []agent.RankedPaper
	completed, err := r.checkpoints.stageCompleted(ctx, p, StageRanking, &ranked)
	if err != nil {
		r.fail(ctx, p, StageRanking, err)
		return false
	}
	if completed {
		return true
	}
	if !r.begin(ctx, p, StageRanking, .25) {
		return false
	}
	ranked, err = r.stages.Rank(ctx, p.TopicID, p.Topic, r.config.Pipeline.MaxPapers)
	if err != nil {
		r.fail(ctx, p, StageRanking, err)
		return false
	}
	if err := r.checkpoints.completeStage(ctx, p, StageRanking, ranked); err != nil {
		r.fail(ctx, p, StageRanking, err)
		return false
	}
	return true
}

func (r *PipelineRunner) runAnalysis(ctx context.Context, p *Pipeline) bool {
	completed, err := r.checkpoints.stageCompleted(ctx, p, StageAnalysis, nil)
	if err != nil {
		r.fail(ctx, p, StageAnalysis, err)
		return false
	}
	if completed {
		return true
	}
	var ranked []agent.RankedPaper
	if _, err := r.checkpoints.stageCompleted(ctx, p, StageRanking, &ranked); err != nil {
		r.fail(ctx, p, StageAnalysis, err)
		return false
	}
	if !r.begin(ctx, p, StageAnalysis, .35) {
		return false
	}
	pending, err := r.stages.PendingPapers(ctx, p.TopicID, ranked)
	if err != nil {
		r.fail(ctx, p, StageAnalysis, err)
		return false
	}
	if err := r.stages.Analyze(ctx, p.TopicID, pending, func(completed, total int) {
		if total > 0 {
			r.state.PublishProgress(p.TopicID, .35+(float64(completed)/float64(total))*.30)
		}
	}); err != nil {
		r.fail(ctx, p, StageAnalysis, err)
		return false
	}
	if err := r.checkpoints.completeStage(ctx, p, StageAnalysis, map[string]int{"total": len(pending), "succeeded": len(pending), "failed": 0}); err != nil {
		r.fail(ctx, p, StageAnalysis, err)
		return false
	}
	return true
}

func (r *PipelineRunner) runGapDetection(ctx context.Context, p *Pipeline) bool {
	var gaps []agent.ResearchGap
	completed, err := r.checkpoints.stageCompleted(ctx, p, StageGapDetection, &gaps)
	if err != nil {
		r.fail(ctx, p, StageGapDetection, err)
		return false
	}
	if completed {
		return true
	}
	if !r.begin(ctx, p, StageGapDetection, .65) {
		return false
	}
	gaps, err = r.stages.Detect(ctx, p.TopicID, p.Topic)
	if err != nil {
		r.fail(ctx, p, StageGapDetection, err)
		return false
	}
	if err := r.checkpoints.completeStage(ctx, p, StageGapDetection, gaps); err != nil {
		r.fail(ctx, p, StageGapDetection, err)
		return false
	}
	return true
}

func (r *PipelineRunner) runFeasibility(ctx context.Context, p *Pipeline) bool {
	completed, err := r.checkpoints.stageCompleted(ctx, p, StageFeasibility, nil)
	if err != nil {
		r.fail(ctx, p, StageFeasibility, err)
		return false
	}
	if completed {
		return true
	}
	var gaps []agent.ResearchGap
	if _, err := r.checkpoints.stageCompleted(ctx, p, StageGapDetection, &gaps); err != nil {
		r.fail(ctx, p, StageFeasibility, err)
		return false
	}
	if !r.begin(ctx, p, StageFeasibility, .80) {
		return false
	}
	results, err := r.stages.Evaluate(ctx, p.TopicID, gaps)
	if err != nil {
		r.fail(ctx, p, StageFeasibility, err)
		return false
	}
	if err := r.checkpoints.completeStage(ctx, p, StageFeasibility, map[string]int{"total": len(gaps), "succeeded": len(results), "failed": 0}); err != nil {
		r.fail(ctx, p, StageFeasibility, err)
		return false
	}
	return true
}

func (r *PipelineRunner) runReport(ctx context.Context, p *Pipeline) bool {
	completed, err := r.checkpoints.stageCompleted(ctx, p, StageReport, nil)
	if err != nil {
		r.fail(ctx, p, StageReport, err)
		return false
	}
	if completed {
		return true
	}
	if !r.begin(ctx, p, StageReport, .90) {
		return false
	}
	report, err := r.stages.GenerateReport(ctx, p.TopicID)
	if err != nil {
		r.fail(ctx, p, StageReport, err)
		return false
	}
	r.reports.Cache(p.TopicID, report)
	if err := r.checkpoints.completeStage(ctx, p, StageReport, map[string]bool{"generated": true}); err != nil {
		r.fail(ctx, p, StageReport, err)
		return false
	}
	return true
}

func (r *PipelineRunner) begin(ctx context.Context, p *Pipeline, stage Stage, progress float64) bool {
	r.setStatus(p, stage, progress, "")
	r.state.Publish(ctx, p)
	if err := r.checkpoints.startStage(ctx, p, stage); err != nil {
		r.fail(ctx, p, stage, err)
		return false
	}
	return true
}

func (r *PipelineRunner) fail(ctx context.Context, p *Pipeline, stage Stage, cause error) {
	if errors.Is(cause, context.Canceled) && r.appCtx.Err() != nil {
		r.state.Publish(ctx, p)
		return
	}
	r.setStatus(p, StageFailed, 0, cause.Error())
	persistCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := r.checkpoints.failStage(persistCtx, p, stage, cause); err != nil {
		logger.From(ctx).Warn().Err(err).Str("topic_id", p.TopicID).Msg("Failed to persist failed topic state")
	}
	r.state.Publish(ctx, p)
	logger.From(ctx).Error().Err(cause).Str("stage", string(stage)).Msg("Pipeline failed")
	if err := r.logs.CloseRun(p.RunID); err != nil {
		logger.From(ctx).Error().Err(err).Str("run_id", p.RunID).Msg("Failed to close run log")
	}
}

func (r *PipelineRunner) setStatus(p *Pipeline, stage Stage, progress float64, message string) {
	p.Stage, p.Progress, p.UpdatedAt = stage, progress, time.Now()
	p.Error = message
	if message != "" {
		p.Status = "failed"
	} else if stage == StageCompleted {
		p.Status = "completed"
	} else {
		p.Status = "processing"
	}
}

type agentPipelineStages struct {
	config      *config.Config
	postgres    *postgres.Client
	expander    *agent.QueryExpander
	discoverer  *agent.PaperDiscoverer
	ranker      *agent.Ranker
	analyzer    *agent.Analyzer
	indexer     *agent.Indexer
	gaps        *agent.GapDetector
	feasibility *agent.FeasibilityEvaluator
	reports     *agent.ReportGenerator
}

// NewAgentPipelineStages creates production pipeline stages from focused agent services.
func NewAgentPipelineStages(cfg *config.Config, pg *postgres.Client, expander *agent.QueryExpander, discoverer *agent.PaperDiscoverer, ranker *agent.Ranker, analyzer *agent.Analyzer, indexer *agent.Indexer, gaps *agent.GapDetector, feasibility *agent.FeasibilityEvaluator, reports *agent.ReportGenerator) (PipelineStages, error) {
	if cfg == nil || pg == nil || expander == nil || discoverer == nil || ranker == nil || analyzer == nil || indexer == nil || gaps == nil || feasibility == nil || reports == nil {
		return nil, fmt.Errorf("pipeline stages require configuration, postgres, and all agents")
	}
	return &agentPipelineStages{config: cfg, postgres: pg, expander: expander, discoverer: discoverer, ranker: ranker, analyzer: analyzer, indexer: indexer, gaps: gaps, feasibility: feasibility, reports: reports}, nil
}

func (s *agentPipelineStages) Expand(ctx context.Context, topicID, topic string) (*agent.ExpandedQuery, error) {
	return s.expander.Expand(ctx, topicID, topic)
}
func (s *agentPipelineStages) CountPapers(ctx context.Context, topicID string) (int64, error) {
	return s.discoverer.CountPapers(ctx, topicID)
}
func (s *agentPipelineStages) Rank(ctx context.Context, topicID, topic string, max int) ([]agent.RankedPaper, error) {
	return s.ranker.Rank(ctx, topicID, topic, max)
}
func (s *agentPipelineStages) Detect(ctx context.Context, topicID, topic string) ([]agent.ResearchGap, error) {
	return s.gaps.Detect(ctx, topicID, topic)
}
func (s *agentPipelineStages) Evaluate(ctx context.Context, topicID string, gaps []agent.ResearchGap) ([]agent.FeasibilityResult, error) {
	return s.feasibility.Evaluate(ctx, topicID, gaps)
}
func (s *agentPipelineStages) GenerateReport(ctx context.Context, topicID string) (*agent.Report, error) {
	return s.reports.Generate(ctx, topicID)
}

func (s *agentPipelineStages) Discover(ctx context.Context, topicID, topic string, expanded *agent.ExpandedQuery) ([]agent.DiscoveredPaper, error) {
	const attempts = 3
	levels := []agent.QueryLevel{agent.QueryLevelFull, agent.QueryLevelBroad, agent.QueryLevelMinimal}
	if expanded == nil {
		return nil, fmt.Errorf("discovery requires expanded query")
	}
	var lastErr error
	var papers []agent.DiscoveredPaper
	var err error
	for attempt, level := range levels {
		if attempt > 0 {
			if err := s.discoverer.ClearPapers(ctx, topicID); err != nil {
				return nil, fmt.Errorf("clear papers before discovery retry %d: %w", attempt+1, err)
			}
		}
		papers, err = s.discoverer.Discover(ctx, topicID, expanded.GetQueriesForLevel(level, topic), expanded.GetKeywordsForLevel(level))
		if err == nil && len(papers) >= s.config.Pipeline.MinPapersForAnalysis {
			return papers, nil
		}
		if err != nil {
			lastErr = err
		}
	}
	if len(papers) > 0 {
		return papers, nil
	}
	if lastErr != nil {
		return nil, fmt.Errorf("all discovery attempts failed: %w", lastErr)
	}
	return nil, fmt.Errorf("not enough papers found after %d attempts", attempts)
}

func (s *agentPipelineStages) PendingPapers(ctx context.Context, topicID string, ranked []agent.RankedPaper) ([]agent.RankedPaper, error) {
	id, err := uuid.Parse(topicID)
	if err != nil {
		return nil, fmt.Errorf("invalid topic ID %q: %w", topicID, err)
	}
	completed, err := s.postgres.Queries().GetCompletedPaperIDsByTopic(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("load completed paper analyses: %w", err)
	}
	done := make(map[string]struct{}, len(completed))
	for _, id := range completed {
		done[id.String()] = struct{}{}
	}
	pending := make([]agent.RankedPaper, 0, len(ranked))
	for _, paper := range ranked {
		if _, ok := done[paper.ID]; !ok {
			pending = append(pending, paper)
		}
	}
	return pending, nil
}

func (s *agentPipelineStages) Analyze(ctx context.Context, topicID string, papers []agent.RankedPaper, progress func(int, int)) error {
	if max := s.config.Pipeline.PapersToAnalyze; max > 0 && len(papers) > max {
		papers = papers[:max]
	}
	analysis := make([]agent.AnalysisPaper, 0, len(papers))
	for _, paper := range papers {
		analysis = append(analysis, agent.AnalysisPaper{ID: paper.ID, Title: paper.Title, Abstract: paper.Abstract, PDFURL: paper.PDFURL})
	}
	if len(analysis) == 0 {
		return nil
	}
	indexCtx, cancel := context.WithTimeout(ctx, s.config.Pipeline.PDFIndexingTimeout)
	defer cancel()
	if err := s.indexer.Index(indexCtx, topicID, papers); err != nil {
		return fmt.Errorf("index paper documents: %w", err)
	}
	return s.analyzer.Analyze(ctx, topicID, analysis, progress)
}
