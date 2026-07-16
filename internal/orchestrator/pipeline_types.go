package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/paper-scout/internal/agent"
)

var (
	// ErrPipelineNotFound reports an unknown research topic.
	ErrPipelineNotFound = errors.New("pipeline not found")
	// ErrInvalidTopicID reports a topic identifier that is not a UUID.
	ErrInvalidTopicID = errors.New("invalid topic ID")
)

// Pipeline is the externally visible state of a research run.
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

// Stage identifies a durable pipeline checkpoint.
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

// ReportService owns report generation and the process-local completed-report cache.
type ReportService struct {
	generator *agent.ReportGenerator
	mu        sync.Mutex
	cache     map[string]*agent.Report
}

// NewReportService constructs report generation with a process-local completed-report cache.
func NewReportService(generator *agent.ReportGenerator) *ReportService {
	return &ReportService{generator: generator, cache: make(map[string]*agent.Report)}
}

// Get returns a cached report or generates and caches it.
func (s *ReportService) Get(ctx context.Context, topicID string) (*agent.Report, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if report, ok := s.cache[topicID]; ok {
		return report, nil
	}
	if s.generator == nil {
		return nil, fmt.Errorf("report generator is not configured")
	}
	report, err := s.generator.Generate(ctx, topicID)
	if err != nil {
		return nil, err
	}
	s.cache[topicID] = report
	return report, nil
}

// Cache records a completed report for subsequent reads.
func (s *ReportService) Cache(topicID string, report *agent.Report) {
	if report == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cache[topicID] = report
}

func clonePipeline(p *Pipeline) *Pipeline {
	if p == nil {
		return nil
	}
	clone := *p
	return &clone
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
