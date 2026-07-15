package handler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/paper-scout/internal/agent"
	"github.com/paper-scout/internal/logger"
	"github.com/paper-scout/internal/orchestrator"
)

type Handler struct {
	orch      researchService
	heartbeat time.Duration
}

type researchService interface {
	StartResearch(ctx context.Context, topic string) (*orchestrator.Pipeline, error)
	GetPipeline(ctx context.Context, topicID string) (*orchestrator.Pipeline, error)
	GetReport(ctx context.Context, topicID string) (*agent.Report, error)
	GetSSEManager() *orchestrator.SSEManager
}

func New(orch researchService) *Handler {
	return &Handler{orch: orch, heartbeat: 30 * time.Second}
}

type CreateResearchRequest struct {
	Topic string `json:"topic" binding:"required,min=10,max=500"`
}

type CreateResearchResponse struct {
	TopicID string `json:"topic_id"`
	RunID   string `json:"run_id"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

func (h *Handler) CreateResearch(c *gin.Context) {
	var req CreateResearchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid research request"})
		return
	}

	pipeline, err := h.orch.StartResearch(c.Request.Context(), req.Topic)
	if err != nil {
		logger.From(c.Request.Context()).Error().Err(err).Msg("Failed to start research")
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Research could not be started"})
		return
	}

	c.JSON(http.StatusAccepted, CreateResearchResponse{
		TopicID: pipeline.TopicID,
		RunID:   pipeline.RunID,
		Status:  pipeline.Status,
		Message: "Research started. Use the topic_id to track progress.",
	})
}

type ResearchResponse struct {
	TopicID          string              `json:"topic_id"`
	RunID            string              `json:"run_id"`
	Topic            string              `json:"topic"`
	Status           string              `json:"status"`
	Stage            string              `json:"stage"`
	Progress         float64             `json:"progress"`
	StartedAt        string              `json:"started_at"`
	Error            string              `json:"error,omitempty"`
	ExecutiveSummary string              `json:"executive_summary,omitempty"`
	LiteratureReview string              `json:"literature_review,omitempty"`
	GeneratedAt      string              `json:"generated_at,omitempty"`
	Papers           []PaperResponse     `json:"papers"`
	ResearchGaps     []GapResponse       `json:"research_gaps"`
	NovelDirections  []DirectionResponse `json:"novel_directions"`
	BibTeX           string              `json:"bibtex,omitempty"`
}

type PaperResponse struct {
	ID               string   `json:"id"`
	Title            string   `json:"title"`
	Authors          []string `json:"authors"`
	Year             int      `json:"year"`
	Venue            string   `json:"venue"`
	Abstract         string   `json:"abstract"`
	ProblemStatement string   `json:"problem_statement"`
	Methodology      string   `json:"methodology"`
	KeyFindings      string   `json:"key_findings"`
	Limitations      string   `json:"limitations"`
	RelevanceScore   float64  `json:"relevance_score"`
}

type GapResponse struct {
	Type        string `json:"gap_type"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Evidence    string `json:"evidence"`
}

type DirectionResponse struct {
	Title             string  `json:"title"`
	Description       string  `json:"description"`
	Difficulty        string  `json:"difficulty"`
	EstimatedCost     string  `json:"estimated_cost"`
	IndustryViability string  `json:"industry_viability"`
	TimeToMVP         string  `json:"time_to_mvp"`
	FeasibilityScore  float64 `json:"feasibility_score"`
}

func (h *Handler) GetResearch(c *gin.Context) {
	topicID := c.Param("id")
	if topicID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Topic ID required"})
		return
	}

	pipeline, err := h.orch.GetPipeline(c.Request.Context(), topicID)
	if err != nil {
		writePipelineLookupError(c, err)
		return
	}

	var report *agent.Report
	if pipeline.Status == "completed" {
		report, err = h.orch.GetReport(c.Request.Context(), topicID)
		if err != nil {
			logger.From(c.Request.Context()).Error().Err(err).Str("topic_id", topicID).Msg("Failed to assemble completed research result")
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Research result is temporarily unavailable"})
			return
		}
	}

	c.JSON(http.StatusOK, buildResearchResponse(pipeline, report))
}

func buildResearchResponse(pipeline *orchestrator.Pipeline, report *agent.Report) ResearchResponse {
	response := ResearchResponse{
		TopicID:         pipeline.TopicID,
		RunID:           pipeline.RunID,
		Topic:           pipeline.Topic,
		Status:          pipeline.Status,
		Stage:           string(pipeline.Stage),
		Progress:        pipeline.Progress,
		StartedAt:       pipeline.StartedAt.Format(time.RFC3339),
		Error:           pipeline.Error,
		Papers:          make([]PaperResponse, 0),
		ResearchGaps:    make([]GapResponse, 0),
		NovelDirections: make([]DirectionResponse, 0),
	}
	if report == nil {
		return response
	}
	response.ExecutiveSummary = report.ExecutiveSummary
	response.LiteratureReview = report.LiteratureReview
	response.GeneratedAt = report.GeneratedAt.Format(time.RFC3339)
	response.BibTeX = report.BibTeX
	response.Papers = make([]PaperResponse, 0, len(report.Papers))
	response.ResearchGaps = make([]GapResponse, 0, len(report.Gaps))
	response.NovelDirections = make([]DirectionResponse, 0, len(report.Directions))
	for _, paper := range report.Papers {
		response.Papers = append(response.Papers, PaperResponse{
			ID:               paper.ID,
			Title:            paper.Title,
			Authors:          append([]string(nil), paper.Authors...),
			Year:             paper.Year,
			Venue:            paper.Venue,
			Abstract:         paper.Abstract,
			ProblemStatement: paper.ProblemStatement,
			Methodology:      paper.Methodology,
			KeyFindings:      paper.KeyFindings,
			Limitations:      paper.Limitations,
			RelevanceScore:   paper.RelevanceScore,
		})
	}
	for _, gap := range report.Gaps {
		response.ResearchGaps = append(response.ResearchGaps, GapResponse{
			Type:        gap.Type,
			Title:       gap.Title,
			Description: gap.Description,
			Evidence:    gap.Evidence,
		})
	}
	for _, direction := range report.Directions {
		response.NovelDirections = append(response.NovelDirections, DirectionResponse{
			Title:             direction.Title,
			Description:       direction.Description,
			Difficulty:        direction.Difficulty,
			EstimatedCost:     direction.EstimatedCost,
			IndustryViability: direction.IndustryViability,
			TimeToMVP:         direction.TimeToMVP,
			FeasibilityScore:  direction.FeasibilityScore,
		})
	}
	return response
}

type StatusResponse struct {
	TopicID  string  `json:"topic_id"`
	RunID    string  `json:"run_id"`
	Status   string  `json:"status"`
	Stage    string  `json:"stage"`
	Progress float64 `json:"progress"`
	Error    string  `json:"error,omitempty"`
}

func (h *Handler) GetStatus(c *gin.Context) {
	topicID := c.Param("id")
	if topicID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Topic ID required"})
		return
	}

	pipeline, err := h.orch.GetPipeline(c.Request.Context(), topicID)
	if err != nil {
		writePipelineLookupError(c, err)
		return
	}

	c.JSON(http.StatusOK, StatusResponse{
		TopicID:  pipeline.TopicID,
		RunID:    pipeline.RunID,
		Status:   pipeline.Status,
		Stage:    string(pipeline.Stage),
		Progress: pipeline.Progress,
		Error:    pipeline.Error,
	})
}

func writePipelineLookupError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, orchestrator.ErrInvalidTopicID):
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid topic ID"})
	case errors.Is(err, orchestrator.ErrPipelineNotFound):
		c.JSON(http.StatusNotFound, gin.H{"error": "Research not found"})
	default:
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Research state unavailable"})
	}
}

func (h *Handler) Stream(c *gin.Context) {
	topicID := c.Param("id")
	if topicID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Topic ID required"})
		return
	}

	pipeline, err := h.orch.GetPipeline(c.Request.Context(), topicID)
	if err != nil {
		writePipelineLookupError(c, err)
		return
	}

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Streaming not supported"})
		return
	}
	controller := http.NewResponseController(c.Writer)
	if err := controller.SetWriteDeadline(time.Time{}); err != nil && !errors.Is(err, http.ErrNotSupported) {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Streaming deadline configuration failed"})
		return
	}

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")

	sse := h.orch.GetSSEManager()
	ch := sse.Subscribe(topicID)
	defer sse.Unsubscribe(topicID, ch)
	if latest, lookupErr := h.orch.GetPipeline(c.Request.Context(), topicID); lookupErr == nil {
		pipeline = latest
	}

	c.SSEvent("status", StatusResponse{TopicID: pipeline.TopicID, RunID: pipeline.RunID, Status: pipeline.Status, Stage: string(pipeline.Stage), Progress: pipeline.Progress, Error: pipeline.Error})
	flusher.Flush()
	ticker := time.NewTicker(h.heartbeat)
	defer ticker.Stop()

	for {
		select {
		case <-c.Request.Context().Done():
			return
		case data, ok := <-ch:
			if !ok {
				return
			}
			if _, err := c.Writer.Write(data); err != nil {
				return
			}
			flusher.Flush()
		case <-ticker.C:
			c.SSEvent("ping", gin.H{"time": time.Now().Unix()})
			flusher.Flush()
		}
	}
}

func (h *Handler) GetReport(c *gin.Context) {
	topicID := c.Param("id")
	if topicID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Topic ID required"})
		return
	}

	if !h.requireCompleted(c, topicID) {
		return
	}
	report, err := h.orch.GetReport(c.Request.Context(), topicID)
	if err != nil {
		logger.From(c.Request.Context()).Error().Err(err).Str("topic_id", topicID).Msg("Failed to assemble Markdown report")
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "Report is temporarily unavailable"})
		return
	}

	c.Header("Content-Type", "text/markdown")
	c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=research-report-%s.md", topicID))
	c.String(http.StatusOK, agent.FormatMarkdown(report))
}

func (h *Handler) GetBibTeX(c *gin.Context) {
	topicID := c.Param("id")
	if topicID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Topic ID required"})
		return
	}

	if !h.requireCompleted(c, topicID) {
		return
	}
	report, err := h.orch.GetReport(c.Request.Context(), topicID)
	if err != nil {
		logger.From(c.Request.Context()).Error().Err(err).Str("topic_id", topicID).Msg("Failed to assemble BibTeX report")
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "BibTeX is temporarily unavailable"})
		return
	}

	c.Header("Content-Type", "text/plain")
	c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=references-%s.bib", topicID))
	c.String(http.StatusOK, report.BibTeX)
}

func (h *Handler) requireCompleted(c *gin.Context, topicID string) bool {
	pipeline, err := h.orch.GetPipeline(c.Request.Context(), topicID)
	if err != nil {
		writePipelineLookupError(c, err)
		return false
	}
	if pipeline.Status != "completed" {
		c.JSON(http.StatusConflict, gin.H{"error": "Research report is not available until the pipeline completes", "status": pipeline.Status, "stage": pipeline.Stage})
		return false
	}
	return true
}
