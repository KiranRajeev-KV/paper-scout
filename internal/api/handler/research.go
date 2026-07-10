package handler

import (
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/paper-scout/internal/agent"
	"github.com/paper-scout/internal/orchestrator"
)

type Handler struct {
	orch *orchestrator.Orchestrator
}

func New(orch *orchestrator.Orchestrator) *Handler {
	return &Handler{orch: orch}
}

type CreateResearchRequest struct {
	Topic string `json:"topic" binding:"required,min=10,max=500"`
}

type CreateResearchResponse struct {
	TopicID string `json:"topic_id"`
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

func (h *Handler) CreateResearch(c *gin.Context) {
	var req CreateResearchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request: " + err.Error()})
		return
	}

	pipeline, err := h.orch.StartResearch(c.Request.Context(), req.Topic)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to start research: " + err.Error()})
		return
	}

	c.JSON(http.StatusAccepted, CreateResearchResponse{
		TopicID: pipeline.TopicID,
		Status:  pipeline.Status,
		Message: "Research started. Use the topic_id to track progress.",
	})
}

type ResearchResponse struct {
	TopicID          string              `json:"topic_id"`
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

	pipeline, err := h.orch.GetPipeline(topicID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Research not found"})
		return
	}

	var report *agent.Report
	if pipeline.Status == "completed" {
		report, err = h.orch.GetReport(c.Request.Context(), topicID)
		if err != nil {
			c.JSON(http.StatusNotFound, gin.H{"error": "Research result not found"})
			return
		}
	}

	c.JSON(http.StatusOK, buildResearchResponse(pipeline, report))
}

func buildResearchResponse(pipeline *orchestrator.Pipeline, report *agent.Report) ResearchResponse {
	response := ResearchResponse{
		TopicID:         pipeline.TopicID,
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

	pipeline, err := h.orch.GetPipeline(topicID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Research not found"})
		return
	}

	c.JSON(http.StatusOK, StatusResponse{
		TopicID:  pipeline.TopicID,
		Status:   pipeline.Status,
		Stage:    string(pipeline.Stage),
		Progress: pipeline.Progress,
		Error:    pipeline.Error,
	})
}

func (h *Handler) Stream(c *gin.Context) {
	topicID := c.Param("id")
	if topicID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Topic ID required"})
		return
	}

	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("Access-Control-Allow-Origin", "*")

	sse := h.orch.GetSSEManager()
	ch := sse.Subscribe(topicID)
	defer sse.Unsubscribe(topicID, ch)

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Streaming not supported"})
		return
	}

	c.SSEvent("connected", gin.H{"topic_id": topicID})
	flusher.Flush()

	for {
		select {
		case <-c.Request.Context().Done():
			return
		case data, ok := <-ch:
			if !ok {
				return
			}
			c.Writer.Write(data)
			flusher.Flush()
		case <-time.After(30 * time.Second):
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

	report, err := h.orch.GetReport(c.Request.Context(), topicID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Report not found"})
		return
	}

	c.Header("Content-Type", "text/markdown")
	c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=research-report-%s.md", topicID))
	c.String(http.StatusOK, h.generateMarkdown(report))
}

func (h *Handler) GetBibTeX(c *gin.Context) {
	topicID := c.Param("id")
	if topicID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Topic ID required"})
		return
	}

	report, err := h.orch.GetReport(c.Request.Context(), topicID)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "Report not found"})
		return
	}

	c.Header("Content-Type", "text/plain")
	c.Header("Content-Disposition", fmt.Sprintf("attachment; filename=references-%s.bib", topicID))
	c.String(http.StatusOK, report.BibTeX)
}

func (h *Handler) generateMarkdown(report *agent.Report) string {
	var md string

	md += "# Research Report\n\n"
	md += fmt.Sprintf("**Topic:** %s\n\n", report.Topic)
	md += fmt.Sprintf("*Generated: %s*\n\n---\n\n", report.GeneratedAt.Format("January 2, 2006"))

	md += report.ExecutiveSummary
	md += "\n\n"
	md += report.LiteratureReview

	if len(report.Gaps) > 0 {
		md += "\n\n## Research Gaps\n\n"
		for i, gap := range report.Gaps {
			md += fmt.Sprintf("### %d. %s\n", i+1, gap.Title)
			md += fmt.Sprintf("**Type:** %s\n\n", gap.Type)
			md += fmt.Sprintf("%s\n\n", gap.Description)
		}
	}

	if len(report.Directions) > 0 {
		md += "\n\n## Research Directions\n\n"
		for i, dir := range report.Directions {
			md += fmt.Sprintf("### %d. %s\n", i+1, dir.Title)
			md += fmt.Sprintf("**Difficulty:** %s | **Score:** %.1f\n\n", dir.Difficulty, dir.FeasibilityScore)
			md += fmt.Sprintf("%s\n\n", dir.Description)
		}
	}

	md += "\n\n## References\n\n"
	md += "```bibtex\n" + report.BibTeX + "\n```\n"

	return md
}
