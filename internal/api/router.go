package api

import (
	"github.com/gin-gonic/gin"
	"github.com/paper-scout/internal/api/handler"
	"github.com/paper-scout/internal/api/middleware"
	"github.com/paper-scout/internal/config"
	"github.com/paper-scout/internal/orchestrator"
)

func SetupRouter(orch *orchestrator.Orchestrator, health *handler.HealthHandler, cfg config.ServerConfig) *gin.Engine {
	r := gin.New()

	r.Use(middleware.Logger())
	r.Use(middleware.Recovery())
	r.Use(middleware.CORS(cfg.AllowedOrigins))

	h := handler.New(orch)

	api := r.Group("/api/v1")
	{
		api.POST("/research", middleware.SubmissionAdmission(cfg.SubmissionRate, cfg.SubmissionBurst), h.CreateResearch)
		api.GET("/research/:id", h.GetResearch)
		api.GET("/research/:id/status", h.GetStatus)
		api.GET("/research/:id/stream", h.Stream)
		api.GET("/research/:id/report", h.GetReport)
		api.GET("/research/:id/bibtex", h.GetBibTeX)
	}

	r.GET("/health", health.Check)
	r.GET("/health/live", health.Live)
	r.GET("/health/ready", health.Check)

	return r
}
