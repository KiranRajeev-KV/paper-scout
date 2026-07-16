package api

import (
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/paper-scout/internal/api/handler"
	"github.com/paper-scout/internal/api/middleware"
	"github.com/paper-scout/internal/config"
	"github.com/paper-scout/internal/logger"
	"github.com/paper-scout/internal/orchestrator"
)

// SetupRouter constructs the HTTP router with explicit application-log ownership.
func SetupRouter(orch *orchestrator.Orchestrator, health *handler.HealthHandler, cfg config.ServerConfig, logs *logger.Manager) (*gin.Engine, error) {
	if logs == nil {
		return nil, fmt.Errorf("router requires logging manager")
	}
	r := gin.New()

	requestLogger, err := middleware.Logger(logs.App())
	if err != nil {
		return nil, err
	}
	r.Use(requestLogger)
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

	return r, nil
}
