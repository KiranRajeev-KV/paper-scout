package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/paper-scout/internal/api/handler"
	"github.com/paper-scout/internal/api/middleware"
	"github.com/paper-scout/internal/orchestrator"
)

func SetupRouter(orch *orchestrator.Orchestrator) *gin.Engine {
	r := gin.New()

	r.Use(middleware.Logger())
	r.Use(middleware.Recovery())
	r.Use(middleware.CORS())

	h := handler.New(orch)

	api := r.Group("/api/v1")
	{
		api.POST("/research", h.CreateResearch)
		api.GET("/research/:id", h.GetResearch)
		api.GET("/research/:id/status", h.GetStatus)
		api.GET("/research/:id/stream", h.Stream)
		api.GET("/research/:id/report", h.GetReport)
		api.GET("/research/:id/bibtex", h.GetBibTeX)
	}

	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	return r
}
