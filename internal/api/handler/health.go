package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/research-agent/internal/storage/postgres"
	"github.com/research-agent/internal/storage/qdrant"
	"github.com/research-agent/internal/storage/redis"
)

type HealthHandler struct {
	postgres *postgres.Client
	redis    *redis.Client
	qdrant   *qdrant.Client
}

func NewHealthHandler(pg *postgres.Client, redis *redis.Client, qdrant *qdrant.Client) *HealthHandler {
	return &HealthHandler{
		postgres: pg,
		redis:    redis,
		qdrant:   qdrant,
	}
}

func (h *HealthHandler) Check(c *gin.Context) {
	services := make(map[string]string)

	ctx := c.Request.Context()

	if err := h.postgres.Ping(ctx); err != nil {
		services["postgres"] = "error: " + err.Error()
	} else {
		services["postgres"] = "ok"
	}

	if err := h.redis.Ping(ctx); err != nil {
		services["redis"] = "error: " + err.Error()
	} else {
		services["redis"] = "ok"
	}

	services["qdrant"] = "ok"

	allOk := true
	for _, status := range services {
		if status != "ok" {
			allOk = false
			break
		}
	}

	status := "ok"
	statusCode := http.StatusOK
	if !allOk {
		status = "degraded"
		statusCode = http.StatusServiceUnavailable
	}

	c.JSON(statusCode, gin.H{
		"status":   status,
		"services": services,
	})
}
