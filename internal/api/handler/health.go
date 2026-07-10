package handler

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/paper-scout/internal/storage/postgres"
	"github.com/paper-scout/internal/storage/qdrant"
	"github.com/paper-scout/internal/storage/redis"
)

type healthDependency interface {
	Ping(context.Context) error
}

type healthCheckFunc func(context.Context) error

func (f healthCheckFunc) Ping(ctx context.Context) error {
	return f(ctx)
}

type HealthHandler struct {
	dependencies map[string]healthDependency
}

func NewHealthHandler(pg *postgres.Client, redis *redis.Client, qdrant *qdrant.Client, geminiInitialized bool) *HealthHandler {
	return &HealthHandler{
		dependencies: map[string]healthDependency{
			"postgres": pg,
			"redis":    redis,
			"qdrant":   qdrant,
			"gemini": healthCheckFunc(func(context.Context) error {
				if !geminiInitialized {
					return errors.New("client is not initialized")
				}
				return nil
			}),
		},
	}
}

func (h *HealthHandler) Check(c *gin.Context) {
	services := make(map[string]string)

	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	for name, dependency := range h.dependencies {
		if dependency == nil {
			services[name] = "error: dependency is not configured"
			continue
		}
		if err := dependency.Ping(ctx); err != nil {
			services[name] = "error: " + err.Error()
			continue
		}
		services[name] = "ok"
	}

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

func (h *HealthHandler) Live(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
