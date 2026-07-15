package handler

import (
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/paper-scout/internal/logger"
)

// HealthCheck is the minimal readiness contract used by the HTTP layer.
type HealthCheck interface {
	Ping(context.Context) error
}

type healthDependency = HealthCheck

// HealthCheckFunc adapts a function to a readiness check.
type HealthCheckFunc func(context.Context) error

// Ping invokes the adapted readiness function.
func (f HealthCheckFunc) Ping(ctx context.Context) error { return f(ctx) }

type healthCheckFunc = HealthCheckFunc

// HealthHandler reports liveness separately from bounded dependency readiness.
type HealthHandler struct {
	dependencies map[string]HealthCheck
	timeout      time.Duration
}

// NewHealthHandler constructs a readiness handler from explicitly named checks.
func NewHealthHandler(dependencies map[string]HealthCheck) *HealthHandler {
	copyOfDependencies := make(map[string]HealthCheck, len(dependencies))
	for name, dependency := range dependencies {
		copyOfDependencies[name] = dependency
	}
	return &HealthHandler{dependencies: copyOfDependencies, timeout: 2 * time.Second}
}

// Check runs dependency checks concurrently so one slow provider cannot consume every check's budget.
func (h *HealthHandler) Check(c *gin.Context) {
	services := make(map[string]string, len(h.dependencies))
	var mu sync.Mutex
	var wg sync.WaitGroup
	for name, dependency := range h.dependencies {
		name, dependency := name, dependency
		wg.Add(1)
		go func() {
			defer wg.Done()
			status := "ok"
			if dependency == nil {
				status = "unavailable"
			} else {
				timeout := h.timeout
				if timeout <= 0 {
					timeout = 2 * time.Second
				}
				ctx, cancel := context.WithTimeout(c.Request.Context(), timeout)
				err := dependency.Ping(ctx)
				cancel()
				if err != nil {
					status = "unavailable"
					logger.From(c.Request.Context()).Warn().Err(err).Str("dependency", name).Msg("Readiness check failed")
				}
			}
			mu.Lock()
			services[name] = status
			mu.Unlock()
		}()
	}
	wg.Wait()

	allOK := true
	for _, status := range services {
		if status != "ok" {
			allOK = false
			break
		}
	}
	if !allOK {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "degraded", "services": services})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok", "services": services})
}

// Live reports whether the HTTP process is serving requests without probing dependencies.
func (h *HealthHandler) Live(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}
