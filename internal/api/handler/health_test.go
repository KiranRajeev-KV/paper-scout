package handler

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestReadinessFailsWhenDependencyFails(t *testing.T) {
	gin.SetMode(gin.TestMode)

	for _, unavailable := range []string{"postgres", "redis", "qdrant", "gemini"} {
		t.Run(unavailable, func(t *testing.T) {
			dependencies := map[string]healthDependency{}
			for _, name := range []string{"postgres", "redis", "qdrant", "gemini"} {
				name := name
				dependencies[name] = healthCheckFunc(func(context.Context) error {
					if name == unavailable {
						return errors.New("unavailable")
					}
					return nil
				})
			}

			handler := &HealthHandler{dependencies: dependencies}
			router := gin.New()
			router.GET("/health/ready", handler.Check)

			response := httptest.NewRecorder()
			router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
			if response.Code != http.StatusServiceUnavailable {
				t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusServiceUnavailable, response.Body.String())
			}
		})
	}
}

func TestReadinessSucceedsWhenAllDependenciesAreHealthy(t *testing.T) {
	gin.SetMode(gin.TestMode)
	dependencies := map[string]healthDependency{}
	for _, name := range []string{"postgres", "redis", "qdrant", "gemini"} {
		dependencies[name] = healthCheckFunc(func(context.Context) error { return nil })
	}

	handler := &HealthHandler{dependencies: dependencies}
	router := gin.New()
	router.GET("/health/ready", handler.Check)

	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusOK, response.Body.String())
	}
}
