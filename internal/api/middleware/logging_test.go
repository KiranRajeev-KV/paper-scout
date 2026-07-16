package middleware

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/paper-scout/internal/logger"
	"github.com/rs/zerolog"
)

// Protects request middleware attaches the configured logger to handler contexts.
func TestLoggerAttachesApplicationLoggerToRequestContext(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var output bytes.Buffer
	app := zerolog.New(&output)
	middleware, err := Logger(&app)
	if err != nil {
		t.Fatalf("Logger returned error: %v", err)
	}
	router := gin.New()
	router.Use(middleware)
	router.GET("/", func(c *gin.Context) { logger.From(c.Request.Context()).Info().Msg("handler event") })
	router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	logs := output.String()
	if !strings.Contains(logs, "handler event") || !strings.Contains(logs, "HTTP request") {
		t.Fatalf("request logs = %q, want handler and access events", logs)
	}
}

// Protects request middleware rejects a missing application logger.
func TestLoggerRejectsMissingApplicationLogger(t *testing.T) {
	if _, err := Logger(nil); err == nil {
		t.Fatal("Logger accepted nil application logger")
	}
}
