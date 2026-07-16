package middleware

import (
	"fmt"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/paper-scout/internal/logger"
	"github.com/rs/zerolog"
)

// Logger attaches the application logger to each request before recording its outcome.
func Logger(app *zerolog.Logger) (gin.HandlerFunc, error) {
	if app == nil {
		return nil, fmt.Errorf("request logger requires an application logger")
	}
	return func(c *gin.Context) {
		c.Request = c.Request.WithContext(app.WithContext(c.Request.Context()))
		start := time.Now()
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery

		c.Next()

		latency := time.Since(start)
		status := c.Writer.Status()

		if query != "" {
			path = path + "?" + query
		}

		requestContext := c.Request.Context()
		event := logger.From(requestContext).Info()
		if status >= 400 {
			event = logger.From(requestContext).Warn()
		}
		if status >= 500 {
			event = logger.From(requestContext).Error()
		}

		event.
			Str("method", c.Request.Method).
			Str("path", path).
			Int("status", status).
			Dur("latency", latency).
			Str("client_ip", c.ClientIP()).
			Msg("HTTP request")
	}, nil
}
