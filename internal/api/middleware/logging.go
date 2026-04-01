package middleware

import (
	"time"

	"github.com/gin-gonic/gin"
	"github.com/paper-scout/internal/logger"
)

func Logger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery

		c.Next()

		latency := time.Since(start)
		status := c.Writer.Status()

		if query != "" {
			path = path + "?" + query
		}

		event := logger.Info()
		if status >= 400 {
			event = logger.Warn()
		}
		if status >= 500 {
			event = logger.Error()
		}

		event.
			Str("method", c.Request.Method).
			Str("path", path).
			Int("status", status).
			Dur("latency", latency).
			Str("client_ip", c.ClientIP()).
			Msg("HTTP request")
	}
}
