package middleware

import (
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

type admissionLimiter struct {
	mu       sync.Mutex
	tokens   float64
	capacity float64
	rate     float64
	updated  time.Time
}

// SubmissionAdmission bounds process-wide creation of expensive research runs.
func SubmissionAdmission(requestsPerSecond float64, burst int) gin.HandlerFunc {
	limiter := &admissionLimiter{tokens: float64(burst), capacity: float64(burst), rate: requestsPerSecond, updated: time.Now()}
	return func(c *gin.Context) {
		if !limiter.allow(time.Now()) {
			c.Header("Retry-After", "10")
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{"error": "Research submission limit exceeded"})
			return
		}
		c.Next()
	}
}

func (l *admissionLimiter) allow(now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.tokens += now.Sub(l.updated).Seconds() * l.rate
	l.updated = now
	if l.tokens > l.capacity {
		l.tokens = l.capacity
	}
	if l.tokens < 1 {
		return false
	}
	l.tokens--
	return true
}
