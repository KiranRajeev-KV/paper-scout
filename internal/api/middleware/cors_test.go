package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

// Protects cors rejects unlisted origin.
func TestCORSRejectsUnlistedOrigin(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(CORS([]string{"http://localhost:3000"}))
	router.GET("/", func(c *gin.Context) { c.Status(http.StatusOK) })
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Origin", "https://example.invalid")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, req)
	if response.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusForbidden)
	}
}

// Protects submission admission rejects burst overflow.
func TestSubmissionAdmissionRejectsBurstOverflow(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.POST("/", SubmissionAdmission(0.01, 1), func(c *gin.Context) { c.Status(http.StatusAccepted) })
	first := httptest.NewRecorder()
	router.ServeHTTP(first, httptest.NewRequest(http.MethodPost, "/", nil))
	second := httptest.NewRecorder()
	router.ServeHTTP(second, httptest.NewRequest(http.MethodPost, "/", nil))
	if first.Code != http.StatusAccepted || second.Code != http.StatusTooManyRequests {
		t.Fatalf("statuses = (%d, %d), want (%d, %d)", first.Code, second.Code, http.StatusAccepted, http.StatusTooManyRequests)
	}
}
