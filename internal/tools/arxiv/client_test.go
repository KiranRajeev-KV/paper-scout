package arxiv

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/paper-scout/internal/config"
)

// Protects external client policies retry ar xiv requests.
func TestExternalClientPoliciesRetryArXivRequests(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(`<feed xmlns="http://www.w3.org/2005/Atom"></feed>`))
	}))
	defer server.Close()

	client := NewClient(t.Context(), config.ArXivConfig{
		BaseURL:    server.URL,
		Timeout:    time.Second,
		Resilience: config.ResilienceConfig{MaxRetries: 1, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond, FailureThreshold: 3, OpenTimeout: time.Second},
	})
	if _, err := client.Search(context.Background(), "all:test", 1); err != nil {
		t.Fatalf("Search returned error: %v", err)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("requests = %d, want 2 after retry", got)
	}
}
