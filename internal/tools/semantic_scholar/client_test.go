package semantic_scholar

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/paper-scout/internal/config"
)

func TestExternalClientPoliciesRetrySemanticScholarRequests(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"total":0,"data":[]}`))
	}))
	defer server.Close()

	client := NewClient(config.SemanticScholarConfig{
		BaseURL:    server.URL,
		Timeout:    time.Second,
		Resilience: config.ResilienceConfig{MaxRetries: 1, BaseBackoff: time.Millisecond, MaxBackoff: time.Millisecond, FailureThreshold: 3, OpenTimeout: time.Second},
	})
	if _, err := client.Search(context.Background(), "test", 1, 0); err != nil {
		t.Fatalf("Search returned error: %v", err)
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("requests = %d, want 2 after retry", got)
	}
}
