package handler

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/paper-scout/internal/agent"
	"github.com/paper-scout/internal/orchestrator"
)

type fakeResearchService struct {
	pipeline *orchestrator.Pipeline
	sse      *orchestrator.SSEManager
}

func (f *fakeResearchService) StartResearch(context.Context, string) (*orchestrator.Pipeline, error) {
	return f.pipeline, nil
}
func (f *fakeResearchService) GetPipeline(context.Context, string) (*orchestrator.Pipeline, error) {
	copy := *f.pipeline
	return &copy, nil
}
func (f *fakeResearchService) GetReport(context.Context, string) (*agent.Report, error) {
	return &agent.Report{}, nil
}
func (f *fakeResearchService) GetSSEManager() *orchestrator.SSEManager { return f.sse }

// Protects sse survives write timeout and replays current status.
func TestSSESurvivesWriteTimeoutAndReplaysCurrentStatus(t *testing.T) {
	gin.SetMode(gin.TestMode)
	topicID := "6dbf4af6-8ca3-4bb7-b621-c2f34fd72199"
	service := &fakeResearchService{
		pipeline: &orchestrator.Pipeline{TopicID: topicID, RunID: "run-1", Status: "processing", Stage: orchestrator.StageAnalysis, Progress: 0.4},
		sse:      orchestrator.NewSSEManager(t.Context()),
	}
	handler := New(service)
	handler.heartbeat = 10 * time.Millisecond
	router := gin.New()
	router.GET("/stream/:id", handler.Stream)
	server := httptest.NewUnstartedServer(router)
	server.Config.WriteTimeout = 25 * time.Millisecond
	server.Start()
	defer server.Close()

	response, err := http.Get(server.URL + "/stream/" + topicID)
	if err != nil {
		t.Fatalf("open SSE stream: %v", err)
	}
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	immediate := readSSEEvent(t, reader, time.Second)
	if !strings.Contains(immediate, "event:status") || !strings.Contains(immediate, `"progress":0.4`) {
		t.Fatalf("immediate event = %q, want current status", immediate)
	}

	time.Sleep(60 * time.Millisecond)
	if err := service.sse.Send(topicID, "progress", map[string]any{"topic_id": topicID, "progress": 0.7}); err != nil {
		t.Fatalf("send progress: %v", err)
	}
	deadline := time.After(time.Second)
	for {
		event := readSSEEvent(t, reader, time.Second)
		if strings.Contains(event, "event: progress") && strings.Contains(event, `"progress":0.7`) {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("progress event not received after write timeout; last event %q", event)
		default:
		}
	}
}

func readSSEEvent(t *testing.T, reader *bufio.Reader, timeout time.Duration) string {
	t.Helper()
	type result struct {
		value string
		err   error
	}
	done := make(chan result, 1)
	go func() {
		var lines strings.Builder
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				done <- result{err: err}
				return
			}
			lines.WriteString(line)
			if line == "\n" {
				done <- result{value: lines.String()}
				return
			}
		}
	}()
	select {
	case result := <-done:
		if result.err != nil && result.err != io.EOF {
			t.Fatalf("read SSE event: %v", result.err)
		}
		return result.value
	case <-time.After(timeout):
		t.Fatal("timed out reading SSE event")
		return ""
	}
}
