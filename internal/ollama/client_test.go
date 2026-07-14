package ollama

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Protects embedder separates document and query inputs.
func TestEmbedderSeparatesDocumentAndQueryInputs(t *testing.T) {
	var requests []map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
			return
		}
		requests = append(requests, request)
		inputs := request["input"].([]any)
		vectors := make([][]float32, len(inputs))
		for i := range vectors {
			vectors[i] = []float32{1, 2, 3}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"embeddings": vectors})
	}))
	defer server.Close()

	embedder, err := NewEmbedder(EmbeddingConfig{BaseURL: server.URL, Model: "qwen3-embedding:8b", Timeout: time.Second, Dimensions: 3, QueryInstruction: "retrieve academic papers", InstructionVersion: "qwen-v1", IndexingVersion: "v1"})
	if err != nil {
		t.Fatalf("NewEmbedder returned error: %v", err)
	}
	if _, err := embedder.EmbedDocuments(context.Background(), []string{"document one", "document two"}); err != nil {
		t.Fatalf("EmbedDocuments returned error: %v", err)
	}
	if _, err := embedder.EmbedQuery(context.Background(), "transformers"); err != nil {
		t.Fatalf("EmbedQuery returned error: %v", err)
	}

	documents := requests[0]["input"].([]any)
	query := requests[1]["input"].([]any)
	if !reflect.DeepEqual(documents, []any{"document one", "document two"}) {
		t.Fatalf("document input = %#v, want unmodified documents", documents)
	}
	if !reflect.DeepEqual(query, []any{"Instruct: retrieve academic papers\nQuery:transformers"}) {
		t.Fatalf("query input = %#v, want Qwen instruction format", query)
	}
	if requests[0]["dimensions"] != float64(3) || requests[0]["model"] != "qwen3-embedding:8b" {
		t.Fatalf("batch request = %#v, want configured dimensions and model", requests[0])
	}
}

// Protects embedder rejects invalid vectors.
func TestEmbedderRejectsInvalidVectors(t *testing.T) {
	tests := []struct {
		name    string
		vectors [][]float32
	}{
		{name: "count", vectors: [][]float32{{1, 2}}},
		{name: "dimension", vectors: [][]float32{{1}, {1, 2}}},
		{name: "nan", vectors: [][]float32{{1, 2}, {1, float32(math.NaN())}}},
		{name: "infinity", vectors: [][]float32{{1, 2}, {1, float32(math.Inf(1))}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_ = json.NewEncoder(w).Encode(map[string]any{"embeddings": test.vectors})
			}))
			defer server.Close()
			embedder, err := NewEmbedder(EmbeddingConfig{BaseURL: server.URL, Model: "embed", Timeout: time.Second, Dimensions: 2, QueryInstruction: "task", InstructionVersion: "v1", IndexingVersion: "v1"})
			if err != nil {
				t.Fatalf("NewEmbedder returned error: %v", err)
			}
			if _, err := embedder.EmbedDocuments(context.Background(), []string{"one", "two"}); err == nil {
				t.Fatal("EmbedDocuments returned nil error for malformed vectors")
			}
		})
	}
}

// Protects health reports unavailable model.
func TestHealthReportsUnavailableModel(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"models": []map[string]string{{"name": "another:latest"}}})
	}))
	defer server.Close()
	generator, err := NewGenerator(GenerationConfig{BaseURL: server.URL, Model: "missing:4b", Timeout: time.Second, Concurrency: 1, MaxOutputTokens: 64})
	if err != nil {
		t.Fatalf("NewGenerator returned error: %v", err)
	}
	if err := generator.Health(context.Background()); !errors.Is(err, ErrModelUnavailable) {
		t.Fatalf("Health error = %v, want ErrModelUnavailable", err)
	}
}

// Protects generator sends structured json schema.
func TestGeneratorSendsStructuredJSONSchema(t *testing.T) {
	var request map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"message": map[string]string{"content": `{"answer":"ok"}`}})
	}))
	defer server.Close()
	generator, err := NewGenerator(GenerationConfig{BaseURL: server.URL, Model: "qwen3.5:4b-q4_K_M", Timeout: time.Second, KeepAlive: "5m", Concurrency: 1, Think: false, MaxOutputTokens: 64, Temperature: 0})
	if err != nil {
		t.Fatalf("NewGenerator returned error: %v", err)
	}
	result, err := generator.GenerateStructured(context.Background(), "prompt", struct {
		Answer string `json:"answer"`
	}{Answer: ""})
	if err != nil {
		t.Fatalf("GenerateStructured returned error: %v", err)
	}
	options, _ := request["options"].(map[string]any)
	messages, _ := request["messages"].([]any)
	content := messages[0].(map[string]any)["content"].(string)
	if result != `{"answer":"ok"}` || request["format"] == nil || request["stream"] != false || request["think"] != false || options["num_predict"] != float64(64) || options["temperature"] != float64(0) || !strings.Contains(content, `"answer"`) {
		t.Fatalf("request/result = %#v/%q, want native structured request", request, result)
	}
}

// Protects structured generation accepts only raw JSON or one fenced JSON value.
func TestNormalizeStructuredJSON(t *testing.T) {
	for name, test := range map[string]struct {
		input string
		want  string
		ok    bool
	}{
		"raw json":               {`{"answer":"ok"}`, `{"answer":"ok"}`, true},
		"fenced json":            {"```json\n{\"answer\":\"ok\"}\n```", `{"answer":"ok"}`, true},
		"upper case fence label": {"```JSON\n{\"answer\":\"ok\"}\n```", `{"answer":"ok"}`, true},
		"prose before fence":     {"Here is the answer:\n```json\n{\"answer\":\"ok\"}\n```", "", false},
		"multiple fences":        {"```json\n{\"answer\":\"ok\"}\n```\n```json\n{}\n```", "", false},
		"malformed json":         {"```json\n{\"answer\":}\n```", "", false},
	} {
		t.Run(name, func(t *testing.T) {
			got, gotOK := normalizeStructuredJSON(test.input)
			if got != test.want || gotOK != test.ok {
				t.Fatalf("normalizeStructuredJSON(%q) = (%q, %t), want (%q, %t)", test.input, got, gotOK, test.want, test.ok)
			}
		})
	}
}
