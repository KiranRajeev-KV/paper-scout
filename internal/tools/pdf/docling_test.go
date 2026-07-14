package pdf

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// Protects docling success uses stable file endpoint.
func TestDoclingSuccessUsesStableFileEndpoint(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/convert/file" {
			t.Errorf("path = %s, want stable conversion endpoint", r.URL.Path)
		}
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Errorf("parse multipart: %v", err)
			return
		}
		file, _, err := r.FormFile("files")
		if err != nil {
			t.Errorf("PDF part: %v", err)
			return
		}
		defer file.Close()
		if r.FormValue("from_formats") != "pdf" || r.FormValue("to_formats") != "md" || r.FormValue("do_ocr") != "false" {
			t.Errorf("form values = %#v", r.MultipartForm.Value)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"document": map[string]any{"md_content": "# Paper\n\nUseful extracted content", "json_content": map[string]any{"pages": 1}}, "status": "success", "processing_time": 0.25, "errors": []any{}})
	}))
	defer server.Close()
	client := testDoclingClient(t, server.URL, "never", time.Second, 10)
	document, err := client.Parse(context.Background(), "paper.pdf", []byte("%PDF-1.7"))
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	if document.Markdown == "" || len(document.JSON) == 0 || document.UsedOCR {
		t.Fatalf("document = %+v, want Markdown/JSON without OCR", document)
	}
}

// Protects docling partial result is an error.
func TestDoclingPartialResultIsAnError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"document": map[string]any{"md_content": "partial useful text"}, "status": "partial_success", "errors": []map[string]string{{"message": "page failed"}}})
	}))
	defer server.Close()
	client := testDoclingClient(t, server.URL, "never", time.Second, 5)
	document, err := client.Parse(context.Background(), "paper.pdf", []byte("%PDF"))
	if !errors.Is(err, ErrPartialDoclingResult) || document.Markdown == "" {
		t.Fatalf("document/error = %+v/%v, want inspectable partial result with strict error", document, err)
	}
}

// Protects docling empty extraction triggers ocr fallback.
func TestDoclingEmptyExtractionTriggersOCRFallback(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			t.Errorf("parse multipart: %v", err)
			return
		}
		ocr := r.FormValue("do_ocr")
		content := "x"
		if ocr == "true" {
			content = "OCR recovered enough useful academic content"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"document": map[string]any{"md_content": content}, "status": "success", "errors": []any{}})
	}))
	defer server.Close()
	client := testDoclingClient(t, server.URL, "fallback", time.Second, 20)
	document, err := client.Parse(context.Background(), "scan.pdf", []byte("%PDF"))
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	if calls != 2 || !document.UsedOCR {
		t.Fatalf("calls/UsedOCR = %d/%v, want one fallback OCR conversion", calls, document.UsedOCR)
	}
}

// Protects docling request timeout.
func TestDoclingRequestTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { time.Sleep(100 * time.Millisecond) }))
	defer server.Close()
	client := testDoclingClient(t, server.URL, "never", 20*time.Millisecond, 5)
	if _, err := client.Parse(context.Background(), "paper.pdf", []byte("%PDF")); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Parse error = %v, want deadline exceeded", err)
	}
}

func testDoclingClient(t *testing.T, baseURL, ocr string, timeout time.Duration, minimum int) *DoclingClient {
	t.Helper()
	client, err := NewDoclingClient(DoclingConfig{BaseURL: baseURL, RequestTimeout: timeout, DocumentTimeout: time.Second, OCRBehavior: ocr, OutputFormat: "md", Concurrency: 1, Version: "1.21.0", MaxResponseBytes: 1 << 20, MinExtractedCharacters: minimum})
	if err != nil {
		t.Fatalf("NewDoclingClient returned error: %v", err)
	}
	return client
}
