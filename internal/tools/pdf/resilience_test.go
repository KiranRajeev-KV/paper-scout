package pdf

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/paper-scout/internal/httpresilience"
)

type trackingReader struct {
	data []byte
	read int
}

func (r *trackingReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	r.read += n
	return n, nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func testHTTPPolicy() *httpresilience.Policy {
	return httpresilience.New(context.Background(), "test", httpresilience.Config{
		MaxRetries:       1,
		BaseBackoff:      time.Millisecond,
		MaxBackoff:       time.Millisecond,
		FailureThreshold: 5,
		OpenTimeout:      time.Second,
	}, 0, 0, nil)
}

// Protects downloader retries transient response.
func TestDownloaderRetriesTransientResponse(t *testing.T) {
	calls := 0
	downloader := NewDownloaderWithPolicyAndMaxBytes(time.Second, testHTTPPolicy(), defaultMaxPDFBytes)
	downloader.SetHTTPClient(&http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return &http.Response{StatusCode: http.StatusBadGateway, Body: io.NopCloser(strings.NewReader("retry")), Header: make(http.Header)}, nil
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("%PDF-1.7")), Header: make(http.Header)}, nil
	})})

	_, data, err := downloader.Download(t.Context(), "http://papers.test/paper.pdf")
	if err != nil {
		t.Fatalf("Download returned error: %v", err)
	}
	if calls != 2 || string(data) != "%PDF-1.7" {
		t.Fatalf("calls = %d, data = %q; want two calls and final PDF", calls, string(data))
	}
}

// Protects downloader rejects oversized body.
func TestDownloaderRejectsOversizedBody(t *testing.T) {
	const maxBytes = int64(8)
	body := &trackingReader{data: []byte("1234567890")}
	downloader := NewDownloaderWithMaxBytes(time.Second, maxBytes)
	downloader.SetHTTPClient(&http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode:    http.StatusOK,
			Body:          io.NopCloser(body),
			ContentLength: -1,
			Header:        make(http.Header),
		}, nil
	})})

	_, _, err := downloader.Download(t.Context(), "http://papers.test/paper.pdf")
	if err == nil || !strings.Contains(err.Error(), "exceeds maximum size") {
		t.Fatalf("Download error = %v; want size-limit error", err)
	}
	if body.read != int(maxBytes)+1 {
		t.Fatalf("body bytes read = %d; want %d", body.read, maxBytes+1)
	}
}

// Protects downloader rejects declared oversized body without reading.
func TestDownloaderRejectsDeclaredOversizedBodyWithoutReading(t *testing.T) {
	const maxBytes = int64(8)
	body := &trackingReader{data: []byte("1234567890")}
	downloader := NewDownloaderWithMaxBytes(time.Second, maxBytes)
	downloader.SetHTTPClient(&http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode:    http.StatusOK,
			Body:          io.NopCloser(body),
			ContentLength: maxBytes + 1,
			Header:        make(http.Header),
		}, nil
	})})

	_, _, err := downloader.Download(t.Context(), "http://papers.test/paper.pdf")
	if err == nil || !strings.Contains(err.Error(), "exceeds maximum size") {
		t.Fatalf("Download error = %v; want size-limit error", err)
	}
	if body.read != 0 {
		t.Fatalf("body bytes read = %d; want 0", body.read)
	}
}

// Protects downloader rejects invalid content type without reading.
func TestDownloaderRejectsInvalidContentTypeWithoutReading(t *testing.T) {
	body := &trackingReader{data: []byte("not a PDF")}
	downloader := NewDownloaderWithMaxBytes(time.Second, 1024)
	downloader.SetHTTPClient(&http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(body),
			Header:     http.Header{"Content-Type": []string{"text/html"}},
		}, nil
	})})

	_, _, err := downloader.Download(t.Context(), "http://papers.test/paper.pdf")
	if err == nil || !strings.Contains(err.Error(), "unexpected PDF content type") {
		t.Fatalf("Download error = %v; want content-type error", err)
	}
	if body.read != 0 {
		t.Fatalf("body bytes read = %d; want 0", body.read)
	}
}

// Protects PDF ingestion from accepting non-PDF bytes with an allowed media type.
func TestDownloaderRejectsMissingPDFSignature(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/pdf")
		_, _ = w.Write([]byte("not actually a PDF"))
	}))
	defer server.Close()
	downloader := NewDownloaderWithPolicyAndMaxBytes(time.Second, nil, 1024)
	if _, _, err := downloader.Download(t.Context(), server.URL); err == nil {
		t.Fatal("Download accepted bytes without a PDF signature")
	}
}
