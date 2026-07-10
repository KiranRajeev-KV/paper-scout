package pdf

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/paper-scout/internal/httpresilience"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func testHTTPPolicy() *httpresilience.Policy {
	return httpresilience.New("test", httpresilience.Config{
		MaxRetries:       1,
		BaseBackoff:      time.Millisecond,
		MaxBackoff:       time.Millisecond,
		FailureThreshold: 5,
		OpenTimeout:      time.Second,
	}, 0, 0, nil)
}

func TestGrobidRetriesWithReplayableMultipartBody(t *testing.T) {
	calls := 0
	bodySizes := make([]int64, 0, 2)
	grobid := NewGrobidClientWithPolicy("http://grobid.test", time.Second, testHTTPPolicy())
	grobid.SetHTTPClient(&http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls++
		body, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		bodySizes = append(bodySizes, int64(len(body)))
		if calls == 1 {
			return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: io.NopCloser(strings.NewReader("busy")), Header: make(http.Header)}, nil
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("<TEI><text><body/></text></TEI>")), Header: make(http.Header)}, nil
	})})

	if _, err := grobid.Parse(t.Context(), "paper.pdf", []byte("pdf-data")); err != nil {
		t.Fatalf("Grobid.Parse returned error: %v", err)
	}
	if calls != 2 || len(bodySizes) != 2 || bodySizes[0] == 0 || bodySizes[0] != bodySizes[1] {
		t.Fatalf("calls = %d, body sizes = %v; want two equivalent multipart attempts", calls, bodySizes)
	}
}

func TestDownloaderRetriesTransientResponse(t *testing.T) {
	calls := 0
	downloader := NewDownloaderWithPolicy(time.Second, testHTTPPolicy())
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
