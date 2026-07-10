package pdf

import (
	"io"
	"net/http"
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

func TestGrobidRejectsOversizedResponse(t *testing.T) {
	const maxBytes = int64(32)
	grobid := NewGrobidClientWithMaxBytes("http://grobid.test", time.Second, maxBytes)
	grobid.SetHTTPClient(&http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("<TEI><text><body><div><p>" + strings.Repeat("x", 64) + "</p></div></body></text></TEI>")),
			Header:     make(http.Header),
		}, nil
	})})

	_, err := grobid.Parse(t.Context(), "paper.pdf", []byte("pdf-data"))
	if err == nil || !strings.Contains(err.Error(), "exceeds maximum size") {
		t.Fatalf("Parse error = %v; want size-limit error", err)
	}
}

func TestGrobidRequestDoesNotRetainMultipartBuffer(t *testing.T) {
	grobid := NewGrobidClientWithMaxBytes("http://grobid.test", time.Second, 1024)
	grobid.SetHTTPClient(&http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if _, err := io.Copy(io.Discard, req.Body); err != nil {
			return nil, err
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("<TEI><text><body/></text></TEI>")), Header: make(http.Header)}, nil
	})})

	if _, err := grobid.Parse(t.Context(), "paper.pdf", []byte("pdf-data")); err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
}
