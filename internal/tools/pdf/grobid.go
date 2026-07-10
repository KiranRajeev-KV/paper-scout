package pdf

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"time"

	"github.com/paper-scout/internal/httpresilience"
	"github.com/paper-scout/internal/logger"
)

const (
	defaultMaxGrobidResponseBytes int64 = 16 << 20
	maxGrobidErrorBytes           int64 = 64 << 10
)

type GrobidClient struct {
	httpClient *http.Client
	baseURL    string
	policy     *httpresilience.Policy
	maxBytes   int64
}

func NewGrobidClientWithPolicy(baseURL string, timeout time.Duration, policy *httpresilience.Policy) *GrobidClient {
	client := NewGrobidClient(baseURL, timeout)
	client.policy = policy
	return client
}

func NewGrobidClientWithPolicyAndMaxBytes(baseURL string, timeout time.Duration, policy *httpresilience.Policy, maxBytes int64) *GrobidClient {
	client := NewGrobidClientWithMaxBytes(baseURL, timeout, maxBytes)
	client.policy = policy
	return client
}

func NewGrobidClient(baseURL string, timeout time.Duration) *GrobidClient {
	return NewGrobidClientWithMaxBytes(baseURL, timeout, defaultMaxGrobidResponseBytes)
}

func NewGrobidClientWithMaxBytes(baseURL string, timeout time.Duration, maxBytes int64) *GrobidClient {
	if maxBytes <= 0 {
		maxBytes = defaultMaxGrobidResponseBytes
	}
	return &GrobidClient{
		httpClient: &http.Client{
			Timeout: timeout,
		},
		baseURL:  baseURL,
		maxBytes: maxBytes,
	}
}

func (g *GrobidClient) SetHTTPClient(client *http.Client) {
	if client != nil {
		g.httpClient = client
	}
}

// TEI XML structures
type TEI struct {
	XMLName xml.Name `xml:"TEI"`
	Text    Text     `xml:"text"`
}

type Text struct {
	Body Body `xml:"body"`
}

type Body struct {
	Divs []Div `xml:"div"`
}

type Div struct {
	Head string   `xml:"head"`
	Ps   []string `xml:"p"`
}

func (g *GrobidClient) Parse(ctx context.Context, filename string, data []byte) (*TEI, error) {
	start := time.Now()

	url := fmt.Sprintf("%s/api/processFulltextDocument", g.baseURL)
	request := func(ctx context.Context) (*http.Response, error) {
		pipeReader, pipeWriter := io.Pipe()
		writer := multipart.NewWriter(pipeWriter)
		contentType := writer.FormDataContentType()
		req, err := http.NewRequestWithContext(ctx, "POST", url, pipeReader)
		if err != nil {
			_ = pipeReader.CloseWithError(err)
			return nil, fmt.Errorf("failed to create request: %w", err)
		}
		req.Header.Set("Content-Type", contentType)
		req.Header.Set("Accept", "application/xml")

		go func() {
			defer pipeWriter.Close()
			part, err := writer.CreateFormFile("input", filename)
			if err != nil {
				_ = pipeWriter.CloseWithError(fmt.Errorf("failed to create form file: %w", err))
				return
			}
			if _, err := io.Copy(part, bytes.NewReader(data)); err != nil {
				_ = pipeWriter.CloseWithError(fmt.Errorf("failed to write file data: %w", err))
				return
			}
			if err := writer.Close(); err != nil {
				_ = pipeWriter.CloseWithError(fmt.Errorf("failed to close writer: %w", err))
			}
		}()

		resp, err := g.httpClient.Do(req)
		if err != nil {
			_ = pipeReader.CloseWithError(err)
		}
		return resp, err
	}
	var resp *http.Response
	var err error
	if g.policy != nil {
		resp, err = g.policy.Do(ctx, "processFulltextDocument", request)
	} else {
		resp, err = request(ctx)
	}
	if err != nil {
		if resp != nil {
			defer resp.Body.Close()
			body, _ := io.ReadAll(io.LimitReader(resp.Body, maxGrobidErrorBytes))
			return nil, fmt.Errorf("grobid failed with status %d: %s", resp.StatusCode, string(body))
		}
		return nil, fmt.Errorf("grobid request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxGrobidErrorBytes))
		return nil, fmt.Errorf("grobid failed with status %d: %s", resp.StatusCode, string(body))
	}

	var tei TEI
	limited := &countingReader{Reader: io.LimitReader(resp.Body, g.maxBytes+1)}
	decoder := xml.NewDecoder(limited)
	if err := decoder.Decode(&tei); err != nil {
		if limited.N > g.maxBytes {
			return nil, fmt.Errorf("GROBID response exceeds maximum size of %d bytes", g.maxBytes)
		}
		return nil, fmt.Errorf("failed to parse XML response: %w", err)
	}
	_, err = io.Copy(io.Discard, limited)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}
	if limited.N > g.maxBytes {
		return nil, fmt.Errorf("GROBID response exceeds maximum size of %d bytes", g.maxBytes)
	}

	logger.Debug().
		Int("divs", len(tei.Text.Body.Divs)).
		Dur("duration", time.Since(start)).
		Msg("PDF parsed with Grobid")

	return &tei, nil
}

type countingReader struct {
	io.Reader
	N int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	r.N += int64(n)
	return n, err
}

func (g *GrobidClient) ExtractText(tei *TEI) string {
	var sb strings.Builder
	for _, div := range tei.Text.Body.Divs {
		if div.Head != "" {
			sb.WriteString(div.Head + "\n")
		}
		for _, p := range div.Ps {
			sb.WriteString(p + "\n")
		}
		sb.WriteString("\n")
	}
	return sb.String()
}

func (g *GrobidClient) ExtractSections(tei *TEI) map[string]string {
	sections := make(map[string]string)
	for _, div := range tei.Text.Body.Divs {
		if div.Head == "" {
			continue
		}

		var content strings.Builder
		for _, p := range div.Ps {
			content.WriteString(p + "\n")
		}
		sections[div.Head] = content.String()
	}
	return sections
}
