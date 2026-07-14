package pdf

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/paper-scout/internal/accelerator"
)

var ErrPartialDoclingResult = errors.New("Docling returned a partial result")

type Document struct {
	Markdown       string
	JSON           json.RawMessage
	Warnings       []string
	UsedOCR        bool
	ProcessingTime time.Duration
}

type DoclingConfig struct {
	BaseURL                string
	RequestTimeout         time.Duration
	DocumentTimeout        time.Duration
	OCRBehavior            string
	OutputFormat           string
	Concurrency            int
	Version                string
	MaxResponseBytes       int64
	MinExtractedCharacters int
	Gate                   *accelerator.Gate
	HTTPClient             *http.Client
}

type DoclingClient struct {
	baseURL                *url.URL
	http                   *http.Client
	requestTimeout         time.Duration
	documentTimeout        time.Duration
	ocrBehavior            string
	outputFormat           string
	version                string
	maxResponseBytes       int64
	minExtractedCharacters int
	permits                chan struct{}
	gate                   *accelerator.Gate
}

func NewDoclingClient(cfg DoclingConfig) (*DoclingClient, error) {
	baseURL, err := url.Parse(strings.TrimRight(cfg.BaseURL, "/"))
	if err != nil || baseURL.Scheme == "" || baseURL.Host == "" {
		return nil, fmt.Errorf("invalid Docling base URL %q", cfg.BaseURL)
	}
	if cfg.RequestTimeout <= 0 || cfg.DocumentTimeout <= 0 || cfg.MaxResponseBytes <= 0 || cfg.MinExtractedCharacters <= 0 {
		return nil, fmt.Errorf("Docling timeouts, response limit, and extraction threshold must be positive")
	}
	if cfg.OutputFormat != "md" {
		return nil, fmt.Errorf("unsupported Docling output format %q", cfg.OutputFormat)
	}
	if cfg.OCRBehavior != "fallback" && cfg.OCRBehavior != "always" && cfg.OCRBehavior != "never" {
		return nil, fmt.Errorf("unsupported Docling OCR behavior %q", cfg.OCRBehavior)
	}
	if cfg.Concurrency < 1 {
		cfg.Concurrency = 1
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{}
	}
	return &DoclingClient{
		baseURL: baseURL, http: cfg.HTTPClient, requestTimeout: cfg.RequestTimeout,
		documentTimeout: cfg.DocumentTimeout, ocrBehavior: cfg.OCRBehavior, outputFormat: cfg.OutputFormat,
		version: cfg.Version, maxResponseBytes: cfg.MaxResponseBytes,
		minExtractedCharacters: cfg.MinExtractedCharacters, permits: make(chan struct{}, cfg.Concurrency), gate: cfg.Gate,
	}, nil
}

func (c *DoclingClient) Provider() string { return "docling" }
func (c *DoclingClient) Version() string  { return c.version }

func (c *DoclingClient) Health(ctx context.Context) error {
	requestCtx, cancel := context.WithTimeout(ctx, minDuration(c.requestTimeout, 5*time.Second))
	defer cancel()
	endpoint := c.baseURL.ResolveReference(&url.URL{Path: "/health"})
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("Docling health request: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("Docling health returned HTTP %d", resp.StatusCode)
	}
	return nil
}

func (c *DoclingClient) Parse(ctx context.Context, filename string, data []byte) (Document, error) {
	if err := c.acquire(ctx); err != nil {
		return Document{}, err
	}
	defer c.release()
	useOCR := c.ocrBehavior == "always"
	document, err := c.convert(ctx, filename, data, useOCR)
	if err != nil {
		return document, err
	}
	if c.ocrBehavior == "fallback" && !usableMarkdown(document.Markdown, c.minExtractedCharacters) {
		document, err = c.convert(ctx, filename, data, true)
		if err != nil {
			return document, fmt.Errorf("Docling OCR fallback: %w", err)
		}
		document.UsedOCR = true
	}
	if !usableMarkdown(document.Markdown, c.minExtractedCharacters) {
		return Document{}, fmt.Errorf("Docling extraction is empty or below %d useful characters", c.minExtractedCharacters)
	}
	return document, nil
}

func (c *DoclingClient) acquire(ctx context.Context) error {
	select {
	case c.permits <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := c.gate.Acquire(ctx); err != nil {
		<-c.permits
		return err
	}
	return nil
}

func (c *DoclingClient) release() { c.gate.Release(); <-c.permits }

func (c *DoclingClient) convert(ctx context.Context, filename string, data []byte, ocr bool) (Document, error) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	part, err := writer.CreateFormFile("files", filename)
	if err != nil {
		return Document{}, fmt.Errorf("create Docling PDF form part: %w", err)
	}
	if _, err := part.Write(data); err != nil {
		return Document{}, fmt.Errorf("write Docling PDF form part: %w", err)
	}
	fields := map[string]string{
		"from_formats": "pdf", "to_formats": c.outputFormat, "do_ocr": strconv.FormatBool(ocr),
		"document_timeout": strconv.FormatFloat(c.documentTimeout.Seconds(), 'f', -1, 64),
	}
	for name, value := range fields {
		if err := writer.WriteField(name, value); err != nil {
			return Document{}, err
		}
	}
	if err := writer.Close(); err != nil {
		return Document{}, err
	}
	requestCtx, cancel := context.WithTimeout(ctx, c.requestTimeout)
	defer cancel()
	endpoint := c.baseURL.ResolveReference(&url.URL{Path: "/v1/convert/file"})
	req, err := http.NewRequestWithContext(requestCtx, http.MethodPost, endpoint.String(), &body)
	if err != nil {
		return Document{}, fmt.Errorf("create Docling request: %w", err)
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())
	resp, err := c.http.Do(req)
	if err != nil {
		return Document{}, fmt.Errorf("Docling conversion request: %w", err)
	}
	defer resp.Body.Close()
	dataResponse, err := io.ReadAll(io.LimitReader(resp.Body, c.maxResponseBytes+1))
	if err != nil {
		return Document{}, fmt.Errorf("read Docling response: %w", err)
	}
	if int64(len(dataResponse)) > c.maxResponseBytes {
		return Document{}, fmt.Errorf("Docling response exceeds %d bytes", c.maxResponseBytes)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Document{}, fmt.Errorf("Docling returned HTTP %d: %s", resp.StatusCode, truncate(string(dataResponse), 4096))
	}
	var result struct {
		Document struct {
			Markdown string          `json:"md_content"`
			JSON     json.RawMessage `json:"json_content"`
		} `json:"document"`
		Status         string            `json:"status"`
		ProcessingTime float64           `json:"processing_time"`
		Errors         []json.RawMessage `json:"errors"`
	}
	if err := json.Unmarshal(dataResponse, &result); err != nil {
		return Document{}, fmt.Errorf("decode Docling response: %w", err)
	}
	warnings := make([]string, 0, len(result.Errors))
	for _, item := range result.Errors {
		warnings = append(warnings, truncate(string(item), 1024))
	}
	document := Document{Markdown: result.Document.Markdown, JSON: result.Document.JSON, Warnings: warnings, UsedOCR: ocr, ProcessingTime: time.Duration(result.ProcessingTime * float64(time.Second))}
	switch result.Status {
	case "success":
		return document, nil
	case "partial_success":
		return document, fmt.Errorf("%w: %s", ErrPartialDoclingResult, strings.Join(warnings, "; "))
	case "skipped", "failure":
		return Document{}, fmt.Errorf("Docling conversion status %q: %s", result.Status, strings.Join(warnings, "; "))
	default:
		return Document{}, fmt.Errorf("unknown Docling conversion status %q", result.Status)
	}
}

func usableMarkdown(markdown string, minimum int) bool {
	useful := 0
	for _, char := range markdown {
		if !strings.ContainsRune(" \t\r\n#*_`-", char) {
			useful++
		}
	}
	return useful >= minimum
}

func truncate(value string, maximum int) string {
	if len(value) <= maximum {
		return value
	}
	return value[:maximum]
}
func minDuration(first, second time.Duration) time.Duration {
	if first < second {
		return first
	}
	return second
}
