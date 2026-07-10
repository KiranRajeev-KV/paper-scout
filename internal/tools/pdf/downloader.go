package pdf

import (
	"context"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/paper-scout/internal/httpresilience"
	"github.com/paper-scout/internal/logger"
)

const defaultMaxPDFBytes int64 = 50 << 20

type Downloader struct {
	httpClient *http.Client
	timeout    time.Duration
	tmpDir     string
	policy     *httpresilience.Policy
	maxBytes   int64
}

func NewDownloaderWithPolicy(timeout time.Duration, policy *httpresilience.Policy) *Downloader {
	d := NewDownloader(timeout)
	d.policy = policy
	return d
}

func NewDownloaderWithPolicyAndMaxBytes(timeout time.Duration, policy *httpresilience.Policy, maxBytes int64) *Downloader {
	d := NewDownloaderWithMaxBytes(timeout, maxBytes)
	d.policy = policy
	return d
}

func NewDownloader(timeout time.Duration) *Downloader {
	return NewDownloaderWithMaxBytes(timeout, defaultMaxPDFBytes)
}

func NewDownloaderWithMaxBytes(timeout time.Duration, maxBytes int64) *Downloader {
	if maxBytes <= 0 {
		maxBytes = defaultMaxPDFBytes
	}
	return &Downloader{
		httpClient: &http.Client{
			Timeout: timeout,
		},
		timeout:  timeout,
		tmpDir:   os.TempDir(),
		maxBytes: maxBytes,
	}
}

func (d *Downloader) SetHTTPClient(client *http.Client) {
	if client != nil {
		d.httpClient = client
	}
}

func (d *Downloader) SetTempDir(dir string) {
	d.tmpDir = dir
}

func (d *Downloader) Download(ctx context.Context, url string) (string, []byte, error) {
	start := time.Now()

	request := func(ctx context.Context) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}
		req.Header.Set("User-Agent", "Research-Agent/1.0")
		return d.httpClient.Do(req)
	}
	var resp *http.Response
	var err error
	if d.policy != nil {
		resp, err = d.policy.Do(ctx, "download", request)
	} else {
		resp, err = request(ctx)
	}
	if err != nil {
		if resp != nil {
			defer resp.Body.Close()
			return "", nil, fmt.Errorf("download failed with status: %d", resp.StatusCode)
		}
		return "", nil, fmt.Errorf("download failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", nil, fmt.Errorf("download failed with status: %d", resp.StatusCode)
	}
	if err := validatePDFContentType(resp.Header.Get("Content-Type")); err != nil {
		return "", nil, err
	}
	if resp.ContentLength > d.maxBytes {
		return "", nil, fmt.Errorf("PDF response exceeds maximum size of %d bytes", d.maxBytes)
	}

	filename := d.extractFilename(url)
	if filename == "" {
		filename = fmt.Sprintf("paper_%d.pdf", time.Now().UnixNano())
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, d.maxBytes+1))
	if err != nil {
		return "", nil, fmt.Errorf("failed to read response: %w", err)
	}
	if int64(len(data)) > d.maxBytes {
		return "", nil, fmt.Errorf("PDF response exceeds maximum size of %d bytes", d.maxBytes)
	}

	logger.Debug().
		Str("url", url).
		Int("bytes", len(data)).
		Dur("duration", time.Since(start)).
		Msg("PDF downloaded")

	return filename, data, nil
}

func validatePDFContentType(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	mediaType, _, err := mime.ParseMediaType(value)
	if err != nil {
		return fmt.Errorf("invalid PDF content type %q", value)
	}
	if mediaType != "application/pdf" && mediaType != "application/octet-stream" {
		return fmt.Errorf("unexpected PDF content type %q", mediaType)
	}
	return nil
}

func (d *Downloader) DownloadToTemp(ctx context.Context, url string) (string, error) {
	filename, data, err := d.Download(ctx, url)
	if err != nil {
		return "", err
	}

	tmpPath := filepath.Join(d.tmpDir, filename)
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return "", fmt.Errorf("failed to write temp file: %w", err)
	}

	return tmpPath, nil
}

func (d *Downloader) extractFilename(url string) string {
	parts := filepath.SplitList(url)
	if len(parts) > 0 {
		last := parts[len(parts)-1]
		if filepath.Ext(last) == ".pdf" {
			return last
		}
	}

	for i := len(url) - 1; i >= 0; i-- {
		if url[i] == '/' {
			candidate := url[i+1:]
			if filepath.Ext(candidate) == ".pdf" {
				return candidate
			}
			return candidate + ".pdf"
		}
	}

	return ""
}

func (d *Downloader) Cleanup(path string) error {
	return os.Remove(path)
}
