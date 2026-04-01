package pdf

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/paper-scout/internal/logger"
)

type Downloader struct {
	httpClient *http.Client
	timeout    time.Duration
	tmpDir     string
}

func NewDownloader(timeout time.Duration) *Downloader {
	return &Downloader{
		httpClient: &http.Client{
			Timeout: timeout,
		},
		timeout: timeout,
		tmpDir:  os.TempDir(),
	}
}

func (d *Downloader) SetTempDir(dir string) {
	d.tmpDir = dir
}

func (d *Downloader) Download(ctx context.Context, url string) (string, []byte, error) {
	start := time.Now()

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("User-Agent", "Research-Agent/1.0")

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return "", nil, fmt.Errorf("download failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", nil, fmt.Errorf("download failed with status: %d", resp.StatusCode)
	}

	filename := d.extractFilename(url)
	if filename == "" {
		filename = fmt.Sprintf("paper_%d.pdf", time.Now().UnixNano())
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", nil, fmt.Errorf("failed to read response: %w", err)
	}

	logger.Debug().
		Str("url", url).
		Int("bytes", len(data)).
		Dur("duration", time.Since(start)).
		Msg("PDF downloaded")

	return filename, data, nil
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
