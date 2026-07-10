package arxiv

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/paper-scout/internal/config"
	"github.com/paper-scout/internal/httpresilience"
	"github.com/paper-scout/internal/logger"
)

const (
	BaseURL = "http://export.arxiv.org/api/query"
)

type Client struct {
	httpClient *http.Client
	baseURL    string
	policy     *httpresilience.Policy
}

func NewClient(cfg config.ArXivConfig) *Client {
	return &Client{
		httpClient: &http.Client{
			Timeout: cfg.Timeout,
		},
		baseURL: cfg.BaseURL,
		policy: httpresilience.New("arxiv", httpresilience.Config{
			MaxRetries: cfg.Resilience.MaxRetries, BaseBackoff: cfg.Resilience.BaseBackoff,
			MaxBackoff: cfg.Resilience.MaxBackoff, FailureThreshold: cfg.Resilience.FailureThreshold,
			OpenTimeout: cfg.Resilience.OpenTimeout,
		}, cfg.RateLimit.RequestsPerSecond, cfg.RateLimit.Burst, nil),
	}
}

func (c *Client) Search(ctx context.Context, query string, maxResults int) (*Feed, error) {
	var feed *Feed
	start := time.Now()

	resp, err := c.policy.Do(ctx, "search", func(ctx context.Context) (*http.Response, error) {
		params := url.Values{}
		params.Set("search_query", query)
		params.Set("start", "0")
		params.Set("max_results", strconv.Itoa(maxResults))
		params.Set("sortBy", "relevance")
		params.Set("sortOrder", "descending")

		fullURL := c.baseURL + "?" + params.Encode()

		req, err := http.NewRequestWithContext(ctx, "GET", fullURL, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		req.Header.Set("Accept", "application/xml")

		return c.httpClient.Do(req)
	})
	if err != nil && resp == nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	if resp == nil {
		return nil, fmt.Errorf("request returned no response")
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return nil, fmt.Errorf("failed to read response: %w", readErr)
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("API error (status %d): %s", resp.StatusCode, string(body))
	}
	logger.Debug().Str("query", query).Int("status", resp.StatusCode).Dur("duration", time.Since(start)).Msg("arXiv API call")
	var f Feed
	if err := xml.Unmarshal(body, &f); err != nil {
		return nil, fmt.Errorf("failed to parse XML: %w", err)
	}
	feed = &f
	return feed, nil
}

func (c *Client) GetByID(ctx context.Context, arxivID string) (*Entry, error) {
	query := "id_list:" + arxivID
	feed, err := c.Search(ctx, query, 1)
	if err != nil {
		return nil, err
	}

	if len(feed.Entries) == 0 {
		return nil, fmt.Errorf("paper not found: %s", arxivID)
	}

	return &feed.Entries[0], nil
}

func BuildQuery(topic string, keywords []string) string {
	var parts []string

	parts = append(parts, "all:"+topic)

	for _, kw := range keywords {
		kw = strings.TrimSpace(kw)
		if kw != "" {
			parts = append(parts, "all:"+kw)
		}
	}

	return strings.Join(parts, " AND ")
}

func ExtractArXivID(idURL string) string {
	parts := strings.Split(idURL, "/")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return idURL
}
