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

	"github.com/research-agent/internal/config"
	"github.com/research-agent/internal/logger"
)

const (
	BaseURL = "http://export.arxiv.org/api/query"
)

type Client struct {
	httpClient *http.Client
	baseURL    string
	rateLimit  *RateLimiter
}

func NewClient(cfg config.ArXivConfig) *Client {
	return &Client{
		httpClient: &http.Client{
			Timeout: cfg.Timeout,
		},
		baseURL:   cfg.BaseURL,
		rateLimit: NewRateLimiter(cfg.RateLimit.RequestsPerSecond, cfg.RateLimit.Burst),
	}
}

func (c *Client) Search(ctx context.Context, query string, maxResults int) (*Feed, error) {
	if err := c.rateLimit.Wait(ctx); err != nil {
		return nil, fmt.Errorf("rate limit wait failed: %w", err)
	}

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

	start := time.Now()
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	logger.Debug().
		Str("query", query).
		Int("status", resp.StatusCode).
		Dur("duration", time.Since(start)).
		Msg("arXiv API call")

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("API error (status %d): %s", resp.StatusCode, string(body))
	}

	var feed Feed
	if err := xml.Unmarshal(body, &feed); err != nil {
		return nil, fmt.Errorf("failed to parse XML: %w", err)
	}

	return &feed, nil
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
