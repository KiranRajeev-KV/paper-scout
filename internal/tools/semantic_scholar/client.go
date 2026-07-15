package semantic_scholar

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/paper-scout/internal/config"
	"github.com/paper-scout/internal/httpresilience"
	"github.com/paper-scout/internal/logger"
)

const (
	BaseURL = "https://api.semanticscholar.org/graph/v1"
)

type Client struct {
	httpClient *http.Client
	apiKey     string
	baseURL    string
	policy     *httpresilience.Policy
}

func NewClient(cfg config.SemanticScholarConfig) *Client {
	return &Client{
		httpClient: &http.Client{
			Timeout: cfg.Timeout,
		},
		apiKey:  cfg.APIKey,
		baseURL: cfg.BaseURL,
		policy: httpresilience.New("semantic-scholar", httpresilience.Config{
			MaxRetries: cfg.Resilience.MaxRetries, BaseBackoff: cfg.Resilience.BaseBackoff,
			MaxBackoff: cfg.Resilience.MaxBackoff, FailureThreshold: cfg.Resilience.FailureThreshold,
			OpenTimeout: cfg.Resilience.OpenTimeout,
		}, cfg.RateLimit.RequestsPerSecond, cfg.RateLimit.Burst, nil),
	}
}

func (c *Client) doRequest(ctx context.Context, endpoint string, params url.Values) ([]byte, error) {
	resp, err := c.policy.Do(ctx, endpoint, func(ctx context.Context) (*http.Response, error) {
		fullURL := c.baseURL + endpoint
		if params != nil {
			fullURL = fullURL + "?" + params.Encode()
		}

		req, err := http.NewRequestWithContext(ctx, "GET", fullURL, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		if c.apiKey != "" {
			req.Header.Set("x-api-key", c.apiKey)
		}
		req.Header.Set("Accept", "application/json")

		return c.httpClient.Do(req)
	})
	if err != nil && resp == nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	if resp == nil {
		return nil, fmt.Errorf("request returned no response")
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, (16<<20)+1))
	if readErr != nil {
		return nil, fmt.Errorf("failed to read response: %w", readErr)
	}
	if len(body) > 16<<20 {
		return nil, fmt.Errorf("response exceeds %d bytes", 16<<20)
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("API error (status %d): %s", resp.StatusCode, string(body[:min(len(body), 1024)]))
	}
	logger.From(ctx).Debug().Str("endpoint", endpoint).Int("status", resp.StatusCode).Msg("Semantic Scholar API call")
	return body, nil
}

func (c *Client) Search(ctx context.Context, query string, limit, offset int) (*SearchResponse, error) {
	params := url.Values{}
	params.Set("query", query)
	params.Set("limit", strconv.Itoa(limit))
	params.Set("offset", strconv.Itoa(offset))
	params.Set("fields", "paperId,title,abstract,year,authors,venue,url,openAccessPdf,citationCount,publicationDate,externalIds")

	body, err := c.doRequest(ctx, "/paper/search", params)
	if err != nil {
		return nil, err
	}

	var response SearchResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &response, nil
}

func (c *Client) GetPaper(ctx context.Context, paperID string) (*Paper, error) {
	params := url.Values{}
	params.Set("fields", "paperId,title,abstract,year,authors,venue,url,openAccessPdf,citationCount,publicationDate,externalIds,references,citations")

	body, err := c.doRequest(ctx, "/paper/"+paperID, params)
	if err != nil {
		return nil, err
	}

	var paper Paper
	if err := json.Unmarshal(body, &paper); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	return &paper, nil
}

func (c *Client) GetPapersBatch(ctx context.Context, paperIDs []string) ([]Paper, error) {
	if len(paperIDs) == 0 {
		return nil, nil
	}

	var papers []Paper

	resp, err := c.policy.Do(ctx, "paper/batch", func(ctx context.Context) (*http.Response, error) {
		params := url.Values{}
		params.Set("fields", "paperId,title,abstract,year,authors,venue,url,openAccessPdf,citationCount,publicationDate,externalIds")

		reqBody := struct {
			IDs []string `json:"ids"`
		}{IDs: paperIDs}

		jsonBody, err := json.Marshal(reqBody)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request: %w", err)
		}

		fullURL := c.baseURL + "/paper/batch?" + params.Encode()
		req, err := http.NewRequestWithContext(ctx, "POST", fullURL, strings.NewReader(string(jsonBody)))
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		if c.apiKey != "" {
			req.Header.Set("x-api-key", c.apiKey)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")

		return c.httpClient.Do(req)
	})
	if err != nil && resp == nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	if resp == nil {
		return nil, fmt.Errorf("request returned no response")
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, (16<<20)+1))
	if readErr != nil {
		return nil, fmt.Errorf("failed to read response: %w", readErr)
	}
	if len(body) > 16<<20 {
		return nil, fmt.Errorf("response exceeds %d bytes", 16<<20)
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("API error (status %d): %s", resp.StatusCode, string(body[:min(len(body), 1024)]))
	}
	if err := json.Unmarshal(body, &papers); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}
	return papers, nil
}
