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
	"time"

	"github.com/research-agent/internal/circuitbreaker"
	"github.com/research-agent/internal/config"
	"github.com/research-agent/internal/logger"
)

const (
	BaseURL = "https://api.semanticscholar.org/graph/v1"
)

type Client struct {
	httpClient     *http.Client
	apiKey         string
	baseURL        string
	rateLimit      *RateLimiter
	circuitBreaker *circuitbreaker.CircuitBreaker
}

func NewClient(cfg config.SemanticScholarConfig) *Client {
	cb := circuitbreaker.New("semantic-scholar", circuitbreaker.Config{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		OpenTimeout:      30 * time.Second,
		OnStateChange: func(name string, from, to circuitbreaker.State) {
			logger.Warn().
				Str("name", name).
				Str("from", from.String()).
				Str("to", to.String()).
				Msg("Circuit breaker state changed")
		},
	})

	return &Client{
		httpClient: &http.Client{
			Timeout: cfg.Timeout,
		},
		apiKey:         cfg.APIKey,
		baseURL:        cfg.BaseURL,
		rateLimit:      NewRateLimiter(cfg.RateLimit.RequestsPerSecond, cfg.RateLimit.Burst),
		circuitBreaker: cb,
	}
}

func (c *Client) doRequest(ctx context.Context, endpoint string, params url.Values) ([]byte, error) {
	var result []byte

	err := c.circuitBreaker.Execute(ctx, func(ctx context.Context) error {
		if err := c.rateLimit.Wait(ctx); err != nil {
			return fmt.Errorf("rate limit wait failed: %w", err)
		}

		fullURL := c.baseURL + endpoint
		if params != nil {
			fullURL = fullURL + "?" + params.Encode()
		}

		req, err := http.NewRequestWithContext(ctx, "GET", fullURL, nil)
		if err != nil {
			return fmt.Errorf("failed to create request: %w", err)
		}

		if c.apiKey != "" {
			req.Header.Set("x-api-key", c.apiKey)
		}
		req.Header.Set("Accept", "application/json")

		start := time.Now()
		resp, err := c.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("request failed: %w", err)
		}
		defer resp.Body.Close()

		logger.Debug().
			Str("endpoint", endpoint).
			Int("status", resp.StatusCode).
			Dur("duration", time.Since(start)).
			Msg("Semantic Scholar API call")

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed to read response: %w", err)
		}

		if resp.StatusCode >= 400 {
			return fmt.Errorf("API error (status %d): %s", resp.StatusCode, string(body))
		}

		result = body
		return nil
	})

	return result, err
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

	err := c.circuitBreaker.Execute(ctx, func(ctx context.Context) error {
		params := url.Values{}
		params.Set("fields", "paperId,title,abstract,year,authors,venue,url,openAccessPdf,citationCount,publicationDate,externalIds")

		reqBody := struct {
			IDs []string `json:"ids"`
		}{IDs: paperIDs}

		jsonBody, err := json.Marshal(reqBody)
		if err != nil {
			return fmt.Errorf("failed to marshal request: %w", err)
		}

		fullURL := c.baseURL + "/paper/batch?" + params.Encode()
		req, err := http.NewRequestWithContext(ctx, "POST", fullURL, strings.NewReader(string(jsonBody)))
		if err != nil {
			return fmt.Errorf("failed to create request: %w", err)
		}

		if c.apiKey != "" {
			req.Header.Set("x-api-key", c.apiKey)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")

		if err := c.rateLimit.Wait(ctx); err != nil {
			return fmt.Errorf("rate limit wait failed: %w", err)
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("request failed: %w", err)
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("failed to read response: %w", err)
		}

		if resp.StatusCode >= 400 {
			return fmt.Errorf("API error (status %d): %s", resp.StatusCode, string(body))
		}

		if err := json.Unmarshal(body, &papers); err != nil {
			return fmt.Errorf("failed to parse response: %w", err)
		}

		return nil
	})

	return papers, err
}
