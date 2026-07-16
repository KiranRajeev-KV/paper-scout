package llm

import (
	"context"
	"fmt"
	"time"

	"github.com/paper-scout/internal/circuitbreaker"
	"github.com/paper-scout/internal/config"
	"github.com/paper-scout/internal/logger"
	"google.golang.org/genai"
)

type Client struct {
	client          *genai.Client
	config          config.LLMConfig
	retry           *RetryPolicy
	model           string
	circuitBreaker  *circuitbreaker.CircuitBreaker
	rateLimiter     quotaWaiter
	generateContent func(context.Context, string, []*genai.Content, *genai.GenerateContentConfig) (*genai.GenerateContentResponse, error)
}

type quotaWaiter interface{ Wait(context.Context) error }

func NewClient(ctx context.Context, cfg config.LLMConfig) (*Client, error) {
	client, err := genai.NewClient(ctx, &genai.ClientConfig{
		APIKey:  cfg.APIKey,
		Backend: genai.BackendGeminiAPI,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create gemini client: %w", err)
	}

	retry := NewRetryPolicy(cfg.MaxRetries, cfg.BaseBackoff, cfg.MaxBackoff)

	appLog := *logger.From(ctx)
	cb := circuitbreaker.New("gemini", circuitbreaker.Config{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		OpenTimeout:      60 * time.Second,
		OnStateChange: func(name string, from, to circuitbreaker.State) {
			appLog.Warn().
				Str("name", name).
				Str("from", from.String()).
				Str("to", to.String()).
				Msg("Circuit breaker state changed")
		},
	})

	var rateLimiter *LLMRateLimiter
	if cfg.RequestsPerMinute > 0 && cfg.RequestsPerDay > 0 {
		rateLimiter = NewLLMRateLimiter(cfg.RequestsPerMinute, cfg.RequestsPerDay)
		logger.From(ctx).Info().
			Int("rpm", cfg.RequestsPerMinute).
			Int("rpd", cfg.RequestsPerDay).
			Msg("LLM rate limiter initialized")
	}

	logger.From(ctx).Info().
		Str("model", cfg.Model).
		Int("max_output_tokens", cfg.MaxOutputTokens).
		Msg("Connected to Gemini LLM")

	result := &Client{
		client:         client,
		config:         cfg,
		retry:          retry,
		model:          cfg.Model,
		circuitBreaker: cb,
		rateLimiter:    rateLimiter,
	}
	result.generateContent = client.Models.GenerateContent
	return result, nil
}

func (c *Client) Close() error {
	return nil
}

func (c *Client) Config() config.LLMConfig {
	return c.config
}

func (c *Client) Provider() string { return "gemini" }

func (c *Client) Model() string { return c.model }

func (c *Client) Health(ctx context.Context) error {
	if c.client == nil || c.model == "" {
		return fmt.Errorf("gemini generator is not configured")
	}
	checkCtx, cancel := context.WithTimeout(ctx, c.config.Timeout)
	defer cancel()
	model, err := c.client.Models.Get(checkCtx, c.model, nil)
	if err != nil {
		return fmt.Errorf("get Gemini model %q: %w", c.model, err)
	}
	if model == nil || model.Name == "" {
		return fmt.Errorf("Gemini model %q returned an empty resource", c.model)
	}
	return nil
}

func (c *Client) GenerateStructured(ctx context.Context, prompt string, schema any) (string, error) {
	responseSchema, err := inferSchema(schema)
	if err != nil {
		return "", fmt.Errorf("build Gemini response schema: %w", err)
	}
	return c.GenerateWithConfig(ctx, prompt, &genai.GenerateContentConfig{
		ResponseMIMEType: "application/json",
		ResponseSchema:   responseSchema,
	})
}

func (c *Client) Generate(ctx context.Context, prompt string) (string, error) {
	var result string
	var usage *TokenUsage

	err := c.circuitBreaker.Execute(ctx, func(ctx context.Context) error {
		return c.retry.Execute(ctx, func(attempt int) error {
			if c.rateLimiter != nil {
				if err := c.rateLimiter.Wait(ctx); err != nil {
					return err
				}
			}
			attemptCtx, cancel := context.WithTimeout(ctx, c.config.Timeout)
			defer cancel()
			parts := []*genai.Part{{Text: prompt}}
			contents := []*genai.Content{{Parts: parts}}

			var genConfig *genai.GenerateContentConfig
			if c.config.MaxOutputTokens > 0 {
				genConfig = &genai.GenerateContentConfig{
					MaxOutputTokens: int32(c.config.MaxOutputTokens),
				}
			}

			logger.From(ctx).Debug().Str("provider", "gemini").Str("model", c.model).Int("attempt", attempt).Msg("Calling generation provider")
			resp, err := c.generateContent(attemptCtx, c.model, contents, genConfig)
			if err != nil {
				return err
			}

			result = resp.Text()
			if resp.UsageMetadata != nil {
				usage = &TokenUsage{
					InputTokens:  int(resp.UsageMetadata.PromptTokenCount),
					OutputTokens: int(resp.UsageMetadata.CandidatesTokenCount),
					TotalTokens:  int(resp.UsageMetadata.TotalTokenCount),
				}
			}
			return nil
		})
	})

	if err != nil {
		return "", err
	}

	if usage != nil {
		logger.From(ctx).Debug().
			Int("input_tokens", usage.InputTokens).
			Int("output_tokens", usage.OutputTokens).
			Msg("LLM generate complete")
	}

	return result, nil
}

func (c *Client) GenerateWithConfig(ctx context.Context, prompt string, cfg *genai.GenerateContentConfig) (string, error) {
	var result string
	var usage *TokenUsage

	err := c.circuitBreaker.Execute(ctx, func(ctx context.Context) error {
		return c.retry.Execute(ctx, func(attempt int) error {
			if c.rateLimiter != nil {
				if err := c.rateLimiter.Wait(ctx); err != nil {
					return err
				}
			}
			attemptCtx, cancel := context.WithTimeout(ctx, c.config.Timeout)
			defer cancel()
			parts := []*genai.Part{{Text: prompt}}
			contents := []*genai.Content{{Parts: parts}}

			requestConfig := *cfg
			if c.config.MaxOutputTokens > 0 && requestConfig.MaxOutputTokens == 0 {
				requestConfig.MaxOutputTokens = int32(c.config.MaxOutputTokens)
			}

			logger.From(ctx).Debug().Str("provider", "gemini").Str("model", c.model).Int("attempt", attempt).Msg("Calling structured generation provider")
			resp, err := c.generateContent(attemptCtx, c.model, contents, &requestConfig)
			if err != nil {
				return err
			}

			result = resp.Text()
			if resp.UsageMetadata != nil {
				usage = &TokenUsage{
					InputTokens:  int(resp.UsageMetadata.PromptTokenCount),
					OutputTokens: int(resp.UsageMetadata.CandidatesTokenCount),
					TotalTokens:  int(resp.UsageMetadata.TotalTokenCount),
				}
			}
			return nil
		})
	})

	if err != nil {
		return "", err
	}

	if usage != nil {
		logger.From(ctx).Debug().
			Int("input_tokens", usage.InputTokens).
			Int("output_tokens", usage.OutputTokens).
			Msg("LLM generate with config complete")
	}

	return result, nil
}

type TokenUsage struct {
	InputTokens  int
	OutputTokens int
	TotalTokens  int
}
