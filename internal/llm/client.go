package llm

import (
	"context"
	"fmt"
	"time"

	"github.com/research-agent/internal/circuitbreaker"
	"github.com/research-agent/internal/config"
	"github.com/research-agent/internal/logger"
	"google.golang.org/genai"
)

type Client struct {
	client         *genai.Client
	config         config.LLMConfig
	retry          *RetryPolicy
	model          string
	embModel       string
	circuitBreaker *circuitbreaker.CircuitBreaker
	rateLimiter    *LLMRateLimiter
}

func NewClient(ctx context.Context, cfg config.LLMConfig) (*Client, error) {
	client, err := genai.NewClient(ctx, &genai.ClientConfig{
		APIKey:  cfg.APIKey,
		Backend: genai.BackendGeminiAPI,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create gemini client: %w", err)
	}

	retry := NewRetryPolicy(cfg.MaxRetries, cfg.BaseBackoff, cfg.MaxBackoff)

	cb := circuitbreaker.New("gemini", circuitbreaker.Config{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		OpenTimeout:      60 * time.Second,
		OnStateChange: func(name string, from, to circuitbreaker.State) {
			logger.Warn().
				Str("name", name).
				Str("from", from.String()).
				Str("to", to.String()).
				Msg("Circuit breaker state changed")
		},
	})

	var rateLimiter *LLMRateLimiter
	if cfg.RequestsPerMinute > 0 && cfg.RequestsPerDay > 0 {
		rateLimiter = NewLLMRateLimiter(cfg.RequestsPerMinute, cfg.RequestsPerDay)
		logger.Info().
			Int("rpm", cfg.RequestsPerMinute).
			Int("rpd", cfg.RequestsPerDay).
			Msg("LLM rate limiter initialized")
	}

	logger.Info().
		Str("model", cfg.Model).
		Str("embedding_model", cfg.EmbeddingModel).
		Int("max_output_tokens", cfg.MaxOutputTokens).
		Msg("Connected to Gemini LLM")

	return &Client{
		client:         client,
		config:         cfg,
		retry:          retry,
		model:          cfg.Model,
		embModel:       cfg.EmbeddingModel,
		circuitBreaker: cb,
		rateLimiter:    rateLimiter,
	}, nil
}

func (c *Client) Close() error {
	return nil
}

func (c *Client) Config() config.LLMConfig {
	return c.config
}

func (c *Client) Generate(ctx context.Context, prompt string) (string, error) {
	var result string
	var usage *TokenUsage

	err := c.circuitBreaker.Execute(ctx, func(ctx context.Context) error {
		if c.rateLimiter != nil {
			if err := c.rateLimiter.Wait(ctx); err != nil {
				return err
			}
		}

		return c.retry.Execute(ctx, func() error {
			parts := []*genai.Part{{Text: prompt}}
			contents := []*genai.Content{{Parts: parts}}

			var genConfig *genai.GenerateContentConfig
			if c.config.MaxOutputTokens > 0 {
				genConfig = &genai.GenerateContentConfig{
					MaxOutputTokens: int32(c.config.MaxOutputTokens),
				}
			}

			resp, err := c.client.Models.GenerateContent(ctx, c.model, contents, genConfig)
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
		logger.Debug().
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
		if c.rateLimiter != nil {
			if err := c.rateLimiter.Wait(ctx); err != nil {
				return err
			}
		}

		return c.retry.Execute(ctx, func() error {
			parts := []*genai.Part{{Text: prompt}}
			contents := []*genai.Content{{Parts: parts}}

			if c.config.MaxOutputTokens > 0 && cfg.MaxOutputTokens == 0 {
				cfg.MaxOutputTokens = int32(c.config.MaxOutputTokens)
			}

			resp, err := c.client.Models.GenerateContent(ctx, c.model, contents, cfg)
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
		logger.Debug().
			Int("input_tokens", usage.InputTokens).
			Int("output_tokens", usage.OutputTokens).
			Msg("LLM generate with config complete")
	}

	return result, nil
}

func (c *Client) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	var embeddings [][]float32

	err := c.circuitBreaker.Execute(ctx, func(ctx context.Context) error {
		if c.rateLimiter != nil {
			if err := c.rateLimiter.Wait(ctx); err != nil {
				return err
			}
		}

		return c.retry.Execute(ctx, func() error {
			contents := make([]*genai.Content, len(texts))
			for i, text := range texts {
				contents[i] = &genai.Content{
					Parts: []*genai.Part{{Text: text}},
				}
			}

			result, err := c.client.Models.EmbedContent(ctx, c.embModel, contents, nil)
			if err != nil {
				return err
			}

			embeddings = make([][]float32, len(result.Embeddings))
			for i, emb := range result.Embeddings {
				embeddings[i] = emb.Values
			}
			return nil
		})
	})

	if err != nil {
		return nil, err
	}

	logger.Debug().
		Int("count", len(embeddings)).
		Msg("Embeddings generated")

	return embeddings, nil
}

func (c *Client) EmbedSingle(ctx context.Context, text string) ([]float32, error) {
	embeddings, err := c.Embed(ctx, []string{text})
	if err != nil {
		return nil, err
	}
	if len(embeddings) == 0 {
		return nil, fmt.Errorf("no embedding returned")
	}
	return embeddings[0], nil
}

type TokenUsage struct {
	InputTokens  int
	OutputTokens int
	TotalTokens  int
}
