package config

import (
	"fmt"

	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/go-ozzo/ozzo-validation/v4/is"
)

func (c *Config) Validate() error {
	return validation.ValidateStruct(c,
		validation.Field(&c.Server, validation.Required),
		validation.Field(&c.Database, validation.Required),
		validation.Field(&c.LLM, validation.Required),
		validation.Field(&c.Pipeline, validation.Required),
		validation.Field(&c.Logging, validation.Required),
	)
}

func (c *Config) ValidateOrPanic() {
	if err := c.Validate(); err != nil {
		panic(fmt.Sprintf("Invalid configuration: %v", err))
	}
}

func (s ServerConfig) Validate() error {
	return validation.ValidateStruct(&s,
		validation.Field(&s.Host, validation.Required),
		validation.Field(&s.Port, validation.Required, validation.Min(1), validation.Max(65535)),
		validation.Field(&s.ReadTimeout, validation.Required),
		validation.Field(&s.WriteTimeout, validation.Required),
	)
}

func (d DatabaseConfig) Validate() error {
	return validation.ValidateStruct(&d,
		validation.Field(&d.Postgres, validation.Required),
		validation.Field(&d.Redis, validation.Required),
		validation.Field(&d.Qdrant, validation.Required),
	)
}

func (p PostgresConfig) Validate() error {
	return validation.ValidateStruct(&p,
		validation.Field(&p.Host, validation.Required),
		validation.Field(&p.Port, validation.Required, validation.Min(1), validation.Max(65535)),
		validation.Field(&p.Database, validation.Required),
		validation.Field(&p.User, validation.Required),
		validation.Field(&p.Password, validation.Required),
		validation.Field(&p.MaxConnections, validation.Required, validation.Min(1)),
		validation.Field(&p.MaxIdle, validation.Required, validation.Min(1)),
	)
}

func (r RedisConfig) Validate() error {
	return validation.ValidateStruct(&r,
		validation.Field(&r.Host, validation.Required),
		validation.Field(&r.Port, validation.Required, validation.Min(1), validation.Max(65535)),
		validation.Field(&r.PoolSize, validation.Required, validation.Min(1)),
	)
}

func (q QdrantConfig) Validate() error {
	return validation.ValidateStruct(&q,
		validation.Field(&q.Host, validation.Required),
		validation.Field(&q.Port, validation.Required, validation.Min(1), validation.Max(65535)),
		validation.Field(&q.Collection, validation.Required),
	)
}

func (l LLMConfig) Validate() error {
	return validation.ValidateStruct(&l,
		validation.Field(&l.Provider, validation.Required),
		validation.Field(&l.APIKey, validation.Required),
		validation.Field(&l.Model, validation.Required),
		validation.Field(&l.EmbeddingModel, validation.Required),
		validation.Field(&l.MaxRetries, validation.Required, validation.Min(0)),
	)
}

func (p PipelineConfig) Validate() error {
	return validation.ValidateStruct(&p,
		validation.Field(&p.MaxPapers, validation.Required, validation.Min(1)),
		validation.Field(&p.MinPapersForAnalysis, validation.Required, validation.Min(1)),
		validation.Field(&p.WorkerPoolSize, validation.Required, validation.Min(1)),
	)
}

func (l LoggingConfig) Validate() error {
	return validation.ValidateStruct(&l,
		validation.Field(&l.Level, validation.Required, validation.In("debug", "info", "warn", "error")),
		validation.Field(&l.Format, validation.Required, validation.In("console", "json", "development", "production")),
	)
}

func (a APIsConfig) Validate() error {
	return validation.ValidateStruct(&a,
		validation.Field(&a.SemanticScholar, validation.Required),
		validation.Field(&a.ArXiv, validation.Required),
		validation.Field(&a.Grobid, validation.Required),
	)
}

func (s SemanticScholarConfig) Validate() error {
	return validation.ValidateStruct(&s,
		validation.Field(&s.BaseURL, validation.Required, is.URL),
		validation.Field(&s.RateLimit, validation.Required),
		validation.Field(&s.Timeout, validation.Required),
	)
}

func (a ArXivConfig) Validate() error {
	return validation.ValidateStruct(&a,
		validation.Field(&a.BaseURL, validation.Required, is.URL),
		validation.Field(&a.RateLimit, validation.Required),
		validation.Field(&a.Timeout, validation.Required),
	)
}

func (g GrobidConfig) Validate() error {
	return validation.ValidateStruct(&g,
		validation.Field(&g.BaseURL, validation.Required),
		validation.Field(&g.Timeout, validation.Required),
	)
}

func (r RateLimitConfig) Validate() error {
	return validation.ValidateStruct(&r,
		validation.Field(&r.RequestsPerSecond, validation.Required, validation.Min(0.1)),
		validation.Field(&r.Burst, validation.Required, validation.Min(1)),
	)
}

func (c CacheConfig) Validate() error {
	return validation.ValidateStruct(&c,
		validation.Field(&c.DefaultTTL, validation.Required),
		validation.Field(&c.SearchTTL, validation.Required),
		validation.Field(&c.EmbeddingTTL, validation.Required),
	)
}
