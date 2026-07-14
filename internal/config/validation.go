package config

import (
	"fmt"

	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/go-ozzo/ozzo-validation/v4/is"
)

func (c *Config) Validate() error {
	if err := validation.ValidateStruct(c,
		validation.Field(&c.Server, validation.Required),
		validation.Field(&c.Database, validation.Required),
		validation.Field(&c.Generation, validation.Required),
		validation.Field(&c.Embedding, validation.Required),
		validation.Field(&c.Accelerator, validation.Required),
		validation.Field(&c.APIs, validation.Required),
		validation.Field(&c.Pipeline, validation.Required),
		validation.Field(&c.Logging, validation.Required),
	); err != nil {
		return err
	}
	if c.Pipeline.UseRedisQueue && c.Database.Redis.WorkerPoolSize < c.Pipeline.WorkerPoolSize {
		return fmt.Errorf("database.redis.worker_pool_size must be at least pipeline.worker_pool_size when Redis queue is enabled")
	}
	return nil
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
		validation.Field(&s.AllowedOrigins, validation.Required, validation.Length(1, 20)),
		validation.Field(&s.SubmissionRate, validation.Required, validation.Min(0.01)),
		validation.Field(&s.SubmissionBurst, validation.Required, validation.Min(1)),
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
	if p.MaxIdle > p.MaxConnections {
		return fmt.Errorf("max_idle must not exceed max_connections")
	}
	return validation.ValidateStruct(&p,
		validation.Field(&p.Host, validation.Required),
		validation.Field(&p.Port, validation.Required, validation.Min(1), validation.Max(65535)),
		validation.Field(&p.Database, validation.Required),
		validation.Field(&p.User, validation.Required),
		validation.Field(&p.Password, validation.Required),
		validation.Field(&p.MaxConnections, validation.Required, validation.Min(1)),
		validation.Field(&p.MaxIdle, validation.Required, validation.Min(1)),
		validation.Field(&p.ConnTimeout, validation.Required),
	)
}

func (r RedisConfig) Validate() error {
	return validation.ValidateStruct(&r,
		validation.Field(&r.Host, validation.Required),
		validation.Field(&r.Port, validation.Required, validation.Min(1), validation.Max(65535)),
		validation.Field(&r.PoolSize, validation.Required, validation.Min(1)),
		validation.Field(&r.WorkerPoolSize, validation.Required, validation.Min(1)),
	)
}

func (q QdrantConfig) Validate() error {
	if q.APIKey != "" && !q.UseTLS {
		return fmt.Errorf("qdrant API key requires TLS")
	}
	return validation.ValidateStruct(&q,
		validation.Field(&q.Host, validation.Required),
		validation.Field(&q.Port, validation.Required, validation.Min(1), validation.Max(65535)),
		validation.Field(&q.Alias, validation.Required),
		validation.Field(&q.CollectionPrefix, validation.Required),
	)
}

func (g GenerationConfig) Validate() error {
	if g.Provider != "gemini" && g.Provider != "ollama" {
		return fmt.Errorf("generation provider must be gemini or ollama")
	}
	if g.Provider == "gemini" {
		return g.Gemini.Validate()
	}
	return g.Ollama.Validate()
}

func (l GeminiConfig) Validate() error {
	return validation.ValidateStruct(&l,
		validation.Field(&l.APIKey, validation.Required),
		validation.Field(&l.Model, validation.Required),
		validation.Field(&l.MaxRetries, validation.Min(0)),
		validation.Field(&l.BaseBackoff, validation.Required),
		validation.Field(&l.MaxBackoff, validation.Required),
		validation.Field(&l.Timeout, validation.Required),
		validation.Field(&l.MaxOutputTokens, validation.Required, validation.Min(1)),
		validation.Field(&l.RequestsPerMinute, validation.Required, validation.Min(1)),
		validation.Field(&l.RequestsPerDay, validation.Required, validation.Min(1)),
	)
}

func (o OllamaGenerationConfig) Validate() error {
	return validation.ValidateStruct(&o,
		validation.Field(&o.BaseURL, validation.Required, is.URL),
		validation.Field(&o.Model, validation.Required),
		validation.Field(&o.Timeout, validation.Required),
		validation.Field(&o.Concurrency, validation.Required, validation.Min(1)),
		validation.Field(&o.MaxOutputTokens, validation.Required, validation.Min(1)),
		validation.Field(&o.Temperature, validation.Min(0), validation.Max(2)),
	)
}

func (e EmbeddingConfig) Validate() error {
	if e.Provider != "ollama" {
		return fmt.Errorf("embedding provider must be ollama")
	}
	return validation.ValidateStruct(&e,
		validation.Field(&e.BaseURL, validation.Required, is.URL),
		validation.Field(&e.Model, validation.Required),
		validation.Field(&e.Timeout, validation.Required),
		validation.Field(&e.Concurrency, validation.Required, validation.Min(1)),
		validation.Field(&e.Dimensions, validation.Required, validation.Min(1)),
		validation.Field(&e.QueryInstruction, validation.Required),
		validation.Field(&e.InstructionVersion, validation.Required),
		validation.Field(&e.IndexingVersion, validation.Required),
	)
}

func (a AcceleratorConfig) Validate() error {
	return validation.ValidateStruct(&a, validation.Field(&a.MaxConcurrent, validation.Required, validation.Min(1)))
}

func (p PipelineConfig) Validate() error {
	if p.MinPapersForAnalysis > p.MaxPapers {
		return fmt.Errorf("min_papers_for_analysis must not exceed max_papers")
	}
	if p.PapersToAnalyze > p.MaxPapers {
		return fmt.Errorf("papers_to_analyze must not exceed max_papers")
	}
	if p.ChunkOverlap >= p.ChunkMaxWords && p.ChunkMaxWords > 0 {
		return fmt.Errorf("chunk_overlap must be smaller than chunk_max_words")
	}
	return validation.ValidateStruct(&p,
		validation.Field(&p.MaxPapers, validation.Required, validation.Min(1)),
		validation.Field(&p.MinPapersForAnalysis, validation.Required, validation.Min(1)),
		validation.Field(&p.PapersToAnalyze, validation.Required, validation.Min(1)),
		validation.Field(&p.WorkerPoolSize, validation.Required, validation.Min(1)),
		validation.Field(&p.JobTimeout, validation.Required),
		validation.Field(&p.PDFDownloadTimeout, validation.Required),
		validation.Field(&p.PDFMaxBytes, validation.Required, validation.Min(1)),
		validation.Field(&p.PDFRateLimit, validation.Required),
		validation.Field(&p.PDFResilience, validation.Required),
		validation.Field(&p.ChunkMaxWords, validation.Required, validation.Min(1)),
		validation.Field(&p.ChunkOverlap, validation.Min(0)),
		validation.Field(&p.MaxRetrievedChunks, validation.Required, validation.Min(1)),
		validation.Field(&p.PDFIndexingTimeout, validation.Required),
		validation.Field(&p.EmbeddingBatchSize, validation.Required, validation.Min(1)),
	)
}

func (l LoggingConfig) Validate() error {
	return validation.ValidateStruct(&l,
		validation.Field(&l.Level, validation.Required, validation.In("debug", "info", "warn", "error")),
		validation.Field(&l.Format, validation.Required, validation.In("console", "json", "development", "production")),
		validation.Field(&l.Directory, validation.Required),
	)
}

func (a APIsConfig) Validate() error {
	return validation.ValidateStruct(&a,
		validation.Field(&a.SemanticScholar, validation.Required),
		validation.Field(&a.ArXiv, validation.Required),
		validation.Field(&a.Docling, validation.Required),
	)
}

func (s SemanticScholarConfig) Validate() error {
	return validation.ValidateStruct(&s,
		validation.Field(&s.BaseURL, validation.Required, is.URL),
		validation.Field(&s.RateLimit, validation.Required),
		validation.Field(&s.Timeout, validation.Required),
		validation.Field(&s.Resilience, validation.Required),
	)
}

func (a ArXivConfig) Validate() error {
	return validation.ValidateStruct(&a,
		validation.Field(&a.BaseURL, validation.Required, is.URL),
		validation.Field(&a.RateLimit, validation.Required),
		validation.Field(&a.Timeout, validation.Required),
		validation.Field(&a.Resilience, validation.Required),
	)
}

func (d DoclingConfig) Validate() error {
	if d.DocumentTimeout > d.RequestTimeout {
		return fmt.Errorf("Docling document_timeout must not exceed request_timeout")
	}
	return validation.ValidateStruct(&d,
		validation.Field(&d.BaseURL, validation.Required, is.URL),
		validation.Field(&d.RequestTimeout, validation.Required),
		validation.Field(&d.DocumentTimeout, validation.Required),
		validation.Field(&d.OCRBehavior, validation.Required, validation.In("fallback", "always", "never")),
		validation.Field(&d.OutputFormat, validation.Required, validation.In("md")),
		validation.Field(&d.Concurrency, validation.Required, validation.Min(1)),
		validation.Field(&d.Version, validation.Required),
		validation.Field(&d.MaxResponseBytes, validation.Required, validation.Min(1)),
		validation.Field(&d.MinExtractedCharacters, validation.Required, validation.Min(1)),
	)
}

func (r ResilienceConfig) Validate() error {
	return validation.ValidateStruct(&r,
		validation.Field(&r.MaxRetries, validation.Min(0)),
		validation.Field(&r.BaseBackoff, validation.Required),
		validation.Field(&r.MaxBackoff, validation.Required),
		validation.Field(&r.FailureThreshold, validation.Min(1)),
		validation.Field(&r.OpenTimeout, validation.Required),
	)
}

func (r RateLimitConfig) Validate() error {
	return validation.ValidateStruct(&r,
		validation.Field(&r.RequestsPerSecond, validation.Required, validation.Min(0.1)),
		validation.Field(&r.Burst, validation.Required, validation.Min(1)),
	)
}
