package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
	"github.com/research-agent/internal/logger"
)

type Config struct {
	Server   ServerConfig   `koanf:"server"`
	Database DatabaseConfig `koanf:"database"`
	LLM      LLMConfig      `koanf:"llm"`
	APIs     APIsConfig     `koanf:"apis"`
	Pipeline PipelineConfig `koanf:"pipeline"`
	Cache    CacheConfig    `koanf:"cache"`
	Logging  LoggingConfig  `koanf:"logging"`
}

type ServerConfig struct {
	Host         string        `koanf:"host"`
	Port         int           `koanf:"port"`
	ReadTimeout  time.Duration `koanf:"read_timeout"`
	WriteTimeout time.Duration `koanf:"write_timeout"`
}

type DatabaseConfig struct {
	Postgres PostgresConfig `koanf:"postgres"`
	Redis    RedisConfig    `koanf:"redis"`
	Qdrant   QdrantConfig   `koanf:"qdrant"`
}

type PostgresConfig struct {
	Host           string        `koanf:"host"`
	Port           int           `koanf:"port"`
	Database       string        `koanf:"database"`
	User           string        `koanf:"user"`
	Password       string        `koanf:"password"`
	SSLMode        string        `koanf:"sslmode"`
	MaxConnections int           `koanf:"max_connections"`
	MaxIdle        int           `koanf:"max_idle"`
	ConnTimeout    time.Duration `koanf:"conn_timeout"`
}

func (p PostgresConfig) DSN() string {
	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		p.Host, p.Port, p.User, p.Password, p.Database, p.SSLMode,
	)
}

type RedisConfig struct {
	Host     string `koanf:"host"`
	Port     int    `koanf:"port"`
	Password string `koanf:"password"`
	DB       int    `koanf:"db"`
	PoolSize int    `koanf:"pool_size"`
}

func (r RedisConfig) Addr() string {
	return fmt.Sprintf("%s:%d", r.Host, r.Port)
}

type QdrantConfig struct {
	Host       string `koanf:"host"`
	Port       int    `koanf:"port"`
	Collection string `koanf:"collection"`
	APIKey     string `koanf:"api_key"`
}

func (q QdrantConfig) Addr() string {
	return fmt.Sprintf("%s:%d", q.Host, q.Port)
}

type LLMConfig struct {
	Provider          string        `koanf:"provider"`
	APIKey            string        `koanf:"api_key"`
	Model             string        `koanf:"model"`
	EmbeddingModel    string        `koanf:"embedding_model"`
	MaxRetries        int           `koanf:"max_retries"`
	BaseBackoff       time.Duration `koanf:"base_backoff"`
	MaxBackoff        time.Duration `koanf:"max_backoff"`
	Timeout           time.Duration `koanf:"timeout"`
	MaxOutputTokens   int           `koanf:"max_output_tokens"`
	RequestsPerMinute int           `koanf:"requests_per_minute"`
	RequestsPerDay    int           `koanf:"requests_per_day"`
}

type APIsConfig struct {
	SemanticScholar SemanticScholarConfig `koanf:"semantic_scholar"`
	ArXiv           ArXivConfig           `koanf:"arxiv"`
	Unstructured    UnstructuredConfig    `koanf:"unstructured"`
}

type RateLimitConfig struct {
	RequestsPerSecond float64 `koanf:"requests_per_second"`
	Burst             int     `koanf:"burst"`
}

type SemanticScholarConfig struct {
	APIKey    string          `koanf:"api_key"`
	BaseURL   string          `koanf:"base_url"`
	RateLimit RateLimitConfig `koanf:"rate_limit"`
	Timeout   time.Duration   `koanf:"timeout"`
}

type ArXivConfig struct {
	BaseURL   string          `koanf:"base_url"`
	RateLimit RateLimitConfig `koanf:"rate_limit"`
	Timeout   time.Duration   `koanf:"timeout"`
}

type UnstructuredConfig struct {
	BaseURL string        `koanf:"base_url"`
	Timeout time.Duration `koanf:"timeout"`
}

type PipelineConfig struct {
	MaxPapers            int           `koanf:"max_papers"`
	MinPapersForAnalysis int           `koanf:"min_papers_for_analysis"`
	PapersToAnalyze      int           `koanf:"papers_to_analyze"`
	AnalysisDelay        time.Duration `koanf:"analysis_delay"`
	WorkerPoolSize       int           `koanf:"worker_pool_size"`
	JobTimeout           time.Duration `koanf:"job_timeout"`
	PDFDownloadTimeout   time.Duration `koanf:"pdf_download_timeout"`
	UseRedisQueue        bool          `koanf:"use_redis_queue"`
}

type CacheConfig struct {
	DefaultTTL   time.Duration `koanf:"default_ttl"`
	SearchTTL    time.Duration `koanf:"search_ttl"`
	EmbeddingTTL time.Duration `koanf:"embedding_ttl"`
}

type LoggingConfig struct {
	Level  string `koanf:"level"`
	Format string `koanf:"format"`
}

func Load(configPath string) (*Config, error) {
	k := koanf.New(".")

	if err := k.Load(file.Provider(configPath), yaml.Parser()); err != nil {
		return nil, fmt.Errorf("failed to load config file: %w", err)
	}

	if err := k.Load(env.ProviderWithValue("", ".", func(s string, v string) (string, interface{}) {
		key := strings.ToLower(s)
		key = strings.ReplaceAll(key, "__", ".")
		if strings.Contains(v, ",") {
			return key, strings.Split(v, ",")
		}
		return key, v
	}), nil); err != nil {
		return nil, fmt.Errorf("failed to load env vars: %w", err)
	}

	var cfg Config
	if err := k.Unmarshal("", &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &cfg, nil
}

func LoadDefault() (*Config, error) {
	return Load("config/default.yaml")
}

func (c *Config) ApplyLogging() {
	logger.SetLevel(c.Logging.Level)
	if c.Logging.Format == "console" || c.Logging.Format == "development" {
		logger.SetDevelopment()
	} else {
		logger.SetProduction()
	}
	logger.Info().
		Str("level", c.Logging.Level).
		Str("format", c.Logging.Format).
		Msg("Logger initialized")
}
