package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/env"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/v2"
)

type envValueParser func(string) (interface{}, error)

var environmentValueParsers = map[string]envValueParser{
	"server.allowed_origins": parseAllowedOrigins,
}

var environmentRoots = map[string]struct{}{
	"server":      {},
	"database":    {},
	"generation":  {},
	"embedding":   {},
	"accelerator": {},
	"apis":        {},
	"pipeline":    {},
	"logging":     {},
}

type Config struct {
	Server      ServerConfig      `koanf:"server"`
	Database    DatabaseConfig    `koanf:"database"`
	Generation  GenerationConfig  `koanf:"generation"`
	Embedding   EmbeddingConfig   `koanf:"embedding"`
	Accelerator AcceleratorConfig `koanf:"accelerator"`
	APIs        APIsConfig        `koanf:"apis"`
	Pipeline    PipelineConfig    `koanf:"pipeline"`
	Logging     LoggingConfig     `koanf:"logging"`
}

type ServerConfig struct {
	Host            string        `koanf:"host"`
	Port            int           `koanf:"port"`
	ReadTimeout     time.Duration `koanf:"read_timeout"`
	WriteTimeout    time.Duration `koanf:"write_timeout"`
	AllowedOrigins  []string      `koanf:"allowed_origins"`
	SubmissionRate  float64       `koanf:"submission_rate"`
	SubmissionBurst int           `koanf:"submission_burst"`
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
	Host           string `koanf:"host"`
	Port           int    `koanf:"port"`
	Password       string `koanf:"password"`
	DB             int    `koanf:"db"`
	PoolSize       int    `koanf:"pool_size"`
	WorkerPoolSize int    `koanf:"worker_pool_size"`
}

func (r RedisConfig) Addr() string {
	return fmt.Sprintf("%s:%d", r.Host, r.Port)
}

type QdrantConfig struct {
	Host             string `koanf:"host"`
	Port             int    `koanf:"port"`
	Alias            string `koanf:"alias"`
	CollectionPrefix string `koanf:"collection_prefix"`
	APIKey           string `koanf:"api_key"`
	UseTLS           bool   `koanf:"use_tls"`
}

func (q QdrantConfig) Addr() string {
	return fmt.Sprintf("%s:%d", q.Host, q.Port)
}

type GenerationConfig struct {
	Provider string                 `koanf:"provider"`
	Gemini   GeminiConfig           `koanf:"gemini"`
	Ollama   OllamaGenerationConfig `koanf:"ollama"`
}

type GeminiConfig struct {
	APIKey            string        `koanf:"api_key"`
	Model             string        `koanf:"model"`
	MaxRetries        int           `koanf:"max_retries"`
	BaseBackoff       time.Duration `koanf:"base_backoff"`
	MaxBackoff        time.Duration `koanf:"max_backoff"`
	Timeout           time.Duration `koanf:"timeout"`
	MaxOutputTokens   int           `koanf:"max_output_tokens"`
	RequestsPerMinute int           `koanf:"requests_per_minute"`
	RequestsPerDay    int           `koanf:"requests_per_day"`
}

// LLMConfig remains an internal compatibility alias while Gemini is isolated.
type LLMConfig = GeminiConfig

type OllamaGenerationConfig struct {
	BaseURL         string        `koanf:"base_url"`
	Model           string        `koanf:"model"`
	Timeout         time.Duration `koanf:"timeout"`
	KeepAlive       string        `koanf:"keep_alive"`
	Concurrency     int           `koanf:"concurrency"`
	Think           bool          `koanf:"think"`
	MaxOutputTokens int           `koanf:"max_output_tokens"`
	Temperature     float64       `koanf:"temperature"`
}

type EmbeddingConfig struct {
	Provider           string        `koanf:"provider"`
	BaseURL            string        `koanf:"base_url"`
	Model              string        `koanf:"model"`
	Timeout            time.Duration `koanf:"timeout"`
	KeepAlive          string        `koanf:"keep_alive"`
	Concurrency        int           `koanf:"concurrency"`
	Dimensions         int           `koanf:"dimensions"`
	QueryInstruction   string        `koanf:"query_instruction"`
	InstructionVersion string        `koanf:"instruction_version"`
	IndexingVersion    string        `koanf:"indexing_version"`
}

type AcceleratorConfig struct {
	MaxConcurrent int `koanf:"max_concurrent"`
}

type APIsConfig struct {
	SemanticScholar SemanticScholarConfig `koanf:"semantic_scholar"`
	ArXiv           ArXivConfig           `koanf:"arxiv"`
	Docling         DoclingConfig         `koanf:"docling"`
}

type RateLimitConfig struct {
	RequestsPerSecond float64 `koanf:"requests_per_second"`
	Burst             int     `koanf:"burst"`
}

type ResilienceConfig struct {
	MaxRetries       int           `koanf:"max_retries"`
	BaseBackoff      time.Duration `koanf:"base_backoff"`
	MaxBackoff       time.Duration `koanf:"max_backoff"`
	FailureThreshold int           `koanf:"failure_threshold"`
	OpenTimeout      time.Duration `koanf:"open_timeout"`
}

type SemanticScholarConfig struct {
	APIKey     string           `koanf:"api_key"`
	BaseURL    string           `koanf:"base_url"`
	RateLimit  RateLimitConfig  `koanf:"rate_limit"`
	Resilience ResilienceConfig `koanf:"resilience"`
	Timeout    time.Duration    `koanf:"timeout"`
}

type ArXivConfig struct {
	BaseURL    string           `koanf:"base_url"`
	RateLimit  RateLimitConfig  `koanf:"rate_limit"`
	Resilience ResilienceConfig `koanf:"resilience"`
	Timeout    time.Duration    `koanf:"timeout"`
}

type DoclingConfig struct {
	BaseURL                string        `koanf:"base_url"`
	RequestTimeout         time.Duration `koanf:"request_timeout"`
	DocumentTimeout        time.Duration `koanf:"document_timeout"`
	OCRBehavior            string        `koanf:"ocr_behavior"`
	OutputFormat           string        `koanf:"output_format"`
	Concurrency            int           `koanf:"concurrency"`
	Version                string        `koanf:"version"`
	MaxResponseBytes       int64         `koanf:"max_response_bytes"`
	MinExtractedCharacters int           `koanf:"min_extracted_characters"`
}

type PipelineConfig struct {
	MaxPapers            int              `koanf:"max_papers"`
	MinPapersForAnalysis int              `koanf:"min_papers_for_analysis"`
	PapersToAnalyze      int              `koanf:"papers_to_analyze"`
	WorkerPoolSize       int              `koanf:"worker_pool_size"`
	JobTimeout           time.Duration    `koanf:"job_timeout"`
	PDFDownloadTimeout   time.Duration    `koanf:"pdf_download_timeout"`
	PDFMaxBytes          int64            `koanf:"pdf_max_bytes"`
	PDFRateLimit         RateLimitConfig  `koanf:"pdf_rate_limit"`
	PDFResilience        ResilienceConfig `koanf:"pdf_resilience"`
	ChunkMaxWords        int              `koanf:"chunk_max_words"`
	ChunkOverlap         int              `koanf:"chunk_overlap"`
	MaxRetrievedChunks   int              `koanf:"max_retrieved_chunks"`
	PDFIndexingTimeout   time.Duration    `koanf:"pdf_indexing_timeout"`
	EmbeddingBatchSize   int              `koanf:"embedding_batch_size"`
	UseRedisQueue        bool             `koanf:"use_redis_queue"`
}

type LoggingConfig struct {
	Level     string `koanf:"level"`
	Format    string `koanf:"format"`
	Directory string `koanf:"directory"`
}

func Load(configPath string) (*Config, error) {
	k := koanf.New(".")

	if err := k.Load(file.Provider(configPath), yaml.Parser()); err != nil {
		return nil, fmt.Errorf("failed to load config file: %w", err)
	}

	var bindingErr error
	if err := k.Load(env.ProviderWithValue("", ".", func(s string, v string) (string, interface{}) {
		key := strings.ToLower(strings.ReplaceAll(s, "__", "."))
		root, remainder, found := strings.Cut(key, ".")
		if !found || remainder == "" {
			return "", nil
		}
		if _, ok := environmentRoots[root]; !ok {
			return "", nil
		}

		parser, ok := environmentValueParsers[key]
		if !ok {
			return key, v
		}

		parsed, err := parser(v)
		if err != nil {
			bindingErr = fmt.Errorf("invalid environment variable %s: %w", s, err)
			return "", nil
		}
		return key, parsed
	}), nil); err != nil {
		return nil, fmt.Errorf("failed to load env vars: %w", err)
	}
	if bindingErr != nil {
		return nil, bindingErr
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

func parseAllowedOrigins(value string) (interface{}, error) {
	origins := strings.Split(value, ",")
	for i, origin := range origins {
		origins[i] = strings.TrimSpace(origin)
		if origins[i] == "" {
			return nil, fmt.Errorf("must be a comma-separated list without empty values")
		}
	}
	return origins, nil
}
