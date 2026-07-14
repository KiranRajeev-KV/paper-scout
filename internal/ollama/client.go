// Package ollama implements local generation and embedding through Ollama's native API.
package ollama

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"time"

	"github.com/paper-scout/internal/accelerator"
	"github.com/paper-scout/internal/tools/embedding"
)

const defaultMaxResponseBytes = 64 << 20

var ErrModelUnavailable = errors.New("Ollama model is not installed")

type client struct {
	baseURL          *url.URL
	http             *http.Client
	timeout          time.Duration
	keepAlive        string
	maxResponseBytes int64
	permits          chan struct{}
	accelerator      *accelerator.Gate
}

func newClient(baseURL string, timeout time.Duration, keepAlive string, concurrency int, gate *accelerator.Gate, httpClient *http.Client) (*client, error) {
	parsed, err := url.Parse(strings.TrimRight(baseURL, "/"))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("invalid Ollama base URL %q", baseURL)
	}
	if timeout <= 0 {
		return nil, fmt.Errorf("Ollama timeout must be positive")
	}
	if concurrency < 1 {
		concurrency = 1
	}
	if httpClient == nil {
		httpClient = &http.Client{}
	}
	return &client{baseURL: parsed, http: httpClient, timeout: timeout, keepAlive: keepAlive, maxResponseBytes: defaultMaxResponseBytes, permits: make(chan struct{}, concurrency), accelerator: gate}, nil
}

func (c *client) acquire(ctx context.Context) error {
	select {
	case c.permits <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := c.accelerator.Acquire(ctx); err != nil {
		<-c.permits
		return err
	}
	return nil
}

func (c *client) release() {
	c.accelerator.Release()
	<-c.permits
}

func (c *client) doJSON(ctx context.Context, method, path string, requestBody, responseBody any) error {
	requestCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	var body io.Reader
	if requestBody != nil {
		data, err := json.Marshal(requestBody)
		if err != nil {
			return fmt.Errorf("encode Ollama request: %w", err)
		}
		body = bytes.NewReader(data)
	}
	endpoint := c.baseURL.ResolveReference(&url.URL{Path: path})
	req, err := http.NewRequestWithContext(requestCtx, method, endpoint.String(), body)
	if err != nil {
		return fmt.Errorf("create Ollama request: %w", err)
	}
	if requestBody != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("Ollama %s: %w", path, err)
	}
	defer resp.Body.Close()
	limited := io.LimitReader(resp.Body, c.maxResponseBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return fmt.Errorf("read Ollama %s response: %w", path, err)
	}
	if int64(len(data)) > c.maxResponseBytes {
		return fmt.Errorf("Ollama %s response exceeds %d bytes", path, c.maxResponseBytes)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var apiErr struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(data, &apiErr)
		if apiErr.Error == "" {
			apiErr.Error = http.StatusText(resp.StatusCode)
		}
		return fmt.Errorf("Ollama %s returned HTTP %d: %s", path, resp.StatusCode, apiErr.Error)
	}
	if responseBody != nil {
		if err := json.Unmarshal(data, responseBody); err != nil {
			return fmt.Errorf("decode Ollama %s response: %w", path, err)
		}
	}
	return nil
}

func (c *client) health(ctx context.Context, model string) error {
	var response struct {
		Models []struct {
			Name  string `json:"name"`
			Model string `json:"model"`
		} `json:"models"`
	}
	if err := c.doJSON(ctx, http.MethodGet, "/api/tags", nil, &response); err != nil {
		return err
	}
	for _, installed := range response.Models {
		if modelMatches(model, installed.Name) || modelMatches(model, installed.Model) {
			return nil
		}
	}
	return fmt.Errorf("%w: %s; install it with `ollama pull %s`", ErrModelUnavailable, model, model)
}

func modelMatches(configured, installed string) bool {
	if configured == installed {
		return true
	}
	return !strings.Contains(configured, ":") && configured+":latest" == installed
}

// GenerationConfig configures the native Ollama chat endpoint.
type GenerationConfig struct {
	BaseURL         string
	Model           string
	Timeout         time.Duration
	KeepAlive       string
	Concurrency     int
	Think           bool
	MaxOutputTokens int
	Temperature     float64
	Gate            *accelerator.Gate
	HTTPClient      *http.Client
}

// Generator implements schema-constrained and free-form local generation.
type Generator struct {
	client          *client
	model           string
	think           bool
	maxOutputTokens int
	temperature     float64
}

func NewGenerator(cfg GenerationConfig) (*Generator, error) {
	if strings.TrimSpace(cfg.Model) == "" {
		return nil, fmt.Errorf("Ollama generation model is required")
	}
	client, err := newClient(cfg.BaseURL, cfg.Timeout, cfg.KeepAlive, cfg.Concurrency, cfg.Gate, cfg.HTTPClient)
	if err != nil {
		return nil, err
	}
	if cfg.MaxOutputTokens < 1 {
		return nil, fmt.Errorf("Ollama max output tokens must be positive")
	}
	if cfg.Temperature < 0 || cfg.Temperature > 2 {
		return nil, fmt.Errorf("Ollama temperature must be between 0 and 2")
	}
	return &Generator{client: client, model: cfg.Model, think: cfg.Think, maxOutputTokens: cfg.MaxOutputTokens, temperature: cfg.Temperature}, nil
}

func (g *Generator) Provider() string                 { return "ollama" }
func (g *Generator) Model() string                    { return g.model }
func (g *Generator) Health(ctx context.Context) error { return g.client.health(ctx, g.model) }

func (g *Generator) Generate(ctx context.Context, prompt string) (string, error) {
	return g.generate(ctx, prompt, nil)
}

func (g *Generator) GenerateStructured(ctx context.Context, prompt string, schema any) (string, error) {
	format, err := jsonSchema(schema)
	if err != nil {
		return "", fmt.Errorf("build Ollama response schema: %w", err)
	}
	schemaJSON, err := json.Marshal(format)
	if err != nil {
		return "", fmt.Errorf("marshal Ollama response schema: %w", err)
	}
	prompt = fmt.Sprintf("%s\n\nReturn exactly one JSON value matching this JSON Schema. Do not use Markdown fences or add any text outside the JSON value:\n%s", prompt, schemaJSON)
	return g.generate(ctx, prompt, format)
}

func (g *Generator) generate(ctx context.Context, prompt string, format any) (string, error) {
	if err := g.client.acquire(ctx); err != nil {
		return "", err
	}
	defer g.client.release()
	request := map[string]any{
		"model": g.model, "messages": []map[string]string{{"role": "user", "content": prompt}},
		"stream": false, "think": g.think,
		"options": map[string]any{"num_predict": g.maxOutputTokens, "temperature": g.temperature},
	}
	if format != nil {
		request["format"] = format
	}
	if g.client.keepAlive != "" {
		request["keep_alive"] = g.client.keepAlive
	}
	var response struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	}
	if err := g.client.doJSON(ctx, http.MethodPost, "/api/chat", request, &response); err != nil {
		return "", err
	}
	if strings.TrimSpace(response.Message.Content) == "" {
		return "", fmt.Errorf("Ollama returned an empty generation")
	}
	content := response.Message.Content
	if format != nil {
		var ok bool
		content, ok = normalizeStructuredJSON(content)
		if !ok {
			return "", fmt.Errorf("Ollama returned invalid structured JSON")
		}
	}
	return content, nil
}

// normalizeStructuredJSON accepts raw JSON or exactly one JSON Markdown fence.
func normalizeStructuredJSON(content string) (string, bool) {
	trimmed := strings.TrimSpace(content)
	if json.Valid([]byte(trimmed)) {
		return trimmed, true
	}

	const opening = "```json\n"
	if !strings.HasPrefix(strings.ToLower(trimmed), opening) || !strings.HasSuffix(trimmed, "```") {
		return "", false
	}
	body := strings.TrimSpace(trimmed[len(opening) : len(trimmed)-len("```")])
	if strings.Contains(body, "```") || !json.Valid([]byte(body)) {
		return "", false
	}
	return body, true
}

// EmbeddingConfig configures Qwen or another Ollama embedding model.
type EmbeddingConfig struct {
	BaseURL            string
	Model              string
	Timeout            time.Duration
	KeepAlive          string
	Concurrency        int
	Dimensions         int
	QueryInstruction   string
	InstructionVersion string
	IndexingVersion    string
	Gate               *accelerator.Gate
	HTTPClient         *http.Client
}

// Embedder implements Ollama's batch embedding endpoint.
type Embedder struct {
	client           *client
	identity         embedding.Identity
	queryInstruction string
}

func NewEmbedder(cfg EmbeddingConfig) (*Embedder, error) {
	if strings.TrimSpace(cfg.Model) == "" || cfg.Dimensions < 1 {
		return nil, fmt.Errorf("Ollama embedding model and positive dimensions are required")
	}
	if strings.TrimSpace(cfg.QueryInstruction) == "" || strings.TrimSpace(cfg.InstructionVersion) == "" || strings.TrimSpace(cfg.IndexingVersion) == "" {
		return nil, fmt.Errorf("Ollama query instruction, instruction version, and indexing version are required")
	}
	client, err := newClient(cfg.BaseURL, cfg.Timeout, cfg.KeepAlive, cfg.Concurrency, cfg.Gate, cfg.HTTPClient)
	if err != nil {
		return nil, err
	}
	return &Embedder{client: client, identity: embedding.Identity{Provider: "ollama", Model: cfg.Model, Dimensions: cfg.Dimensions, InstructionVersion: cfg.InstructionVersion, IndexingVersion: cfg.IndexingVersion}, queryInstruction: cfg.QueryInstruction}, nil
}

func (e *Embedder) Identity() embedding.Identity     { return e.identity }
func (e *Embedder) Dimensions() int                  { return e.identity.Dimensions }
func (e *Embedder) Health(ctx context.Context) error { return e.client.health(ctx, e.identity.Model) }

func (e *Embedder) EmbedDocuments(ctx context.Context, texts []string) ([][]float32, error) {
	return e.embed(ctx, texts)
}

func (e *Embedder) EmbedQuery(ctx context.Context, text string) ([]float32, error) {
	vectors, err := e.embed(ctx, []string{fmt.Sprintf("Instruct: %s\nQuery:%s", e.queryInstruction, text)})
	if err != nil {
		return nil, err
	}
	return vectors[0], nil
}

func (e *Embedder) embed(ctx context.Context, texts []string) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, fmt.Errorf("Ollama embedding input is empty")
	}
	if err := e.client.acquire(ctx); err != nil {
		return nil, err
	}
	defer e.client.release()
	request := map[string]any{"model": e.identity.Model, "input": texts, "dimensions": e.identity.Dimensions, "truncate": false}
	if e.client.keepAlive != "" {
		request["keep_alive"] = e.client.keepAlive
	}
	var response struct {
		Embeddings [][]float32 `json:"embeddings"`
	}
	if err := e.client.doJSON(ctx, http.MethodPost, "/api/embed", request, &response); err != nil {
		return nil, err
	}
	if err := embedding.ValidateVectors(response.Embeddings, len(texts), e.identity.Dimensions); err != nil {
		return nil, fmt.Errorf("invalid Ollama embedding response: %w", err)
	}
	return response.Embeddings, nil
}

func jsonSchema(example any) (map[string]any, error) {
	return schemaValue(reflect.ValueOf(example))
}

func schemaValue(value reflect.Value) (map[string]any, error) {
	if !value.IsValid() {
		return map[string]any{"type": "string"}, nil
	}
	for value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface {
		if value.IsNil() {
			return map[string]any{"type": "string"}, nil
		}
		value = value.Elem()
	}
	switch value.Kind() {
	case reflect.String:
		return map[string]any{"type": "string"}, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return map[string]any{"type": "integer"}, nil
	case reflect.Float32, reflect.Float64:
		return map[string]any{"type": "number"}, nil
	case reflect.Bool:
		return map[string]any{"type": "boolean"}, nil
	case reflect.Slice, reflect.Array:
		item := map[string]any{"type": "string"}
		if value.Len() > 0 {
			var err error
			item, err = schemaValue(value.Index(0))
			if err != nil {
				return nil, err
			}
		}
		return map[string]any{"type": "array", "items": item}, nil
	case reflect.Map:
		if value.Type().Key().Kind() != reflect.String {
			return nil, fmt.Errorf("schema map keys must be strings")
		}
		properties := make(map[string]any, value.Len())
		iter := value.MapRange()
		for iter.Next() {
			child, err := schemaValue(iter.Value())
			if err != nil {
				return nil, err
			}
			properties[iter.Key().String()] = child
		}
		return map[string]any{"type": "object", "properties": properties, "required": mapKeys(properties)}, nil
	case reflect.Struct:
		properties := make(map[string]any)
		for i := 0; i < value.NumField(); i++ {
			field := value.Type().Field(i)
			if field.PkgPath != "" {
				continue
			}
			name := strings.Split(field.Tag.Get("json"), ",")[0]
			if name == "-" {
				continue
			}
			if name == "" {
				name = field.Name
			}
			child, err := schemaValue(value.Field(i))
			if err != nil {
				return nil, err
			}
			properties[name] = child
		}
		return map[string]any{"type": "object", "properties": properties, "required": mapKeys(properties)}, nil
	default:
		return nil, fmt.Errorf("unsupported schema type %s", value.Kind())
	}
}

func mapKeys(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}
