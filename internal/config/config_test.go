package config

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

const testConfigPath = "../../config/default.yaml"

// Protects scalar environment values containing commas from being decoded as string slices.
func TestLoadKeepsCommaContainingScalarEnvironmentValues(t *testing.T) {
	instruction := "Retrieve papers about RAG, embeddings, and evaluation."
	t.Setenv("EMBEDDING__QUERY_INSTRUCTION", instruction)

	cfg, err := Load(testConfigPath)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Embedding.QueryInstruction != instruction {
		t.Errorf("QueryInstruction = %q, want %q", cfg.Embedding.QueryInstruction, instruction)
	}
}

// Protects explicit CSV decoding for the allowed-origins collection while preserving scalar decoding.
func TestLoadParsesOnlyRegisteredCollectionEnvironmentValues(t *testing.T) {
	t.Setenv("SERVER__ALLOWED_ORIGINS", " http://one.example,https://two.example ")
	t.Setenv("SERVER__PORT", "9090")
	t.Setenv("DATABASE__QDRANT__USE_TLS", "true")
	t.Setenv("PIPELINE__JOB_TIMEOUT", "42s")
	t.Setenv("UNRELATED__VALUE", "ignored")
	t.Setenv("SERVER", "must not replace the server configuration map")

	cfg, err := Load(testConfigPath)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	wantOrigins := []string{"http://one.example", "https://two.example"}
	if !reflect.DeepEqual(cfg.Server.AllowedOrigins, wantOrigins) {
		t.Errorf("AllowedOrigins = %#v, want %#v", cfg.Server.AllowedOrigins, wantOrigins)
	}
	if cfg.Server.Port != 9090 {
		t.Errorf("Port = %d, want 9090", cfg.Server.Port)
	}
	if !cfg.Database.Qdrant.UseTLS {
		t.Error("UseTLS = false, want true")
	}
	if cfg.Pipeline.JobTimeout != 42*time.Second {
		t.Errorf("JobTimeout = %s, want 42s", cfg.Pipeline.JobTimeout)
	}
}

// Protects configuration validity by rejecting empty values in the allowed-origins CSV setting.
func TestLoadRejectsEmptyAllowedOrigin(t *testing.T) {
	t.Setenv("SERVER__ALLOWED_ORIGINS", "http://one.example,,https://two.example")

	_, err := Load(testConfigPath)
	if err == nil {
		t.Fatal("Load() error = nil, want invalid allowed origins error")
	}
	if !strings.Contains(err.Error(), "SERVER__ALLOWED_ORIGINS") {
		t.Errorf("Load() error = %q, want variable name", err)
	}
}
