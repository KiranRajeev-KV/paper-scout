package config

import "testing"

func TestQdrantConfigRequiresTLSForAPIKey(t *testing.T) {
	cfg := QdrantConfig{
		Host:       "qdrant",
		Port:       6334,
		Collection: "papers",
		APIKey:     "secret",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Qdrant config accepted API key without TLS")
	}
}
