package config

import "testing"

// Protects qdrant config requires tls for api key.
func TestQdrantConfigRequiresTLSForAPIKey(t *testing.T) {
	cfg := QdrantConfig{
		Host:             "qdrant",
		Port:             6334,
		Alias:            "papers",
		CollectionPrefix: "papers",
		APIKey:           "secret",
	}
	if err := cfg.Validate(); err == nil {
		t.Fatal("Qdrant config accepted API key without TLS")
	}
}
