package main

import (
	"strings"
	"testing"

	"github.com/paper-scout/internal/storage/postgres"
	"github.com/paper-scout/internal/tools/embedding"
)

// Protects startup from querying an active generation with a different embedding identity.
func TestValidateActiveEmbeddingGenerationRejectsIdentityMismatch(t *testing.T) {
	identity := embedding.Identity{
		Provider: "ollama", Model: "qwen3-embedding:8b", Dimensions: 4096,
		InstructionVersion: "query-v2", IndexingVersion: "index-v2",
	}
	generation := &postgres.EmbeddingGeneration{
		Provider: "ollama", Model: "qwen3-embedding:8b", Dimensions: 4096,
		InstructionVersion: "query-v1", IndexingVersion: "index-v1", CollectionName: "paper_embeddings_generation",
	}
	err := validateActiveEmbeddingGeneration(generation, identity, generation.CollectionName)
	if err == nil || !strings.Contains(err.Error(), "run just reindex") {
		t.Fatalf("validateActiveEmbeddingGeneration() error = %v, want actionable identity mismatch", err)
	}
}

// Protects startup from rejecting a matching active generation and alias target.
func TestValidateActiveEmbeddingGenerationAcceptsExactMatch(t *testing.T) {
	identity := embedding.Identity{
		Provider: "ollama", Model: "qwen3-embedding:8b", Dimensions: 4096,
		InstructionVersion: "query-v1", IndexingVersion: "index-v1",
	}
	generation := &postgres.EmbeddingGeneration{
		Provider: identity.Provider, Model: identity.Model, Dimensions: int32(identity.Dimensions),
		InstructionVersion: identity.InstructionVersion, IndexingVersion: identity.IndexingVersion,
		CollectionName: "paper_embeddings_generation",
	}
	if err := validateActiveEmbeddingGeneration(generation, identity, generation.CollectionName); err != nil {
		t.Fatalf("validateActiveEmbeddingGeneration() error = %v", err)
	}
}
