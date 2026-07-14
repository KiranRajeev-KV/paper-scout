package embedding

import "testing"

// Protects embedding point id is deterministic and topic scoped.
func TestEmbeddingPointIDIsDeterministicAndTopicScoped(t *testing.T) {
	base := PaperEmbedding{TopicID: "topic-a", PaperID: "paper-a", ChunkType: "pdf", ChunkIndex: 2}
	if got, want := EmbeddingPointID(base), EmbeddingPointID(base); got != want {
		t.Fatalf("point ID changed between equivalent embeddings: %q != %q", got, want)
	}
	otherTopic := base
	otherTopic.TopicID = "topic-b"
	if EmbeddingPointID(base) == EmbeddingPointID(otherTopic) {
		t.Fatal("point ID is not topic scoped")
	}
	otherChunk := base
	otherChunk.ChunkIndex = 3
	if EmbeddingPointID(base) == EmbeddingPointID(otherChunk) {
		t.Fatal("point ID is not chunk scoped")
	}
}
