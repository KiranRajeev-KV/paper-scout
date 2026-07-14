// Package embedding provides application-oriented embedding and vector-search operations.
package embedding

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"strings"

	"github.com/google/uuid"
	"github.com/paper-scout/internal/logger"
	qdrantstore "github.com/paper-scout/internal/storage/qdrant"
	"github.com/qdrant/go-client/qdrant"
)

// Identity distinguishes incompatible embedding generations.
type Identity struct {
	Provider           string
	Model              string
	Dimensions         int
	InstructionVersion string
	IndexingVersion    string
}

func (i Identity) String() string {
	return fmt.Sprintf("%s:%s:%d:%s:%s", i.Provider, i.Model, i.Dimensions, i.InstructionVersion, i.IndexingVersion)
}

// CollectionSuffix returns a stable, filesystem-safe identity suffix.
func (i Identity) CollectionSuffix() string {
	readable := strings.NewReplacer("/", "_", ":", "_", ".", "_", "-", "_").Replace(strings.ToLower(i.Provider + "_" + i.Model))
	readable = strings.Trim(readable, "_")
	sum := sha256.Sum256([]byte(i.String()))
	return fmt.Sprintf("%s_%d_%s", readable, i.Dimensions, hex.EncodeToString(sum[:4]))
}

// Embedder creates document and query vectors without owning vector persistence.
type Embedder interface {
	EmbedDocuments(ctx context.Context, texts []string) ([][]float32, error)
	EmbedQuery(ctx context.Context, text string) ([]float32, error)
	Dimensions() int
	Identity() Identity
	Health(ctx context.Context) error
}

// Generator coordinates an embedding provider with the active Qdrant collection.
type Generator struct {
	embedder Embedder
	qdrant   *qdrantstore.Client
}

func NewGenerator(embedder Embedder, qdrantClient *qdrantstore.Client) *Generator {
	return &Generator{embedder: embedder, qdrant: qdrantClient}
}

func (g *Generator) Generate(ctx context.Context, text string) ([]float32, error) {
	vector, err := g.embedder.EmbedQuery(ctx, text)
	if err != nil {
		return nil, err
	}
	if err := ValidateVectors([][]float32{vector}, 1, g.embedder.Dimensions()); err != nil {
		return nil, err
	}
	return vector, nil
}

func (g *Generator) GenerateBatch(ctx context.Context, texts []string) ([][]float32, error) {
	vectors, err := g.embedder.EmbedDocuments(ctx, texts)
	if err != nil {
		return nil, err
	}
	if err := ValidateVectors(vectors, len(texts), g.embedder.Dimensions()); err != nil {
		return nil, err
	}
	return vectors, nil
}

func (g *Generator) Identity() Identity { return g.embedder.Identity() }

func (g *Generator) Health(ctx context.Context) error { return g.embedder.Health(ctx) }

func (g *Generator) CollectionName() string { return g.qdrant.PhysicalCollectionName() }

type PaperEmbedding struct {
	ChunkID        string
	PaperID        string
	TopicID        string
	ChunkType      string
	ChunkIndex     int
	Text           string
	ContentHash    string
	SectionHeading string
	Identity       Identity
	Vector         []float32
}

func (g *Generator) StoreEmbedding(ctx context.Context, emb PaperEmbedding) error {
	return g.StoreEmbeddings(ctx, []PaperEmbedding{emb})
}

func (g *Generator) StoreEmbeddings(ctx context.Context, embeddings []PaperEmbedding) error {
	if len(embeddings) == 0 {
		return nil
	}
	identity := g.embedder.Identity()
	points := make([]*qdrant.PointStruct, 0, len(embeddings))
	for index := range embeddings {
		emb := &embeddings[index]
		if emb.Identity == (Identity{}) {
			emb.Identity = identity
		}
		if emb.Identity != identity {
			return fmt.Errorf("embedding identity mismatch: got %s want %s", emb.Identity.String(), identity.String())
		}
		if emb.ContentHash == "" {
			sum := sha256.Sum256([]byte(emb.Text))
			emb.ContentHash = hex.EncodeToString(sum[:])
		}
		if err := ValidateVectors([][]float32{emb.Vector}, 1, identity.Dimensions); err != nil {
			return fmt.Errorf("validate vector for paper %s chunk %d: %w", emb.PaperID, emb.ChunkIndex, err)
		}
		points = append(points, &qdrant.PointStruct{
			Id:      qdrant.NewIDUUID(EmbeddingPointID(*emb)),
			Vectors: qdrant.NewVectors(emb.Vector...),
			Payload: qdrant.NewValueMap(map[string]any{
				"chunk_id": emb.ChunkID, "paper_id": emb.PaperID, "topic_id": emb.TopicID,
				"chunk_type": emb.ChunkType, "chunk_index": int64(emb.ChunkIndex), "text": emb.Text,
				"section_heading": emb.SectionHeading,
				"content_hash":    emb.ContentHash, "embedding_provider": identity.Provider,
				"embedding_model": identity.Model, "embedding_dimensions": int64(identity.Dimensions),
				"instruction_version": identity.InstructionVersion, "indexing_version": identity.IndexingVersion,
			}),
		})
	}
	if err := g.qdrant.Upsert(ctx, points); err != nil {
		return fmt.Errorf("store embeddings: %w", err)
	}
	logger.From(ctx).Debug().Int("count", len(embeddings)).Str("provider", identity.Provider).Str("model", identity.Model).Msg("Embeddings stored")
	return nil
}

func (g *Generator) DeleteEmbedding(ctx context.Context, emb PaperEmbedding) error {
	if emb.Identity == (Identity{}) {
		emb.Identity = g.embedder.Identity()
	}
	if err := g.qdrant.Delete(ctx, []*qdrant.PointId{qdrant.NewIDUUID(EmbeddingPointID(emb))}); err != nil {
		return fmt.Errorf("delete embedding: %w", err)
	}
	return nil
}

func (g *Generator) DeletePoint(ctx context.Context, collection, pointID string) error {
	id, err := uuid.Parse(pointID)
	if err != nil {
		return fmt.Errorf("invalid Qdrant point ID %q: %w", pointID, err)
	}
	if err := g.qdrant.DeleteFromCollection(ctx, collection, []*qdrant.PointId{qdrant.NewIDUUID(id.String())}); err != nil {
		return fmt.Errorf("delete point %s from %s: %w", pointID, collection, err)
	}
	return nil
}

func (g *Generator) ExistingPoints(ctx context.Context, pointIDs []string) (map[string]struct{}, error) {
	ids := make([]*qdrant.PointId, 0, len(pointIDs))
	for _, value := range pointIDs {
		id, err := uuid.Parse(value)
		if err != nil {
			return nil, fmt.Errorf("invalid Qdrant point ID %q: %w", value, err)
		}
		ids = append(ids, qdrant.NewIDUUID(id.String()))
	}
	return g.qdrant.ExistingPointIDs(ctx, ids)
}

func EmbeddingPointID(emb PaperEmbedding) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(embeddingKey(emb))).String()
}

func embeddingKey(emb PaperEmbedding) string {
	return fmt.Sprintf("%s:%s:%s:%d:%s:%s", emb.TopicID, emb.PaperID, emb.ChunkType, emb.ChunkIndex, emb.ContentHash, emb.Identity.String())
}

// ValidateVectors rejects malformed or incompatible provider responses.
func ValidateVectors(vectors [][]float32, expectedCount, dimensions int) error {
	if len(vectors) != expectedCount {
		return fmt.Errorf("vector count mismatch: got %d want %d", len(vectors), expectedCount)
	}
	for i, vector := range vectors {
		if len(vector) == 0 {
			return fmt.Errorf("vector %d is empty", i)
		}
		if len(vector) != dimensions {
			return fmt.Errorf("vector %d dimension mismatch: got %d want %d", i, len(vector), dimensions)
		}
		for j, value := range vector {
			if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
				return fmt.Errorf("vector %d contains invalid value at dimension %d", i, j)
			}
		}
	}
	return nil
}

func (g *Generator) SearchSimilar(ctx context.Context, vector []float32, limit uint64, topicID string) ([]*SearchResult, error) {
	var filter *qdrant.Filter
	if topicID != "" {
		filter = &qdrant.Filter{Must: []*qdrant.Condition{qdrant.NewMatch("topic_id", topicID)}}
	}
	return g.search(ctx, vector, limit, filter)
}

// SearchChunks restricts retrieval to persisted full-text chunks.
func (g *Generator) SearchChunks(ctx context.Context, vector []float32, limit uint64, topicID string) ([]*SearchResult, error) {
	filter := &qdrant.Filter{Must: []*qdrant.Condition{
		qdrant.NewMatch("topic_id", topicID), qdrant.NewMatch("chunk_type", "pdf"),
	}}
	return g.search(ctx, vector, limit, filter)
}

func (g *Generator) search(ctx context.Context, vector []float32, limit uint64, filter *qdrant.Filter) ([]*SearchResult, error) {
	if err := ValidateVectors([][]float32{vector}, 1, g.embedder.Dimensions()); err != nil {
		return nil, err
	}
	points, err := g.qdrant.Query(ctx, vector, limit, filter)
	if err != nil {
		return nil, fmt.Errorf("search embeddings: %w", err)
	}
	results := make([]*SearchResult, len(points))
	for i, point := range points {
		result := &SearchResult{Score: point.Score}
		if value, ok := point.Payload["paper_id"]; ok {
			result.PaperID = value.GetStringValue()
		}
		if value, ok := point.Payload["chunk_type"]; ok {
			result.ChunkType = value.GetStringValue()
		}
		if value, ok := point.Payload["chunk_index"]; ok {
			result.ChunkIndex = int(value.GetIntegerValue())
		}
		if value, ok := point.Payload["text"]; ok {
			result.Text = value.GetStringValue()
		}
		results[i] = result
	}
	return results, nil
}

type SearchResult struct {
	PaperID    string
	ChunkType  string
	ChunkIndex int
	Text       string
	Score      float32
}
