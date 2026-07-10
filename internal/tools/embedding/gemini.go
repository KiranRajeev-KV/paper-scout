package embedding

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/paper-scout/internal/llm"
	"github.com/paper-scout/internal/logger"
	qdrantstore "github.com/paper-scout/internal/storage/qdrant"
	"github.com/qdrant/go-client/qdrant"
)

type Generator struct {
	llmClient *llm.Client
	qdrant    *qdrantstore.Client
}

func NewGenerator(llmClient *llm.Client, qdrantClient *qdrantstore.Client) *Generator {
	return &Generator{
		llmClient: llmClient,
		qdrant:    qdrantClient,
	}
}

func (g *Generator) Generate(ctx context.Context, text string) ([]float32, error) {
	return g.llmClient.EmbedSingle(ctx, text)
}

func (g *Generator) GenerateBatch(ctx context.Context, texts []string) ([][]float32, error) {
	return g.llmClient.Embed(ctx, texts)
}

type PaperEmbedding struct {
	ChunkID    string
	PaperID    string
	TopicID    string
	ChunkType  string
	ChunkIndex int
	Text       string
	Vector     []float32
}

func (g *Generator) StoreEmbedding(ctx context.Context, emb PaperEmbedding) error {
	return g.StoreEmbeddings(ctx, []PaperEmbedding{emb})
}

func (g *Generator) StoreEmbeddings(ctx context.Context, embeddings []PaperEmbedding) error {
	if len(embeddings) == 0 {
		return nil
	}
	points := make([]*qdrant.PointStruct, 0, len(embeddings))
	for _, emb := range embeddings {
		points = append(points, &qdrant.PointStruct{
			Id:      qdrant.NewIDUUID(EmbeddingPointID(emb)),
			Vectors: qdrant.NewVectors(emb.Vector...),
			Payload: qdrant.NewValueMap(map[string]any{
				"chunk_id":    emb.ChunkID,
				"paper_id":    emb.PaperID,
				"topic_id":    emb.TopicID,
				"chunk_type":  emb.ChunkType,
				"chunk_index": int64(emb.ChunkIndex),
				"text":        emb.Text,
			}),
		})
	}

	if err := g.qdrant.Upsert(ctx, points); err != nil {
		return fmt.Errorf("failed to store embedding: %w", err)
	}

	logger.Debug().
		Int("count", len(embeddings)).
		Msg("Embeddings stored")

	return nil
}

func (g *Generator) DeleteEmbedding(ctx context.Context, emb PaperEmbedding) error {
	if err := g.qdrant.Delete(ctx, []*qdrant.PointId{qdrant.NewIDUUID(EmbeddingPointID(emb))}); err != nil {
		return fmt.Errorf("failed to delete embedding: %w", err)
	}
	return nil
}

func EmbeddingPointID(emb PaperEmbedding) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(embeddingKey(emb))).String()
}

func embeddingKey(emb PaperEmbedding) string {
	return fmt.Sprintf("%s:%s:%s:%d", emb.TopicID, emb.PaperID, emb.ChunkType, emb.ChunkIndex)
}

func (g *Generator) SearchSimilar(ctx context.Context, vector []float32, limit uint64, topicID string) ([]*SearchResult, error) {
	var filter *qdrant.Filter
	if topicID != "" {
		filter = &qdrant.Filter{
			Must: []*qdrant.Condition{
				qdrant.NewMatch("topic_id", topicID),
			},
		}
	}
	return g.search(ctx, vector, limit, filter)
}

// SearchChunks restricts retrieval to persisted full-text chunks. Abstract
// vectors remain available to ranking through SearchSimilar.
func (g *Generator) SearchChunks(ctx context.Context, vector []float32, limit uint64, topicID string) ([]*SearchResult, error) {
	filter := &qdrant.Filter{Must: []*qdrant.Condition{
		qdrant.NewMatch("topic_id", topicID),
		qdrant.NewMatch("chunk_type", "pdf"),
	}}
	return g.search(ctx, vector, limit, filter)
}

func (g *Generator) search(ctx context.Context, vector []float32, limit uint64, filter *qdrant.Filter) ([]*SearchResult, error) {
	points, err := g.qdrant.Query(ctx, vector, limit, filter)
	if err != nil {
		return nil, fmt.Errorf("search failed: %w", err)
	}

	results := make([]*SearchResult, len(points))
	for i, p := range points {
		paperID := ""
		chunkType := ""
		chunkIndex := 0
		text := ""

		if v, ok := p.Payload["paper_id"]; ok {
			paperID = v.GetStringValue()
		}
		if v, ok := p.Payload["chunk_type"]; ok {
			chunkType = v.GetStringValue()
		}
		if v, ok := p.Payload["chunk_index"]; ok {
			chunkIndex = int(v.GetIntegerValue())
		}
		if v, ok := p.Payload["text"]; ok {
			text = v.GetStringValue()
		}

		results[i] = &SearchResult{
			PaperID:    paperID,
			ChunkType:  chunkType,
			ChunkIndex: chunkIndex,
			Text:       text,
			Score:      p.Score,
		}
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
