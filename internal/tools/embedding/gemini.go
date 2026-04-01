package embedding

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/qdrant/go-client/qdrant"
	"github.com/paper-scout/internal/llm"
	"github.com/paper-scout/internal/logger"
	qdrantstore "github.com/paper-scout/internal/storage/qdrant"
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
	PaperID    string
	TopicID    string
	ChunkType  string
	ChunkIndex int
	Text       string
	Vector     []float32
}

func (g *Generator) StoreEmbedding(ctx context.Context, emb PaperEmbedding) error {
	point := &qdrant.PointStruct{
		Id:      qdrant.NewIDUUID(uuid.NewString()),
		Vectors: qdrant.NewVectors(emb.Vector...),
		Payload: qdrant.NewValueMap(map[string]any{
			"paper_id":    emb.PaperID,
			"topic_id":    emb.TopicID,
			"chunk_type":  emb.ChunkType,
			"chunk_index": int64(emb.ChunkIndex),
			"text":        emb.Text,
		}),
	}

	if err := g.qdrant.Upsert(ctx, []*qdrant.PointStruct{point}); err != nil {
		return fmt.Errorf("failed to store embedding: %w", err)
	}

	logger.Debug().
		Str("paper_id", emb.PaperID).
		Str("chunk_type", emb.ChunkType).
		Int("chunk_index", emb.ChunkIndex).
		Msg("Embedding stored")

	return nil
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
