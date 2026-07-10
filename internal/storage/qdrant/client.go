package qdrant

import (
	"context"
	"fmt"

	"github.com/paper-scout/internal/config"
	"github.com/paper-scout/internal/logger"
	"github.com/qdrant/go-client/qdrant"
)

const (
	VectorSize     = 768
	CollectionName = "paper_embeddings"
)

type Client struct {
	client     *qdrant.Client
	collection string
}

func NewClient(ctx context.Context, cfg config.QdrantConfig) (*Client, error) {
	client, err := qdrant.NewClient(&qdrant.Config{
		Host:   cfg.Host,
		Port:   cfg.Port,
		APIKey: cfg.APIKey,
		UseTLS: cfg.UseTLS,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create qdrant client: %w", err)
	}

	collection := cfg.Collection
	if collection == "" {
		collection = CollectionName
	}

	c := &Client{
		client:     client,
		collection: collection,
	}

	if err := c.ensureCollection(ctx); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to ensure collection: %w", err)
	}

	logger.Info().
		Str("host", cfg.Host).
		Int("port", cfg.Port).
		Str("collection", collection).
		Msg("Connected to Qdrant")

	return c, nil
}

func (c *Client) ensureCollection(ctx context.Context) error {
	exists, err := c.client.CollectionExists(ctx, c.collection)
	if err != nil {
		return fmt.Errorf("failed to check collection %q: %w", c.collection, err)
	}

	if !exists {
		err = c.client.CreateCollection(ctx, &qdrant.CreateCollection{
			CollectionName: c.collection,
			VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
				Size:     VectorSize,
				Distance: qdrant.Distance_Cosine,
			}),
		})
		if err != nil {
			return fmt.Errorf("failed to create collection: %w", err)
		}
		logger.Info().Str("collection", c.collection).Msg("Created Qdrant collection")
	}

	info, err := c.client.GetCollectionInfo(ctx, c.collection)
	if err != nil {
		return fmt.Errorf("failed to inspect collection %q: %w", c.collection, err)
	}
	if err := validateCollectionSchema(c.collection, info); err != nil {
		return err
	}
	return c.ensureTopicIDIndex(ctx)
}

func (c *Client) ensureTopicIDIndex(ctx context.Context) error {
	wait := true
	_, err := c.client.CreateFieldIndex(ctx, &qdrant.CreateFieldIndexCollection{
		CollectionName: c.collection,
		FieldName:      "topic_id",
		FieldType:      qdrant.FieldType_FieldTypeKeyword.Enum(),
		Wait:           &wait,
	})
	if err != nil {
		return fmt.Errorf("failed to create topic_id payload index: %w", err)
	}
	return nil
}

func validateCollectionSchema(collection string, info *qdrant.CollectionInfo) error {
	if info == nil || info.GetConfig() == nil || info.GetConfig().GetParams() == nil {
		return fmt.Errorf("collection %q has no vector configuration", collection)
	}

	vectors := info.GetConfig().GetParams().GetVectorsConfig()
	if vectors == nil {
		return fmt.Errorf("collection %q has no vector configuration", collection)
	}
	if vectors.GetParamsMap() != nil {
		return fmt.Errorf("collection %q uses named vectors; expected one unnamed dense vector", collection)
	}

	params := vectors.GetParams()
	if params == nil {
		return fmt.Errorf("collection %q has no unnamed dense vector configuration", collection)
	}
	if params.GetSize() != VectorSize {
		return fmt.Errorf("collection %q has vector size %d; expected %d", collection, params.GetSize(), VectorSize)
	}
	if params.GetDistance() != qdrant.Distance_Cosine {
		return fmt.Errorf("collection %q uses distance %s; expected cosine", collection, params.GetDistance())
	}
	return nil
}

func (c *Client) Close() error {
	return c.client.Close()
}

func (c *Client) Ping(ctx context.Context) error {
	_, err := c.client.ListCollections(ctx)
	return err
}

func (c *Client) Upsert(ctx context.Context, points []*qdrant.PointStruct) error {
	_, err := c.client.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: c.collection,
		Points:         points,
	})
	return err
}

func (c *Client) Query(ctx context.Context, vector []float32, limit uint64, filter *qdrant.Filter) ([]*qdrant.ScoredPoint, error) {
	return c.client.Query(ctx, &qdrant.QueryPoints{
		CollectionName: c.collection,
		Query:          qdrant.NewQuery(vector...),
		Limit:          &limit,
		Filter:         filter,
	})
}

func (c *Client) Delete(ctx context.Context, ids []*qdrant.PointId) error {
	_, err := c.client.Delete(ctx, &qdrant.DeletePoints{
		CollectionName: c.collection,
		Points: &qdrant.PointsSelector{
			PointsSelectorOneOf: &qdrant.PointsSelector_Points{
				Points: &qdrant.PointsIdsList{
					Ids: ids,
				},
			},
		},
	})
	return err
}

func (c *Client) DeleteByFilter(ctx context.Context, filter *qdrant.Filter) error {
	_, err := c.client.Delete(ctx, &qdrant.DeletePoints{
		CollectionName: c.collection,
		Points: &qdrant.PointsSelector{
			PointsSelectorOneOf: &qdrant.PointsSelector_Filter{
				Filter: filter,
			},
		},
	})
	return err
}

func (c *Client) Scroll(ctx context.Context, limit uint32, filter *qdrant.Filter) ([]*qdrant.RetrievedPoint, error) {
	res, err := c.client.Scroll(ctx, &qdrant.ScrollPoints{
		CollectionName: c.collection,
		Limit:          &limit,
		Filter:         filter,
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (c *Client) Count(ctx context.Context, filter *qdrant.Filter) (uint64, error) {
	return c.client.Count(ctx, &qdrant.CountPoints{
		CollectionName: c.collection,
		Filter:         filter,
		Exact:          qdrant.PtrOf(true),
	})
}

func (c *Client) CollectionName() string {
	return c.collection
}
