package qdrant

import (
	"context"
	"fmt"

	"github.com/paper-scout/internal/config"
	"github.com/paper-scout/internal/logger"
	"github.com/qdrant/go-client/qdrant"
)

// Schema identifies the physical collection required by an embedding generation.
type Schema struct {
	Dimensions       int
	GenerationSuffix string
}

type Client struct {
	client             *qdrant.Client
	alias              string
	physicalCollection string
	writeCollection    string
	queryCollection    string
	dimensions         int
}

func NewClient(ctx context.Context, cfg config.QdrantConfig, schema Schema) (*Client, error) {
	return newClient(ctx, cfg, schema, true)
}

// NewReindexClient targets an inactive physical generation until Activate is called.
func NewReindexClient(ctx context.Context, cfg config.QdrantConfig, schema Schema) (*Client, error) {
	return newClient(ctx, cfg, schema, false)
}

func newClient(ctx context.Context, cfg config.QdrantConfig, schema Schema, requireActive bool) (*Client, error) {
	client, err := qdrant.NewClient(&qdrant.Config{
		Host:   cfg.Host,
		Port:   cfg.Port,
		APIKey: cfg.APIKey,
		UseTLS: cfg.UseTLS,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create qdrant client: %w", err)
	}

	physical := cfg.CollectionPrefix + "_" + schema.GenerationSuffix
	c := &Client{
		client:             client,
		alias:              cfg.Alias,
		physicalCollection: physical,
		writeCollection:    physical,
		queryCollection:    physical,
		dimensions:         schema.Dimensions,
	}

	if err := c.ensureCollection(ctx, requireActive); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to ensure collection: %w", err)
	}
	if requireActive {
		c.writeCollection = c.alias
		c.queryCollection = c.alias
	}

	logger.Info().
		Str("host", cfg.Host).
		Int("port", cfg.Port).
		Str("alias", c.alias).
		Str("collection", c.physicalCollection).
		Msg("Connected to Qdrant")

	return c, nil
}

func (c *Client) ensureCollection(ctx context.Context, requireActive bool) error {
	exists, err := c.client.CollectionExists(ctx, c.physicalCollection)
	if err != nil {
		return fmt.Errorf("failed to check collection %q: %w", c.physicalCollection, err)
	}

	if !exists {
		err = c.client.CreateCollection(ctx, &qdrant.CreateCollection{
			CollectionName: c.physicalCollection,
			VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
				Size:     uint64(c.dimensions),
				Distance: qdrant.Distance_Cosine,
			}),
		})
		if err != nil {
			return fmt.Errorf("failed to create collection: %w", err)
		}
		logger.Info().Str("collection", c.physicalCollection).Msg("Created Qdrant collection")
	}

	info, err := c.client.GetCollectionInfo(ctx, c.physicalCollection)
	if err != nil {
		return fmt.Errorf("failed to inspect collection %q: %w", c.physicalCollection, err)
	}
	if err := validateCollectionSchema(c.physicalCollection, info, c.dimensions); err != nil {
		return err
	}
	if err := c.ensureTopicIDIndex(ctx); err != nil {
		return err
	}
	if requireActive {
		return c.ensureAlias(ctx)
	}
	return nil
}

func (c *Client) ensureAlias(ctx context.Context) error {
	aliases, err := c.client.ListAliases(ctx)
	if err != nil {
		return fmt.Errorf("list Qdrant aliases: %w", err)
	}
	for _, alias := range aliases {
		if alias.GetAliasName() != c.alias {
			continue
		}
		if alias.GetCollectionName() != c.physicalCollection {
			return fmt.Errorf("Qdrant alias %q targets %q, but embedding identity requires %q; run `go run ./cmd/reindex` to build and activate the new generation", c.alias, alias.GetCollectionName(), c.physicalCollection)
		}
		return nil
	}
	if err := c.client.CreateAlias(ctx, c.alias, c.physicalCollection); err != nil {
		return fmt.Errorf("create Qdrant alias %q: %w", c.alias, err)
	}
	return nil
}

func (c *Client) ensureTopicIDIndex(ctx context.Context) error {
	wait := true
	_, err := c.client.CreateFieldIndex(ctx, &qdrant.CreateFieldIndexCollection{
		CollectionName: c.physicalCollection,
		FieldName:      "topic_id",
		FieldType:      qdrant.FieldType_FieldTypeKeyword.Enum(),
		Wait:           &wait,
	})
	if err != nil {
		return fmt.Errorf("failed to create topic_id payload index: %w", err)
	}
	return nil
}

func validateCollectionSchema(collection string, info *qdrant.CollectionInfo, dimensions int) error {
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
	if params.GetSize() != uint64(dimensions) {
		return fmt.Errorf("collection %q has vector size %d; expected %d", collection, params.GetSize(), dimensions)
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
	if _, err := c.client.ListCollections(ctx); err != nil {
		return err
	}
	info, err := c.client.GetCollectionInfo(ctx, c.physicalCollection)
	if err != nil {
		return fmt.Errorf("inspect active collection: %w", err)
	}
	if err := validateCollectionSchema(c.physicalCollection, info, c.dimensions); err != nil {
		return err
	}
	aliases, err := c.client.ListAliases(ctx)
	if err != nil {
		return fmt.Errorf("list aliases: %w", err)
	}
	for _, alias := range aliases {
		if alias.GetAliasName() == c.alias {
			if alias.GetCollectionName() != c.physicalCollection {
				return fmt.Errorf("alias %q targets %q, expected %q", c.alias, alias.GetCollectionName(), c.physicalCollection)
			}
			return nil
		}
	}
	return fmt.Errorf("alias %q is missing", c.alias)
}

func (c *Client) Upsert(ctx context.Context, points []*qdrant.PointStruct) error {
	_, err := c.client.Upsert(ctx, upsertRequest(c.writeCollection, points))
	return err
}

func upsertRequest(collection string, points []*qdrant.PointStruct) *qdrant.UpsertPoints {
	wait := true
	return &qdrant.UpsertPoints{CollectionName: collection, Wait: &wait, Points: points}
}

func (c *Client) Query(ctx context.Context, vector []float32, limit uint64, filter *qdrant.Filter) ([]*qdrant.ScoredPoint, error) {
	return c.client.Query(ctx, queryRequest(c.queryCollection, vector, limit, filter))
}

func queryRequest(collection string, vector []float32, limit uint64, filter *qdrant.Filter) *qdrant.QueryPoints {
	return &qdrant.QueryPoints{
		CollectionName: collection,
		Query:          qdrant.NewQuery(vector...),
		Limit:          &limit,
		Filter:         filter,
		WithPayload:    qdrant.NewWithPayloadInclude("paper_id", "chunk_type", "chunk_index", "text"),
	}
}

func (c *Client) Delete(ctx context.Context, ids []*qdrant.PointId) error {
	return c.DeleteFromCollection(ctx, c.writeCollection, ids)
}

func (c *Client) DeleteFromCollection(ctx context.Context, collection string, ids []*qdrant.PointId) error {
	_, err := c.client.Delete(ctx, deleteIDsRequest(collection, ids))
	return err
}

func deleteIDsRequest(collection string, ids []*qdrant.PointId) *qdrant.DeletePoints {
	wait := true
	return &qdrant.DeletePoints{
		CollectionName: collection,
		Wait:           &wait,
		Points: &qdrant.PointsSelector{
			PointsSelectorOneOf: &qdrant.PointsSelector_Points{
				Points: &qdrant.PointsIdsList{
					Ids: ids,
				},
			},
		},
	}
}

func (c *Client) DeleteByFilter(ctx context.Context, filter *qdrant.Filter) error {
	wait := true
	_, err := c.client.Delete(ctx, &qdrant.DeletePoints{
		CollectionName: c.writeCollection,
		Wait:           &wait,
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
		CollectionName: c.queryCollection,
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
		CollectionName: c.queryCollection,
		Filter:         filter,
		Exact:          qdrant.PtrOf(true),
	})
}

// ListPointIDs returns every point ID in the targeted generation using bounded pages.
func (c *Client) ListPointIDs(ctx context.Context, pageSize uint32) ([]*qdrant.PointId, error) {
	if pageSize == 0 {
		pageSize = 256
	}
	var result []*qdrant.PointId
	var offset *qdrant.PointId
	for {
		response, nextOffset, err := c.client.ScrollAndOffset(ctx, &qdrant.ScrollPoints{
			CollectionName: c.queryCollection, Limit: &pageSize, Offset: offset,
			WithPayload: &qdrant.WithPayloadSelector{SelectorOptions: &qdrant.WithPayloadSelector_Enable{Enable: false}},
			WithVectors: &qdrant.WithVectorsSelector{SelectorOptions: &qdrant.WithVectorsSelector_Enable{Enable: false}},
		})
		if err != nil {
			return nil, err
		}
		for _, point := range response {
			result = append(result, point.Id)
		}
		if nextOffset == nil {
			break
		}
		offset = nextOffset
	}
	return result, nil
}

// ExistingPointIDs returns the subset of requested UUID point IDs present in the active collection.
func (c *Client) ExistingPointIDs(ctx context.Context, ids []*qdrant.PointId) (map[string]struct{}, error) {
	points, err := c.client.Get(ctx, &qdrant.GetPoints{
		CollectionName: c.queryCollection, Ids: ids,
		WithPayload: &qdrant.WithPayloadSelector{SelectorOptions: &qdrant.WithPayloadSelector_Enable{Enable: false}},
		WithVectors: &qdrant.WithVectorsSelector{SelectorOptions: &qdrant.WithVectorsSelector_Enable{Enable: false}},
	})
	if err != nil {
		return nil, err
	}
	existing := make(map[string]struct{}, len(points))
	for _, point := range points {
		existing[point.Id.GetUuid()] = struct{}{}
	}
	return existing, nil
}

func (c *Client) CollectionName() string {
	return c.alias
}

func (c *Client) PhysicalCollectionName() string { return c.physicalCollection }

// Activate atomically switches the stable alias to this physical generation.
func (c *Client) Activate(ctx context.Context) error {
	aliases, err := c.client.ListAliases(ctx)
	if err != nil {
		return fmt.Errorf("list Qdrant aliases before activation: %w", err)
	}
	current := ""
	for _, alias := range aliases {
		if alias.GetAliasName() == c.alias {
			current = alias.GetCollectionName()
			break
		}
	}
	if current == c.physicalCollection {
		return nil
	}
	actions := activationActions(c.alias, current, c.physicalCollection)
	if err := c.client.UpdateAliases(ctx, actions); err != nil {
		return fmt.Errorf("activate Qdrant collection %q through alias %q: %w", c.physicalCollection, c.alias, err)
	}
	c.writeCollection = c.alias
	c.queryCollection = c.alias
	return nil
}

func activationActions(alias, current, target string) []*qdrant.AliasOperations {
	actions := make([]*qdrant.AliasOperations, 0, 2)
	if current != "" {
		actions = append(actions, qdrant.NewAliasDelete(alias))
	}
	return append(actions, qdrant.NewAliasCreate(alias, target))
}
