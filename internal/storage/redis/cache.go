package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/research-agent/internal/config"
)

const (
	cacheKeyPrefix = "cache"
)

type Cache struct {
	client     *Client
	defaultTTL time.Duration
}

func NewCache(client *Client, cfg config.CacheConfig) *Cache {
	return &Cache{
		client:     client,
		defaultTTL: cfg.DefaultTTL,
	}
}

func (c *Cache) cacheKey(key string) string {
	return fmt.Sprintf("%s:%s", cacheKeyPrefix, key)
}

func (c *Cache) Get(ctx context.Context, key string) (string, error) {
	return c.client.Get(ctx, c.cacheKey(key))
}

func (c *Cache) GetJSON(ctx context.Context, key string, dest interface{}) error {
	return c.client.GetJSON(ctx, c.cacheKey(key), dest)
}

func (c *Cache) Set(ctx context.Context, key string, value interface{}) error {
	return c.client.Set(ctx, c.cacheKey(key), value, c.defaultTTL)
}

func (c *Cache) SetWithTTL(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	return c.client.Set(ctx, c.cacheKey(key), value, ttl)
}

func (c *Cache) SetJSON(ctx context.Context, key string, value interface{}) error {
	return c.client.SetJSON(ctx, c.cacheKey(key), value, c.defaultTTL)
}

func (c *Cache) SetJSONWithTTL(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	return c.client.SetJSON(ctx, c.cacheKey(key), value, ttl)
}

func (c *Cache) Delete(ctx context.Context, key string) error {
	return c.client.Del(ctx, c.cacheKey(key))
}

func (c *Cache) Exists(ctx context.Context, key string) (bool, error) {
	n, err := c.client.Exists(ctx, c.cacheKey(key))
	return n > 0, err
}

func PaperCacheKey(source, externalID string) string {
	return fmt.Sprintf("paper:%s:%s", source, externalID)
}

func EmbeddingCacheKey(hash string) string {
	return fmt.Sprintf("embedding:%s", hash)
}

func SearchCacheKey(queryHash string) string {
	return fmt.Sprintf("search:%s", queryHash)
}

func SessionStateKey(topicID string) string {
	return fmt.Sprintf("session:%s:state", topicID)
}
