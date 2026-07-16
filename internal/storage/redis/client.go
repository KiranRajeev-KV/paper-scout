package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/paper-scout/internal/config"
	"github.com/paper-scout/internal/logger"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
)

// Client owns independent Redis control-plane and blocking-worker clients.
type Client struct {
	controlClient *redis.Client
	workerClient  *redis.Client
	log           zerolog.Logger
}

func NewClient(ctx context.Context, cfg config.RedisConfig) (*Client, error) {
	controlClient := newRedisClient(cfg, cfg.PoolSize)
	workerClient := newRedisClient(cfg, cfg.WorkerPoolSize)

	if err := controlClient.Ping(ctx).Err(); err != nil {
		_ = controlClient.Close()
		_ = workerClient.Close()
		return nil, fmt.Errorf("failed to ping Redis control client: %w", err)
	}
	if err := workerClient.Ping(ctx).Err(); err != nil {
		_ = controlClient.Close()
		_ = workerClient.Close()
		return nil, fmt.Errorf("failed to ping Redis worker client: %w", err)
	}

	logger.From(ctx).Info().
		Str("addr", cfg.Addr()).
		Int("db", cfg.DB).
		Int("control_pool_size", cfg.PoolSize).
		Int("worker_pool_size", cfg.WorkerPoolSize).
		Msg("Connected to Redis")

	return &Client{controlClient: controlClient, workerClient: workerClient, log: *logger.From(ctx)}, nil
}

func newRedisClient(cfg config.RedisConfig, poolSize int) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     cfg.Addr(),
		Password: cfg.Password,
		DB:       cfg.DB,
		PoolSize: poolSize,
	})
}

func (c *Client) Close() error {
	controlErr := c.controlClient.Close()
	workerErr := c.workerClient.Close()
	if controlErr == nil && workerErr == nil {
		c.log.Info().Msg("Redis connection closed")
	}
	if controlErr != nil {
		return controlErr
	}
	return workerErr
}

func (c *Client) Ping(ctx context.Context) error {
	return c.controlClient.Ping(ctx).Err()
}

// Client returns the control-plane client used for state, acknowledgements,
// and health checks. It is deliberately separate from blocking stream reads.
func (c *Client) Client() *redis.Client {
	return c.controlClient
}

// WorkerClient is reserved for Redis Streams XREADGROUP BLOCK calls.
func (c *Client) WorkerClient() *redis.Client {
	return c.workerClient
}

func (c *Client) Get(ctx context.Context, key string) (string, error) {
	return c.controlClient.Get(ctx, key).Result()
}

func (c *Client) GetJSON(ctx context.Context, key string, dest interface{}) error {
	data, err := c.controlClient.Get(ctx, key).Bytes()
	if err != nil {
		return err
	}
	return json.Unmarshal(data, dest)
}

func (c *Client) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	return c.controlClient.Set(ctx, key, value, ttl).Err()
}

func (c *Client) SetJSON(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}
	return c.controlClient.Set(ctx, key, data, ttl).Err()
}

func (c *Client) Del(ctx context.Context, keys ...string) error {
	return c.controlClient.Del(ctx, keys...).Err()
}

func (c *Client) Exists(ctx context.Context, keys ...string) (int64, error) {
	return c.controlClient.Exists(ctx, keys...).Result()
}

func (c *Client) Expire(ctx context.Context, key string, ttl time.Duration) error {
	return c.controlClient.Expire(ctx, key, ttl).Err()
}

func (c *Client) TTL(ctx context.Context, key string) (time.Duration, error) {
	return c.controlClient.TTL(ctx, key).Result()
}

func (c *Client) Incr(ctx context.Context, key string) (int64, error) {
	return c.controlClient.Incr(ctx, key).Result()
}

func (c *Client) Decr(ctx context.Context, key string) (int64, error) {
	return c.controlClient.Decr(ctx, key).Result()
}

func (c *Client) LPush(ctx context.Context, key string, values ...interface{}) error {
	return c.controlClient.LPush(ctx, key, values...).Err()
}

func (c *Client) RPush(ctx context.Context, key string, values ...interface{}) error {
	return c.controlClient.RPush(ctx, key, values...).Err()
}

func (c *Client) LPop(ctx context.Context, key string) (string, error) {
	return c.controlClient.LPop(ctx, key).Result()
}

func (c *Client) RPop(ctx context.Context, key string) (string, error) {
	return c.controlClient.RPop(ctx, key).Result()
}

func (c *Client) LLen(ctx context.Context, key string) (int64, error) {
	return c.controlClient.LLen(ctx, key).Result()
}

func (c *Client) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return c.controlClient.LRange(ctx, key, start, stop).Result()
}

func (c *Client) SAdd(ctx context.Context, key string, members ...interface{}) error {
	return c.controlClient.SAdd(ctx, key, members...).Err()
}

func (c *Client) SRem(ctx context.Context, key string, members ...interface{}) error {
	return c.controlClient.SRem(ctx, key, members...).Err()
}

func (c *Client) SMembers(ctx context.Context, key string) ([]string, error) {
	return c.controlClient.SMembers(ctx, key).Result()
}

func (c *Client) SIsMember(ctx context.Context, key string, member interface{}) (bool, error) {
	return c.controlClient.SIsMember(ctx, key, member).Result()
}

func (c *Client) HSet(ctx context.Context, key string, field string, value interface{}) error {
	return c.controlClient.HSet(ctx, key, field, value).Err()
}

func (c *Client) HGet(ctx context.Context, key, field string) (string, error) {
	return c.controlClient.HGet(ctx, key, field).Result()
}

func (c *Client) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return c.controlClient.HGetAll(ctx, key).Result()
}

func (c *Client) HDel(ctx context.Context, key string, fields ...string) error {
	return c.controlClient.HDel(ctx, key, fields...).Err()
}

func (c *Client) Scan(ctx context.Context, cursor uint64, match string, count int64) ([]string, uint64, error) {
	return c.controlClient.Scan(ctx, cursor, match, count).Result()
}
