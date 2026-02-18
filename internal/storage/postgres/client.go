package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/research-agent/internal/config"
	"github.com/research-agent/internal/logger"
)

type Client struct {
	pool    *pgxpool.Pool
	queries *Queries
}

func NewClient(ctx context.Context, cfg config.PostgresConfig) (*Client, error) {
	poolConfig, err := pgxpool.ParseConfig(cfg.DSN())
	if err != nil {
		return nil, fmt.Errorf("failed to parse database config: %w", err)
	}

	poolConfig.MaxConns = int32(cfg.MaxConnections)
	poolConfig.MinConns = int32(cfg.MaxIdle)
	poolConfig.ConnConfig.ConnectTimeout = cfg.ConnTimeout
	poolConfig.HealthCheckPeriod = 1 * time.Minute
	poolConfig.MaxConnLifetime = 2 * time.Hour
	poolConfig.MaxConnIdleTime = 30 * time.Minute

	ctx, cancel := context.WithTimeout(ctx, cfg.ConnTimeout)
	defer cancel()

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	logger.Info().
		Str("host", cfg.Host).
		Int("port", cfg.Port).
		Str("database", cfg.Database).
		Int("max_connections", cfg.MaxConnections).
		Msg("Connected to PostgreSQL")

	return &Client{
		pool:    pool,
		queries: New(pool),
	}, nil
}

func (c *Client) Close() {
	c.pool.Close()
	logger.Info().Msg("PostgreSQL connection pool closed")
}

func (c *Client) Queries() *Queries {
	return c.queries
}

func (c *Client) Pool() *pgxpool.Pool {
	return c.pool
}

func (c *Client) Ping(ctx context.Context) error {
	return c.pool.Ping(ctx)
}

func (c *Client) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return c.pool.Begin(ctx)
}

func (c *Client) WithTx(ctx context.Context, fn func(*Queries) error) error {
	tx, err := c.BeginTx(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := fn(c.queries.WithTx(tx)); err != nil {
		return err
	}

	return tx.Commit(ctx)
}
