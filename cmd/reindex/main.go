package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
	"github.com/paper-scout/internal/accelerator"
	"github.com/paper-scout/internal/config"
	"github.com/paper-scout/internal/ollama"
	"github.com/paper-scout/internal/reindex"
	"github.com/paper-scout/internal/storage/postgres"
	"github.com/paper-scout/internal/storage/qdrant"
	"github.com/paper-scout/internal/tools/embedding"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "reindex:", err)
		os.Exit(1)
	}
}

func run() error {
	_ = godotenv.Load()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	cfg, err := config.LoadDefault()
	if err != nil {
		return err
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	gate := accelerator.NewGate(cfg.Accelerator.MaxConcurrent)
	provider, err := ollama.NewEmbedder(ollama.EmbeddingConfig{
		BaseURL: cfg.Embedding.BaseURL, Model: cfg.Embedding.Model, Timeout: cfg.Embedding.Timeout,
		KeepAlive: cfg.Embedding.KeepAlive, Concurrency: cfg.Embedding.Concurrency, Dimensions: cfg.Embedding.Dimensions,
		QueryInstruction: cfg.Embedding.QueryInstruction, InstructionVersion: cfg.Embedding.InstructionVersion,
		IndexingVersion: cfg.Embedding.IndexingVersion, Gate: gate,
	})
	if err != nil {
		return err
	}
	if err := provider.Health(ctx); err != nil {
		return err
	}
	db, err := postgres.NewClient(ctx, cfg.Database.Postgres)
	if err != nil {
		return err
	}
	defer db.Close()
	identity := provider.Identity()
	vectors, err := qdrant.NewReindexClient(ctx, cfg.Database.Qdrant, qdrant.Schema{Dimensions: identity.Dimensions, GenerationSuffix: identity.CollectionSuffix()})
	if err != nil {
		return err
	}
	defer vectors.Close()
	service := embedding.NewGenerator(provider, vectors)
	runner, err := reindex.NewRunner(db, vectors, service, cfg.Pipeline.EmbeddingBatchSize, os.Stdout)
	if err != nil {
		return err
	}
	return runner.Run(ctx)
}
