package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/paper-scout/internal/api"
	"github.com/paper-scout/internal/config"
	"github.com/paper-scout/internal/llm"
	"github.com/paper-scout/internal/logger"
	"github.com/paper-scout/internal/orchestrator"
	"github.com/paper-scout/internal/storage/postgres"
	"github.com/paper-scout/internal/storage/qdrant"
	"github.com/paper-scout/internal/storage/redis"
	"github.com/paper-scout/internal/tools/arxiv"
	"github.com/paper-scout/internal/tools/semantic_scholar"
)

func main() {
	if err := godotenv.Load(); err != nil {
		fmt.Fprintln(os.Stderr, "No .env file found, using environment variables")
	}

	ctx := context.Background()
	appCtx, appCancel := context.WithCancel(ctx)
	defer appCancel()

	cfg, err := config.LoadDefault()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Invalid config: %v\n", err)
		os.Exit(1)
	}

	cfg.ApplyLogging()

	logger.Info().Str("version", "1.0.0").Msg("Starting Research AI Agent")

	pg, err := postgres.NewClient(ctx, cfg.Database.Postgres)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to connect to Postgres")
		os.Exit(1)
	}
	defer pg.Close()

	redisClient, err := redis.NewClient(ctx, cfg.Database.Redis)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to connect to Redis")
		os.Exit(1)
	}
	defer redisClient.Close()

	qdrantClient, err := qdrant.NewClient(ctx, cfg.Database.Qdrant)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to connect to Qdrant")
		os.Exit(1)
	}
	defer qdrantClient.Close()

	llmClient, err := llm.NewClient(ctx, cfg.LLM)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to create LLM client")
		os.Exit(1)
	}
	defer llmClient.Close()

	ssClient := semantic_scholar.NewClient(cfg.APIs.SemanticScholar)
	arxivClient := arxiv.NewClient(cfg.APIs.ArXiv)

	orch := orchestrator.NewOrchestrator(
		appCtx,
		cfg,
		pg,
		redisClient,
		qdrantClient,
		llmClient,
		ssClient,
		arxivClient,
	)
	defer orch.Shutdown()

	router := api.SetupRouter(orch)

	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	srv := &http.Server{
		Addr:         addr,
		Handler:      router,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
	}

	go func() {
		logger.Info().Str("addr", addr).Msg("Server starting")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error().Err(err).Msg("Server failed")
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info().Msg("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		logger.Error().Err(err).Msg("Server shutdown failed")
		os.Exit(1)
	}

	logger.Info().Msg("Server stopped")
}
