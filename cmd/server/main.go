package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/paper-scout/internal/accelerator"
	"github.com/paper-scout/internal/api"
	"github.com/paper-scout/internal/api/handler"
	"github.com/paper-scout/internal/config"
	"github.com/paper-scout/internal/llm"
	"github.com/paper-scout/internal/logger"
	"github.com/paper-scout/internal/ollama"
	"github.com/paper-scout/internal/orchestrator"
	"github.com/paper-scout/internal/storage/postgres"
	"github.com/paper-scout/internal/storage/qdrant"
	"github.com/paper-scout/internal/storage/redis"
	"github.com/paper-scout/internal/tools/arxiv"
	"github.com/paper-scout/internal/tools/pdf"
	"github.com/paper-scout/internal/tools/semantic_scholar"
)

func main() {
	os.Exit(run())
}

func run() int {
	if err := godotenv.Load(); err != nil {
		fmt.Fprintln(os.Stderr, "No .env file found, using environment variables")
	}

	ctx := context.Background()
	appCtx, appCancel := context.WithCancel(ctx)
	defer appCancel()

	cfg, err := config.LoadDefault()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		return 1
	}

	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Invalid config: %v\n", err)
		return 1
	}

	logManager, err := logger.NewManager(logger.Config{
		Directory: cfg.Logging.Directory, Level: cfg.Logging.Level,
		Development: cfg.Logging.Format == "console" || cfg.Logging.Format == "development",
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logging: %v\n", err)
		return 1
	}
	if err := logger.Install(logManager); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to install logging: %v\n", err)
		_ = logManager.Close()
		return 1
	}
	defer logManager.Close()

	logger.Info().Str("version", "1.0.0").Msg("Starting Paper Scout")

	pg, err := postgres.NewClient(ctx, cfg.Database.Postgres)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to connect to Postgres")
		return 1
	}
	defer pg.Close()

	redisClient, err := redis.NewClient(ctx, cfg.Database.Redis)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to connect to Redis")
		return 1
	}
	defer redisClient.Close()

	gate := accelerator.NewGate(cfg.Accelerator.MaxConcurrent)
	embeddingProvider, err := ollama.NewEmbedder(ollama.EmbeddingConfig{
		BaseURL: cfg.Embedding.BaseURL, Model: cfg.Embedding.Model, Timeout: cfg.Embedding.Timeout,
		KeepAlive: cfg.Embedding.KeepAlive, Concurrency: cfg.Embedding.Concurrency, Dimensions: cfg.Embedding.Dimensions,
		QueryInstruction: cfg.Embedding.QueryInstruction, InstructionVersion: cfg.Embedding.InstructionVersion,
		IndexingVersion: cfg.Embedding.IndexingVersion, Gate: gate,
	})
	if err != nil {
		logger.Error().Err(err).Msg("Failed to create embedding provider")
		return 1
	}
	if err := embeddingProvider.Health(ctx); err != nil {
		logger.Error().Err(err).Str("provider", "ollama").Str("model", cfg.Embedding.Model).Msg("Embedding provider is not ready")
		return 1
	}
	identity := embeddingProvider.Identity()
	qdrantClient, err := qdrant.NewClient(ctx, cfg.Database.Qdrant, qdrant.Schema{
		Dimensions: identity.Dimensions, GenerationSuffix: identity.CollectionSuffix(),
	})
	if err != nil {
		logger.Error().Err(err).Msg("Failed to connect to Qdrant")
		return 1
	}
	defer qdrantClient.Close()

	var generator llm.Generator
	switch cfg.Generation.Provider {
	case "gemini":
		gemini, createErr := llm.NewClient(ctx, cfg.Generation.Gemini)
		if createErr != nil {
			logger.Error().Err(createErr).Msg("Failed to create Gemini generator")
			return 1
		}
		generator = gemini
	case "ollama":
		local, createErr := ollama.NewGenerator(ollama.GenerationConfig{
			BaseURL: cfg.Generation.Ollama.BaseURL, Model: cfg.Generation.Ollama.Model,
			Timeout: cfg.Generation.Ollama.Timeout, KeepAlive: cfg.Generation.Ollama.KeepAlive,
			Concurrency: cfg.Generation.Ollama.Concurrency, Think: cfg.Generation.Ollama.Think,
			MaxOutputTokens: cfg.Generation.Ollama.MaxOutputTokens, Temperature: cfg.Generation.Ollama.Temperature,
			Gate: gate,
		})
		if createErr != nil {
			logger.Error().Err(createErr).Msg("Failed to create Ollama generator")
			return 1
		}
		generator = local
	default:
		logger.Error().Str("provider", cfg.Generation.Provider).Msg("Unsupported generation provider")
		return 1
	}
	if err := generator.Health(ctx); err != nil {
		logger.Error().Err(err).Str("provider", generator.Provider()).Str("model", generator.Model()).Msg("Generation provider is not ready")
		return 1
	}
	docling, err := pdf.NewDoclingClient(pdf.DoclingConfig{
		BaseURL: cfg.APIs.Docling.BaseURL, RequestTimeout: cfg.APIs.Docling.RequestTimeout,
		DocumentTimeout: cfg.APIs.Docling.DocumentTimeout, OCRBehavior: cfg.APIs.Docling.OCRBehavior,
		OutputFormat: cfg.APIs.Docling.OutputFormat, Concurrency: cfg.APIs.Docling.Concurrency,
		Version: cfg.APIs.Docling.Version, MaxResponseBytes: cfg.APIs.Docling.MaxResponseBytes,
		MinExtractedCharacters: cfg.APIs.Docling.MinExtractedCharacters, Gate: gate,
	})
	if err != nil {
		logger.Error().Err(err).Msg("Failed to create Docling parser")
		return 1
	}
	if err := docling.Health(ctx); err != nil {
		logger.Error().Err(err).Str("provider", docling.Provider()).Str("version", docling.Version()).Msg("Document parser is not ready")
		return 1
	}

	ssClient := semantic_scholar.NewClient(cfg.APIs.SemanticScholar)
	arxivClient := arxiv.NewClient(cfg.APIs.ArXiv)

	orch, err := orchestrator.NewOrchestrator(
		appCtx,
		cfg,
		logManager,
		pg,
		redisClient,
		qdrantClient,
		generator,
		embeddingProvider,
		docling,
		ssClient,
		arxivClient,
	)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to create orchestrator")
		return 1
	}

	health := handler.NewHealthHandler(map[string]handler.HealthCheck{
		"postgres":   pg,
		"redis":      redisClient,
		"qdrant":     qdrantClient,
		"generation": handler.HealthCheckFunc(generator.Health),
		"embedding":  handler.HealthCheckFunc(embeddingProvider.Health),
		"docling":    handler.HealthCheckFunc(docling.Health),
	})
	router := api.SetupRouter(orch, health, cfg.Server)

	addr := fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port)
	srv := &http.Server{
		Addr:         addr,
		Handler:      router,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
	}
	// Shutdown does not terminate long-lived streams by itself. Closing every
	// subscription lets their handlers return promptly during graceful shutdown.
	srv.RegisterOnShutdown(func() {
		orch.GetSSEManager().CloseAll()
	})

	serverErr := make(chan error, 1)
	go func() {
		logger.Info().Str("addr", addr).Msg("Server starting")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErr <- err
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	var exitCode int
	select {
	case sig := <-quit:
		logger.Info().Str("signal", sig.String()).Msg("Shutdown signal received")
	case err := <-serverErr:
		logger.Error().Err(err).Msg("Server failed")
		exitCode = 1
	}
	signal.Stop(quit)

	logger.Info().Msg("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	orchestratorDone := make(chan struct{})
	go func() {
		orch.Shutdown()
		close(orchestratorDone)
	}()

	if err := srv.Shutdown(ctx); err != nil {
		logger.Warn().Err(err).Msg("Graceful server shutdown timed out; forcing active connections closed")
		if closeErr := srv.Close(); closeErr != nil && !errors.Is(closeErr, http.ErrServerClosed) {
			logger.Error().Err(closeErr).Msg("Forced server shutdown failed")
			exitCode = 1
		}
	}
	<-orchestratorDone

	logger.Info().Msg("Server stopped")
	return exitCode
}
