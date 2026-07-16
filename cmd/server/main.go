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

	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
	"github.com/paper-scout/internal/accelerator"
	"github.com/paper-scout/internal/api"
	"github.com/paper-scout/internal/api/handler"
	"github.com/paper-scout/internal/application"
	"github.com/paper-scout/internal/config"
	"github.com/paper-scout/internal/llm"
	"github.com/paper-scout/internal/logger"
	"github.com/paper-scout/internal/ollama"
	"github.com/paper-scout/internal/reindex"
	"github.com/paper-scout/internal/storage/postgres"
	"github.com/paper-scout/internal/storage/qdrant"
	"github.com/paper-scout/internal/storage/redis"
	"github.com/paper-scout/internal/tools/arxiv"
	"github.com/paper-scout/internal/tools/embedding"
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

	appCtx, appCancel := context.WithCancel(context.Background())
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
	defer logManager.Close()
	appContext := logManager.App().WithContext(appCtx)

	logger.From(appContext).Info().Str("version", "1.0.0").Msg("Starting Paper Scout")

	pg, err := postgres.NewClient(appContext, cfg.Database.Postgres)
	if err != nil {
		logger.From(appContext).Error().Err(err).Msg("Failed to connect to Postgres")
		return 1
	}
	defer pg.Close()

	redisClient, err := redis.NewClient(appContext, cfg.Database.Redis)
	if err != nil {
		logger.From(appContext).Error().Err(err).Msg("Failed to connect to Redis")
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
		logger.From(appContext).Error().Err(err).Msg("Failed to create embedding provider")
		return 1
	}
	if err := embeddingProvider.Health(appContext); err != nil {
		logger.From(appContext).Error().Err(err).Str("provider", "ollama").Str("model", cfg.Embedding.Model).Msg("Embedding provider is not ready")
		return 1
	}
	identity := embeddingProvider.Identity()
	schema := qdrant.Schema{Dimensions: identity.Dimensions, GenerationSuffix: identity.CollectionSuffix()}
	reindexClient, err := qdrant.NewReindexClient(appContext, cfg.Database.Qdrant, schema)
	if err != nil {
		logger.From(appContext).Error().Err(err).Msg("Failed to connect to Qdrant")
		return 1
	}
	reconciler, err := reindex.NewReconciler(pg, reindexClient)
	if err == nil {
		err = reconciler.Reconcile(appContext)
	}
	if closeErr := reindexClient.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	if err != nil {
		logger.From(appContext).Error().Err(err).Msg("Failed to reconcile Qdrant activation")
		return 1
	}
	qdrantClient, err := qdrant.NewClient(appContext, cfg.Database.Qdrant, schema)
	if err != nil {
		logger.From(appContext).Error().Err(err).Msg("Failed to open active Qdrant collection")
		return 1
	}
	defer qdrantClient.Close()
	activeGeneration, activeErr := pg.Queries().GetActiveEmbeddingGeneration(appContext)
	if activeErr != nil && !errors.Is(activeErr, pgx.ErrNoRows) {
		logger.From(appContext).Error().Err(activeErr).Msg("Failed to inspect active embedding generation")
		return 1
	}
	if activeErr == nil {
		if err := validateActiveEmbeddingGeneration(activeGeneration, identity, qdrantClient.PhysicalCollectionName()); err != nil {
			logger.From(appContext).Error().Err(err).Msg("Active embedding generation is incompatible with startup configuration")
			return 1
		}
	}
	var generator llm.Generator
	switch cfg.Generation.Provider {
	case "gemini":
		gemini, createErr := llm.NewClient(appContext, cfg.Generation.Gemini)
		if createErr != nil {
			logger.From(appContext).Error().Err(createErr).Msg("Failed to create Gemini generator")
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
			logger.From(appContext).Error().Err(createErr).Msg("Failed to create Ollama generator")
			return 1
		}
		generator = local
	default:
		logger.From(appContext).Error().Str("provider", cfg.Generation.Provider).Msg("Unsupported generation provider")
		return 1
	}
	if err := generator.Health(appContext); err != nil {
		logger.From(appContext).Error().Err(err).Str("provider", generator.Provider()).Str("model", generator.Model()).Msg("Generation provider is not ready")
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
		logger.From(appContext).Error().Err(err).Msg("Failed to create Docling parser")
		return 1
	}
	if err := docling.Health(appContext); err != nil {
		logger.From(appContext).Error().Err(err).Str("provider", docling.Provider()).Str("version", docling.Version()).Msg("Document parser is not ready")
		return 1
	}

	ssClient := semantic_scholar.NewClient(appContext, cfg.APIs.SemanticScholar)
	arxivClient := arxiv.NewClient(appContext, cfg.APIs.ArXiv)

	orch, err := application.NewResearchService(appCtx, cfg, logManager, application.Dependencies{
		Postgres: pg, Redis: redisClient, Qdrant: qdrantClient, Generator: generator, EmbeddingProvider: embeddingProvider,
		Parser: docling, SemanticScholar: ssClient, Arxiv: arxivClient,
	})
	if err != nil {
		logger.From(appContext).Error().Err(err).Msg("Failed to create orchestrator")
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
	router, err := api.SetupRouter(orch, health, cfg.Server, logManager)
	if err != nil {
		logger.From(appContext).Error().Err(err).Msg("Failed to create HTTP router")
		return 1
	}

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
		logger.From(appContext).Info().Str("addr", addr).Msg("Server starting")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErr <- err
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	var exitCode int
	select {
	case sig := <-quit:
		logger.From(appContext).Info().Str("signal", sig.String()).Msg("Shutdown signal received")
	case err := <-serverErr:
		logger.From(appContext).Error().Err(err).Msg("Server failed")
		exitCode = 1
	}
	signal.Stop(quit)

	logger.From(appContext).Info().Msg("Shutting down server...")

	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(appContext), 30*time.Second)
	defer cancel()
	orchestratorDone := make(chan struct{})
	go func() {
		orch.Shutdown()
		close(orchestratorDone)
	}()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.From(appContext).Warn().Err(err).Msg("Graceful server shutdown timed out; forcing active connections closed")
		if closeErr := srv.Close(); closeErr != nil && !errors.Is(closeErr, http.ErrServerClosed) {
			logger.From(appContext).Error().Err(closeErr).Msg("Forced server shutdown failed")
			exitCode = 1
		}
	}
	<-orchestratorDone

	logger.From(appContext).Info().Msg("Server stopped")
	return exitCode
}

func validateActiveEmbeddingGeneration(generation *postgres.EmbeddingGeneration, identity embedding.Identity, aliasTarget string) error {
	if generation.CollectionName != aliasTarget {
		return fmt.Errorf("active embedding generation collection %q does not match Qdrant alias target %q", generation.CollectionName, aliasTarget)
	}
	activeIdentity := embedding.Identity{
		Provider:           generation.Provider,
		Model:              generation.Model,
		Dimensions:         int(generation.Dimensions),
		InstructionVersion: generation.InstructionVersion,
		IndexingVersion:    generation.IndexingVersion,
	}
	if activeIdentity != identity {
		return fmt.Errorf("active embedding generation identity %s does not match configured embedding identity %s; run just reindex before startup", activeIdentity.String(), identity.String())
	}
	return nil
}
