# Architecture

## Purpose and boundaries

Paper Scout is an asynchronous research-report service. It accepts a topic over HTTP, persists a durable workflow, gathers and processes scholarly metadata, then exposes progress and completed artefacts over the same HTTP API. It is not a web UI, a general document store, or a guarantee that generated conclusions are correct.

## System view

```text
Client
  | HTTP + SSE
  v
Gin API ──────> Orchestrator ──────> PostgreSQL (authoritative state)
                    |  |                    |
                    |  +──> Redis (queue and 24 h live snapshots)
                    |  +──> Qdrant (versioned embedding generation)
                    |
                    +──> Semantic Scholar + arXiv (discovery)
                    +──> Ollama or Gemini (structured generation)
                    +──> Docling (PDF to Markdown)
```

## Runtime composition

`cmd/server` loads `.env` (if present), reads `config/default.yaml`, overlays supported `SECTION__KEY` environment variables, validates configuration, connects each dependency, checks the configured models/parser, then creates the HTTP server. It shuts down HTTP streams, active pipelines, and workers gracefully on `SIGINT` or `SIGTERM`.

The deployed services in `docker-compose.yml` are PostgreSQL, Redis, Qdrant, and Docling. The application is an opt-in Compose profile; Ollama is deliberately external to the stack and is reached through `host.docker.internal` when the app container is used.

## Research pipeline

Each submission creates a `research_topics` record with a topic UUID and run UUID. The orchestrator executes these stages in order:

1. **Query expansion** — the generator produces search queries, concepts, subtopics, and keywords.
2. **Paper discovery** — Semantic Scholar and arXiv queries run concurrently, results are reconciled by DOI, arXiv ID, and a title/year/author fallback, then persisted as topic membership.
3. **Ranking** — abstracts are embedded, stored in Qdrant, retrieved against the topic, and optionally LLM-reranked (30% vector score / 70% LLM score).
4. **Paper analysis and indexing** — available PDFs are downloaded with size/content checks, converted by Docling, chunked by Markdown section, embedded, and analyzed into structured fields. An abstract is used when no indexed PDF text is available.
5. **Gap detection** — analyzed papers plus retrieved PDF chunks are used to produce cited gaps.
6. **Feasibility evaluation** — each gap becomes a proposed direction with a difficulty-derived score.
7. **Report generation** — data already persisted in PostgreSQL is formatted as a JSON response, Markdown report, and BibTeX bibliography.

Stage transitions and outputs are written to `pipeline_stage_checkpoints`; topic status, stage, progress, and error are written alongside them. On startup, unfinished workflows are reconstructed from Redis where possible and PostgreSQL otherwise, then resumed from completed checkpoints.

## Storage model

PostgreSQL is the source of truth for topics, papers, authors, topic-specific analysis/ranking, gaps, directions, documents, chunks, embedding generations, cleanup tasks, and stage checkpoints. Migrations are forward/backward Goose files in `migrations/`; generated query bindings are in `internal/storage/postgres` and must be regenerated with `just sqlc` after editing `queries.sql`.

Redis has two distinct roles:

- `pipeline:<topic-id>` holds a 24-hour live status snapshot for fast lookup and recovery.
- Redis Streams holds durable worker jobs when `pipeline.use_redis_queue` is enabled. Failed jobs are retried up to their job retry limit before becoming terminal.

Qdrant stores deterministic UUID points for abstract and PDF chunks. A physical collection is derived from the embedding identity (provider, model, dimensions, instruction version, indexing version). The stable configured alias identifies the active generation. The `reindex` command builds a new generation and switches the alias atomically only after a successful build.

## Concurrency and resilience

The worker pool handles PDF downloads, embedding batches, and paper analyses. It uses Redis Streams by default, but can use an in-process queue for development. The accelerator gate limits concurrent Ollama and Docling operations sharing local GPU memory. Provider clients also apply request limits, retries with jitter, and circuit breakers for source APIs and PDF downloads.

Server-side submission admission is a process-local token bucket. It limits expensive new runs but does not coordinate across multiple application instances.

## HTTP and observability

The Gin router serves versioned research endpoints under `/api/v1`, readiness/liveness endpoints under `/health`, JSON or console structured logs, and server-sent events per topic. SSE sends an immediate status event, later `status` and `progress` events, and a `ping` every 30 seconds. Slow subscribers can lose events rather than block the pipeline.

Run logs are placed beneath the configured logging directory. The full endpoint contract is in [API.md](API.md); configuration and operational commands are in [CONFIG.md](CONFIG.md) and [USAGE.md](USAGE.md).
