# Paper Scout

An AI-powered research analysis agent that automates academic paper discovery, analysis, and literature review generation. Give it a research topic and get back a structured report with relevant papers, identified research gaps, and proposed directions.

## Features

- **Multi-Source Discovery** - Searches Semantic Scholar and arXiv simultaneously with deduplication
- **LLM-Powered Analysis** - Uses Google Gemini for query expansion, paper analysis, gap detection, and feasibility evaluation
- **Vector-Based Ranking** - Embeds paper abstracts with Gemini embeddings (768-dim) and re-ranks with LLM scoring
- **PDF Processing** - Downloads and parses PDFs via GROBID (TEI XML extraction) with graceful fallback to abstracts
- **Research Gap Detection** - Identifies unexplored areas, conflicting results, and limitations across papers
- **Feasibility Scoring** - Evaluates research directions on difficulty, cost, industry viability, and time-to-MVP
- **Real-Time Progress** - Server-Sent Events (SSE) streaming for live pipeline status updates
- **Crash-Safe Recovery** - Postgres stage checkpoints resume runs from the first incomplete stage without replaying completed pipeline work
- **Multi-Format Output** - Generates Markdown reports, BibTeX references, and structured JSON

## Quick Start

```bash
# 1. Clone the repo
git clone https://github.com/your-username/paper-scout.git
cd paper-scout

# 2. Set up environment
cp .env.example .env
# Edit .env and add your Gemini API key:
# LLM__API_KEY=your_gemini_api_key_here

# 3. Start dependencies
docker compose up -d

# 4. Start the API server
go run ./cmd/server

# 5. Start a research query
curl -X POST http://localhost:8080/api/v1/research \
  -H "Content-Type: application/json" \
  -d '{"topic": "transformer architectures for time series forecasting"}'
```

`docker compose up -d` starts Postgres, Redis, Qdrant, and GROBID. The API server runs separately via `go run ./cmd/server`.

Fresh Postgres volumes are initialized from `docker/postgres-init/`, which contains only the final forward schema. Existing databases created from the legacy `migrations/001_initial_schema` migration must be upgraded with Goose using `migrations/002_topic_paper_membership.up.sql`.

## Architecture

### Pipeline Stages

```
INPUT: Research Topic
  │
  ▼
┌──────────────────┐
│ 1. Query         │  SYNCHRONOUS
│    Expansion     │  LLM expands topic into search queries, concepts, keywords
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ 2. Paper         │  SYNCHRONOUS
│    Discovery     │  Search Semantic Scholar + arXiv, deduplicate
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ 3. Ranking       │  SYNCHRONOUS
│                  │  Embed abstracts, search Qdrant, LLM re-rank top 50
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ 4. Paper         │  ASYNC (Worker Pool)
│    Analysis      │  Download PDFs via GROBID, LLM extracts structured data
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ 5. Gap           │  SYNCHRONOUS
│    Detection     │  Cross-paper analysis identifies research gaps
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ 6. Feasibility   │  SYNCHRONOUS
│    Evaluation    │  Score each gap on difficulty, cost, viability
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ 7. Report        │  SYNCHRONOUS
│    Generation    │  Assemble Markdown report + BibTeX references
└────────┬─────────┘
         │
         ▼
OUTPUT: Complete Research Report
```

### Reliability

- **Shared HTTP resilience policy** across Semantic Scholar, arXiv, GROBID, and PDF downloads
- **Circuit breaker and token-bucket rate limiting** per external service
- **Status-aware exponential backoff** with jitter and `Retry-After` support
- **Structured resilience events** for request, retry, throttle, and circuit-breaker observability
- **Graceful degradation** (PDF parse failure falls back to abstracts; LLM rerank failure falls back to embedding scores)
- **Discovery retry** with 3 query levels (full, broad, minimal)

## API

Base URL: `http://localhost:8080` after starting the server with `go run ./cmd/server`

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/research` | Start research pipeline |
| `GET` | `/api/v1/research/:id` | Get full research result |
| `GET` | `/api/v1/research/:id/status` | Get pipeline status only |
| `GET` | `/api/v1/research/:id/stream` | SSE stream for real-time updates |
| `GET` | `/api/v1/research/:id/report` | Download Markdown report |
| `GET` | `/api/v1/research/:id/bibtex` | Download BibTeX references |
| `GET` | `/health` | Readiness check for Postgres, Redis, Qdrant, and Gemini initialization |
| `GET` | `/health/live` | Liveness check; does not contact dependencies |
| `GET` | `/health/ready` | Readiness check for Postgres, Redis, Qdrant, and Gemini initialization |

### Start Research

```bash
curl -X POST http://localhost:8080/api/v1/research \
  -H "Content-Type: application/json" \
  -d '{"topic": "large language models for code generation"}'
```

Response:
```json
{
  "topic_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "pending",
  "message": "Research started"
}
```

### Get Research Result

```bash
curl http://localhost:8080/api/v1/research/{topic_id}
```

The result endpoint returns pipeline metadata together with the structured report:

```json
{
  "topic_id": "550e8400-e29b-41d4-a716-446655440000",
  "topic": "large language models for code generation",
  "status": "completed",
  "stage": "completed",
  "progress": 1,
  "papers": [],
  "research_gaps": [],
  "novel_directions": [],
  "executive_summary": "...",
  "literature_review": "...",
  "bibtex": "..."
}
```

Use `/api/v1/research/{topic_id}/status` when only pipeline status is needed.

### Stream Progress

```bash
curl http://localhost:8080/api/v1/research/{topic_id}/stream
```

Events:
```
event: status
data: {"stage": "query_expansion", "progress": 0.05}

event: status
data: {"stage": "paper_discovery", "progress": 0.15}
```

## Configuration

### Required

| Variable | Description |
|----------|-------------|
| `LLM__API_KEY` | Google Gemini API key |

### Key Settings

All defaults are in `config/default.yaml`. Override via environment variables using `__` as delimiter (e.g., `LLM__MODEL` overrides `llm.model`).

| Variable | Default | Description |
|----------|---------|-------------|
| `LLM__MODEL` | `gemini-2.5-flash` | LLM model |
| `LLM__EMBEDDING_MODEL` | `gemini-embedding-001` | Embedding model |
| `LLM__REQUESTS_PER_MINUTE` | `15` | Gemini rate limit |
| `LLM__REQUESTS_PER_DAY` | `1000` | Gemini daily limit |
| `DATABASE__QDRANT__USE_TLS` | `false` | Use TLS for Qdrant connections; required when a Qdrant API key is configured |
| `APIS__SEMANTIC_SCHOLAR__RESILIENCE__MAX_RETRIES` | `3` | Transient Semantic Scholar retries |
| `APIS__ARXIV__RESILIENCE__MAX_RETRIES` | `3` | Transient arXiv retries |
| `APIS__GROBID__RESILIENCE__MAX_RETRIES` | `2` | Transient GROBID retries |
| `APIS__GROBID__MAX_RESPONSE_BYTES` | `16777216` | Maximum GROBID XML response size |
| `PIPELINE__PDF_RESILIENCE__MAX_RETRIES` | `2` | Transient PDF download retries |
| `PIPELINE__PDF_MAX_BYTES` | `52428800` | Maximum PDF download size |
| `PIPELINE__MAX_PAPERS` | `50` | Max papers to discover |
| `PIPELINE__PAPERS_TO_ANALYZE` | `20` | Papers for deep analysis |
| `PIPELINE__WORKER_POOL_SIZE` | `10` | Concurrent workers |
| `SERVER__PORT` | `8080` | HTTP server port |

## Services

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| `postgres` | `postgres:17-alpine` | 5432 | Primary database |
| `redis` | `redis:8-alpine` | 6379 | Pipeline state and job queue |
| `qdrant` | `qdrant/qdrant:latest` | 6333 | Vector database (768-dim, cosine) |
| `grobid` | `lfoppiano/grobid:0.8.1` | 8070 | PDF parsing (TEI XML) |

## Development

### Prerequisites

- Go 1.24+
- Docker + Docker Compose
- [just](https://github.com/casey/just) command runner

### Commands

```bash
just setup          # Install tools, copy .env
just up             # Start services (detached)
just dev            # Run with hot reload
just run            # Run without hot reload
just test           # Run tests
just test-coverage  # Tests with coverage report
just fmt            # Format code
just lint           # Run linter
just check          # Vet + build
just down           # Stop services
just clean          # Stop and remove volumes
just logs           # Tail app logs
```

### Integration Tests

Routine tests are hermetic. PostgreSQL and Redis integration tests are enabled only
when disposable service endpoints are supplied:

```bash
PAPER_SCOUT_TEST_POSTGRES_DSN='postgres://research:research123@localhost:5432/research_agent?sslmode=disable' \
PAPER_SCOUT_TEST_REDIS_ADDR='localhost:6379' \
go test ./internal/storage/postgres ./internal/storage/redis
```

Use an isolated database and Redis instance: these tests create and remove test
records and streams.

## Project Structure

```
├── cmd/server/              # Application entry point
├── config/                  # Default configuration
├── internal/
│   ├── agent/               # Pipeline agents (discovery, ranking, analysis, etc.)
│   ├── api/                 # HTTP handlers, router, middleware
│   ├── circuitbreaker/      # Circuit breaker pattern
│   ├── config/              # Configuration loading and validation
│   ├── llm/                 # LLM abstraction (Gemini client, prompts, rate limiting)
│   ├── logger/              # Structured logging (zerolog)
│   ├── orchestrator/        # Pipeline orchestration and SSE streaming
│   ├── storage/
│   │   ├── postgres/        # PostgreSQL (pgx + sqlc)
│   │   ├── qdrant/          # Vector database client
│   │   └── redis/           # State and job queue
│   ├── tools/
│   │   ├── arxiv/           # arXiv API client
│   │   ├── bibtex/          # BibTeX citation generator
│   │   ├── embedding/       # Gemini embedding generator
│   │   ├── pdf/             # PDF download and GROBID parsing
│   │   └── semantic_scholar/ # Semantic Scholar API client
│   └── worker/              # Background job processing
├── migrations/              # Database migrations
├── docker-compose.yml       # Service orchestration
├── Justfile                 # Development commands
└── go.mod
```

## Tech Stack

- **Language**: Go 1.24
- **HTTP**: Gin
- **Database**: PostgreSQL 17 (pgx/v5 + sqlc)
- **Pipeline state/Queue**: Redis 8
- **Vector DB**: Qdrant
- **LLM**: Google Gemini (gemini-2.5-flash)
- **Embeddings**: Gemini (gemini-embedding-001, 768-dim)
- **PDF Parsing**: GROBID 0.8.1
- **Config**: koanf (YAML + env overlay)
- **Logging**: zerolog

## License

MIT
