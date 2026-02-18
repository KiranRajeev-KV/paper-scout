# Research AI Agent - Project Specification

This document captures all architectural decisions, constraints, and requirements for the Research AI Agent project. All agents and developers must follow these specifications.

---

## Project Overview

Build a **Research AI Agent** that automates academic research analysis:

1. Takes a research topic as input
2. Finds relevant academic papers (arXiv, Semantic Scholar)
3. Ranks them by relevance
4. Reads abstracts and optionally full PDFs
5. Extracts structured data from papers
6. Identifies research gaps and opportunities
7. Suggests novel research directions
8. Generates structured output (JSON + Markdown + BibTeX)

---

## ⚠️ CRITICAL: Decision-First Development Protocol

**This is the most important rule. Violation is not acceptable.**

### The Rule

**NEVER write code without explicit user confirmation. NEVER assume defaults silently.**

When encountering any decision point (architectural, implementation, library choice, pattern selection, etc.), you MUST:

1. **STOP** - Do not proceed with implementation
2. **ASK** - Present the decision to the user with:
   - All available options
   - Merits of each option
   - Demerits of each option
   - Trade-offs involved
   - Your recommendation with reasoning
3. **WAIT** - Do not proceed until user confirms their choice
4. **DOCUMENT** - Record the decision in AGENTS.md if it affects architecture

### What Constitutes a Decision Point

Any of the following REQUIRE user input before proceeding:

- Choice of libraries or packages
- Design patterns or architectural approaches
- Error handling strategies
- Data structures or schemas
- API designs or contract changes
- Concurrency approaches
- Testing strategies
- Configuration approaches
- Any ambiguity in requirements

### Example of Proper Protocol

```
❌ WRONG (assuming defaults):
"I'll use pgx for Postgres since it's popular..."
[proceeds to write code]

✅ CORRECT (asking first):
"Which Postgres driver should we use?

Options:
1. pgx/v5
   - Merits: Fast, pure Go, connection pool built-in
   - Demerits: Slightly different API from database/sql
   - Trade-off: Performance vs familiarity

2. database/sql + pq
   - Merits: Standard library interface, widely known
   - Demerits: Slower, less features
   - Trade-off: Simplicity vs performance

Recommendation: pgx/v5 for performance and features.

Your choice?"
```

### When Decisions Are Already Documented

If AGENTS.md already contains a confirmed decision, you may proceed without asking again. Reference the documented decision in your implementation.

### Emergency Exception

Only bypass this rule if:
1. The decision is trivial (e.g., variable naming)
2. The implementation is fully reversible in < 1 minute
3. You clearly state the assumption and offer to revise

Even then, document what you assumed.

---

## Critical Constraints

### LLM Provider
- **Google Gemini API (free tier)** via official SDK
- Must support structured JSON outputs
- Streaming support required
- Configurable via `GEMINI_API_KEY` environment variable
- Model: `gemini-1.5-flash`
- Embedding model: `text-embedding-004`

### Language
- **Backend: Go (latest stable)**
- **No TypeScript**
- **No Python**

### Infrastructure
- Docker for everything
- Must be reproducible via: `docker compose up --build`

### Quality Rules
- **Never hallucinate citations** - only use API-returned metadata
- **No fake DOIs** - validate all metadata
- **Log every external call**
- **Avoid giant prompts** - use chunking
- **Use RAG properly**

---

## Library Choices

| Component | Library | Version |
|-----------|---------|---------|
| HTTP Framework | Gin | v1.9+ |
| Postgres Driver | pgx/v5 + sqlc | latest |
| Redis Client | go-redis/v9 | latest |
| Logging | zerolog | latest |
| Configuration | koanf | latest |
| Validation | ozzo-validation | v4 |
| UUID Generation | google/uuid | latest |
| Migrations | goose | latest |
| LLM SDK | google.golang.org/genai | latest |
| Vector DB Client | qdrant/go-client | latest |

### Logging Setup
- Separate logger configuration for development (pretty, colored) and production (JSON)
- Centralized logger initialization in `internal/logger/logger.go`
- All packages use the same logger instance

---

## Architectural Decisions

### 1. Service Architecture: Monolith

**Decision:** Single monolithic Go service with clean internal package separation.

**Rationale:**
- Simpler deployment and development
- Single codebase easier to manage
- Lower latency (no network hops between components)
- Can extract microservices later if needed

**Structure:**
```
/internal
  /agents       - Agent implementations
  /tools        - External API integrations
  /llm          - LLM abstraction
  /vector       - Vector store client
  /storage      - Database clients
  /orchestrator - Pipeline management
  /worker       - Worker pool
  /api          - HTTP handlers
  /config       - Configuration
```

### 2. Pipeline Execution: Hybrid (Sync + Async)

**Decision:** Synchronous for critical path, async job queue for heavy processing.

**Synchronous Stages:**
- Query Expansion
- Paper Discovery
- Ranking
- Gap Detection
- Feasibility Evaluation
- Report Generation

**Asynchronous Stages (Worker Pool):**
- Paper Analysis (PDF download, parsing, extraction)

**Rationale:**
- User gets quick feedback on discovery
- Heavy PDF processing doesn't block
- Resumable on failure via job queue

### 3. Vector Database: Qdrant

**Decision:** Qdrant running in Docker container.

**Rationale:**
- Purpose-built for vector search
- Fast filtering capabilities
- Rust-based (high performance)
- Good Go SDK available
- Native metadata filtering

**Collection Schema:**
```json
{
  "collection": "paper_embeddings",
  "vectors": {
    "size": 768,
    "distance": "Cosine"
  },
  "payload_schema": {
    "paper_id": "uuid",
    "topic_id": "uuid",
    "chunk_type": "keyword",
    "chunk_index": "integer",
    "source": "keyword"
  }
}
```

### 4. PDF Parsing: unstructured.io

**Decision:** Use unstructured.io running as Docker container.

**Rationale:**
- High quality parsing
- Handles tables, figures, complex layouts
- No Python in main service (runs in separate container)
- API-based integration

**Endpoint:** `http://unstructured:8000/general/v0/general/parsed`

### 5. Agent Orchestration: Hybrid Pipeline + Conditional Branches

**Decision:** Fixed pipeline stages with conditional branching based on intermediate results.

**Branches:**
- If < 5 papers found → retry with broader queries
- If no PDFs available → skip PDF analysis, use abstracts only
- If analysis fails for paper → continue with others, log error

**Rationale:**
- Predictable flow (easier debugging)
- Flexible enough to adapt to topic complexity
- No LLM planner overhead

### 6. Concurrency: Bounded Worker Pool

**Decision:** Worker pool with configurable size (default: 10 workers).

**Rationale:**
- Prevents resource exhaustion
- Provides backpressure
- Respects API rate limits
- Supports graceful shutdown via context
- Easy to monitor queue depth

**Why not unbounded goroutines:**
- Can spawn thousands, causing memory bloat
- No backpressure mechanism
- Can overwhelm external APIs

### 7. Caching: Redis (TTL-based)

**Decision:** Redis for all caching with configurable TTLs.

**Cache Keys:**
- `cache:paper:{source}:{external_id}` - Paper metadata
- `cache:embedding:{hash}` - Embedding vectors
- `cache:search:{query_hash}` - Search results

**TTLs:**
- Default: 24h
- Search results: 1h
- Embeddings: 168h (1 week)

### 8. Observability: Structured Logging Only

**Decision:** JSON structured logging now, OpenTelemetry integration later.

**Logging Requirements:**
- JSON format
- Structured fields (not string interpolation)
- Log every external API call
- Log LLM token usage
- Log pipeline stage transitions

**Future:** Add OpenTelemetry for traces and metrics when needed.

### 9. Rate Limiting: Token Bucket + Exponential Backoff

**Decision:** Client-side token bucket per API + retry with exponential backoff.

**Rate Limits:**
| API | Rate | Burst |
|-----|------|-------|
| Semantic Scholar (no key) | 5 req/s | 10 |
| arXiv | 1 req/s | 3 |
| Gemini (free tier) | 15 req/min | 5 |

**Retry Policy:**
- Max retries: 3
- Base backoff: 1s
- Max backoff: 30s
- Exponential multiplier: 2

### 10. Storage Schema: Hybrid (Normalized + JSONB)

**Decision:** Normalized tables for core entities, JSONB for LLM-extracted flexible data.

**Normalized Tables:**
- `research_topics`
- `papers` (core metadata)
- `authors`
- `paper_authors`
- `citations`

**JSONB Columns:**
- `papers.analysis` - LLM-extracted structured data
- `research_topics.expanded_queries` - Query expansion results
- `research_gaps.feasibility` - Feasibility analysis
- `pipeline_runs.metrics` - Performance metrics

### 11. State Persistence: Postgres + Redis Hybrid

**Decision:** Postgres for durable storage, Redis for transient pipeline state.

**Postgres:**
- All completed research
- Paper metadata
- Analysis results
- Citation graph

**Redis:**
- In-progress pipeline state
- Session data (TTL: 24h)
- Job queue
- Cache

### 12. Embeddings: Gemini Embeddings API

**Decision:** Use Gemini's text-embedding-004 model.

**Rationale:**
- Same provider as LLM (simple auth)
- Free tier available
- 768 dimensions
- Good quality for academic text

---

## Pipeline Stages

```
INPUT: Research Topic
         │
         ▼
┌─────────────────┐
│ 1. Query        │  SYNCHRONOUS
│    Expansion    │  - Expand topic with LLM
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 2. Paper        │  SYNCHRONOUS
│    Discovery    │  - Search Semantic Scholar + arXiv
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 3. Ranking      │  SYNCHRONOUS
│                 │  - Embed + similarity search + LLM re-rank
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 4. Decision     │  CONDITIONAL
│    Point        │  - Branch based on paper count
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 5. Paper        │  ASYNC (Worker Pool)
│    Analysis     │  - Download PDFs, parse, extract
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 6. Gap          │  SYNCHRONOUS
│    Detection    │  - Cross-paper synthesis
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 7. Feasibility  │  SYNCHRONOUS
│    Evaluation   │  - Score each gap
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ 8. Report       │  SYNCHRONOUS
│    Generation   │  - JSON + Markdown + BibTeX
└────────┬────────┘
         │
         ▼
OUTPUT: Complete Research Report
```

---

## API Endpoints

```
POST   /api/v1/research
  Body: {"topic": "transformer architectures for time series"}
  Response: {"topic_id": "uuid", "status": "pending"}

GET    /api/v1/research/{topic_id}
  Response: Full research result with status

GET    /api/v1/research/{topic_id}/status
  Response: {"status": "processing", "stage": "paper_analysis", "progress": 0.6}

GET    /api/v1/research/{topic_id}/stream
  Response: SSE stream with progress updates

GET    /api/v1/research/{topic_id}/report
  Response: Markdown report

GET    /api/v1/research/{topic_id}/bibtex
  Response: BibTeX entries

GET    /health
  Response: {"status": "ok", "services": {...}}
```

---

## Output Formats

### JSON Output
```json
{
  "topic": "string",
  "papers": [
    {
      "id": "uuid",
      "title": "string",
      "abstract": "string",
      "analysis": {
        "problem_statement": "string",
        "methodology": "string",
        "dataset": "string",
        "evaluation_metrics": ["string"],
        "limitations": "string",
        "future_work": "string"
      }
    }
  ],
  "research_gaps": [
    {
      "gap_type": "unexplored|conflicting|limitation",
      "title": "string",
      "description": "string",
      "evidence": "string"
    }
  ],
  "novel_directions": [
    {
      "title": "string",
      "description": "string",
      "feasibility": {
        "difficulty": "low|medium|high",
        "estimated_cost": "string",
        "industry_viability": "string",
        "time_to_mvp": "string"
      }
    }
  ],
  "feasibility_analysis": {
    "difficulty": "string",
    "estimated_cost": "string",
    "industry_viability": "string",
    "time_to_mvp": "string"
  }
}
```

### Markdown Report Sections
1. Executive Summary
2. Literature Review
3. Comparative Table of Papers
4. Identified Research Gaps
5. Proposed Research Directions
6. BibTeX References

---

## Configuration

### Environment Variables (Required)
```
GEMINI_API_KEY=your_gemini_api_key
```

### Environment Variables (Optional)
```
SEMANTIC_SCHOLAR_API_KEY=your_key  # Increases rate limit
POSTGRES_USER=research
POSTGRES_PASSWORD=research123
REDIS_PASSWORD=
```

### Configuration File
See `config/default.yaml` for all configurable options.

---

## Services (Docker Compose)

| Service | Image | Port | Purpose |
|---------|-------|------|---------|
| app | custom | 8080 | Main Go application |
| postgres | postgres:16-alpine | 5432 | Primary database |
| redis | redis:7-alpine | 6379 | Cache + job queue |
| qdrant | qdrant/qdrant:latest | 6333 | Vector database |
| unstructured | quay.io/unstructured-io/unstructured-api:latest | 8000 | PDF parsing |

---

## Project Configuration

### Max Papers to Analyze
- Default: 50
- Minimum for analysis: 5

### Worker Pool
- Default workers: 10
- Job timeout: 10 minutes
- PDF download timeout: 60 seconds

### Data Retention
- Store completed research indefinitely
- No auto-deletion

### Streaming
- SSE (Server-Sent Events) for real-time progress updates

---

## Reliability Requirements

### Must Implement
- [x] Retry with exponential backoff
- [x] Circuit breaker for API failures
- [x] Structured logging (JSON)
- [x] Graceful shutdown
- [x] Health check endpoint
- [x] Context cancellation throughout
- [x] Timeout controls on all external calls

### Security
- [x] API keys via environment variables only
- [x] No secrets in code
- [x] Config struct with validation on startup

---

## Development Commands (Justfile)

```just
# Build and run all services
build:
    docker compose up --build

# Run migrations
migrate:
    docker compose exec app /app/migrate

# Generate SQLC code
sqlc:
    sqlc generate

# Run tests
test:
    go test -v ./...

# View logs
logs:
    docker compose logs -f app

# Clean up
clean:
    docker compose down -v
```

---

## Implementation Checklist

### Phase 1: Foundation
- [x] go.mod with dependencies
- [x] .env.example
- [x] .gitignore
- [x] Justfile
- [x] config/default.yaml
- [x] internal/config/config.go
- [x] internal/config/validation.go
- [x] internal/logger/logger.go

### Phase 2: Database
- [x] migrations/001_initial_schema.up.sql
- [x] migrations/001_initial_schema.down.sql
- [x] sqlc.yaml
- [x] internal/storage/postgres/queries.sql
- [x] internal/storage/postgres/client.go
- [x] SQLC generated files (models.go, db.go, querier.go, queries.sql.go)

### Phase 3: Infrastructure
- [x] internal/storage/redis/client.go
- [x] internal/storage/redis/cache.go
- [x] internal/storage/redis/queue.go
- [x] internal/storage/qdrant/client.go

### Phase 4: LLM Layer
- [x] internal/llm/client.go
- [x] internal/llm/retry.go
- [x] internal/llm/structured.go
- [x] internal/llm/prompts/prompts.go

### Phase 5: Tools
- [x] internal/tools/semantic_scholar/client.go
- [x] internal/tools/semantic_scholar/models.go
- [x] internal/tools/semantic_scholar/rate_limiter.go
- [x] internal/tools/arxiv/client.go
- [x] internal/tools/arxiv/models.go
- [x] internal/tools/pdf/downloader.go
- [x] internal/tools/pdf/unstructured.go
- [x] internal/tools/pdf/chunker.go
- [x] internal/tools/embedding/gemini.go
- [x] internal/tools/bibtex/generator.go

### Phase 6: Worker Pool
- [x] internal/worker/pool.go
- [x] internal/worker/job.go
- [x] internal/worker/processor.go

### Phase 7: Agents
- [x] internal/agent/helpers.go
- [x] internal/agent/query_expander.go
- [x] internal/agent/paper_discoverer.go
- [x] internal/agent/ranker.go
- [x] internal/agent/analyzer.go
- [x] internal/agent/gap_detector.go
- [x] internal/agent/feasibility.go
- [x] internal/agent/report_generator.go

### Phase 8: Orchestrator
- [x] internal/orchestrator/orchestrator.go
- [x] internal/orchestrator/router.go
- [x] internal/orchestrator/state.go
- [x] internal/orchestrator/sse.go

### Phase 9: API
- [ ] internal/api/router.go
- [ ] internal/api/handler/*
- [ ] internal/api/middleware/*

### Phase 10: Entry Point
- [ ] cmd/server/main.go

### Phase 11: Docker
- [ ] Dockerfile
- [ ] docker-compose.yml
- [x] Justfile

---

## Notes for Agents

1. **⚠️ DECISION-FIRST PROTOCOL (MOST IMPORTANT)** - See "CRITICAL: Decision-First Development Protocol" section above. NEVER assume defaults. ALWAYS ask for user confirmation on any decision point with full options, merits, demerits, and tradeoffs.

2. **Always read this file first** before making changes to understand context.

3. **Follow the implementation order** - each phase depends on previous phases.

4. **Ensure code compiles** after each change. Run `go build ./...` frequently.

5. **No hallucinated data** - all paper metadata must come from real APIs.

6. **Use context for cancellation** - all long-running operations must respect context.

7. **Log external calls** - every API call should be logged with duration and status.

8. **Respect rate limits** - never exceed the defined rate limits for external APIs.

9. **Validate config on startup** - fail fast if required environment variables are missing.
