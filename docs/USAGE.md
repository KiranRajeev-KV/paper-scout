# Usage

These commands use the example topic: **“Methods for reducing hallucinations in retrieval-augmented generation systems.”** Run them from the repository root on Linux.

## Prerequisites

Install Docker Engine with Compose v2, Go 1.24 or newer, and an Ollama server. The provided Docling container requests an NVIDIA GPU; use a Linux host with the NVIDIA Container Toolkit, or deploy/adjust Docling separately. Install `just` if you want the task shortcuts.

```bash
go version
docker compose version
ollama --version
just --version
```

## First local run

```bash
cp .env.example .env
docker compose up -d
ollama pull qwen3.5:4b-q4_K_M
ollama pull qwen3-embedding:8b
go install github.com/pressly/goose/v3/cmd/goose@latest
just migrate
just run
```

If you use Gemini for generation, set `GENERATION__PROVIDER=gemini` and `GENERATION__GEMINI__API_KEY` in `.env`. Embeddings still require Ollama. See [CONFIG.md](CONFIG.md).

Useful alternatives:

```bash
just up                 # start backing services
just build              # build and start backing services
just dev                # hot-reload server (requires air)
just build-local        # build bin/server
docker compose --profile app up -d --build app  # run the server in Compose
```

Run database migrations before starting the application. The server does not apply them automatically.

## Submit, follow, and fetch a report

With `just run` active in one terminal:

```bash
TOPIC='Methods for reducing hallucinations in retrieval-augmented generation systems'
curl -sS http://localhost:8080/api/v1/research \
  -H 'Content-Type: application/json' \
  --data "{\"topic\":\"$TOPIC\"}" | tee /tmp/paper-scout-submit.json
```

Copy the returned `topic_id` into `TOPIC_ID` (or extract it with `jq`):

```bash
TOPIC_ID="$(jq -r .topic_id /tmp/paper-scout-submit.json)"
curl -sS "http://localhost:8080/api/v1/research/$TOPIC_ID/status" | jq
curl -N "http://localhost:8080/api/v1/research/$TOPIC_ID/stream"
curl -sS "http://localhost:8080/api/v1/research/$TOPIC_ID" | jq
curl -sS "http://localhost:8080/api/v1/research/$TOPIC_ID/report" -o report.md
curl -sS "http://localhost:8080/api/v1/research/$TOPIC_ID/bibtex" -o references.bib
```

The report and BibTeX endpoints return `409 Conflict` until the status is `completed`.

## Rebuild embeddings

After changing any embedding model, dimensions, instruction version, or indexing version, build a new generation and run recoverable activation:

```bash
just reindex
```

The command is interruptible with `Ctrl-C`; it does not discover papers or re-run reports.

Inspect activation with the following commands. A successful reindex leaves exactly one active database generation, an alias that points to its collection, and no `pending`, `alias_switched`, or `failed` intent. The previous active collection remains available for recovery and is never deleted by this workflow.

```bash
curl -sS http://localhost:6333/aliases | jq
docker compose exec postgres psql -U research -d research_agent \
  -c "SELECT id, collection_name, status, activated_at FROM embedding_generations WHERE status = 'active';"
docker compose exec postgres psql -U research -d research_agent \
  -c "SELECT id, generation_id, target_collection, expected_alias, previous_collection, status, attempts, last_error, created_at, updated_at FROM embedding_activation_intents WHERE status <> 'completed' ORDER BY created_at;"
```

If startup reports an incomplete activation, inspect those results, correct the reported Qdrant/database problem, and restart the server. Startup performs idempotent reconciliation only; it does not begin a full reindex automatically. If reconciliation reports an unexpected alias target, it fails closed and leaves the alias unchanged.

## Tests and quality checks

```bash
just test
just test-coverage
just fmt
just lint
just check
just sqlc
```

`just check` verifies formatting, vet, tests, and a build. `just sqlc` regenerates the checked-in SQL bindings after a deliberate query/schema change.

## Linux debugging

Start with readiness. It checks PostgreSQL, Redis, Qdrant, generation, embedding, and Docling; liveness only checks the HTTP process.

```bash
curl -sS http://localhost:8080/health/live | jq
curl -sS http://localhost:8080/health | jq
docker compose ps
docker compose logs --tail=100 postgres redis qdrant docling
```

Check local models and the parser:

```bash
ollama list
curl -fsS http://localhost:11434/api/tags | jq
curl -fsS http://localhost:8000/health
```

Inspect the topic's durable workflow and work queue (substitute the actual UUID):

```bash
docker compose exec postgres psql -U research -d research_agent \
  -c "SELECT id, status, current_stage, progress, error_message FROM research_topics WHERE id = '$TOPIC_ID';"
docker compose exec postgres psql -U research -d research_agent \
  -c "SELECT stage, status, attempt, error_message FROM pipeline_stage_checkpoints WHERE topic_id = '$TOPIC_ID' ORDER BY created_at;"
docker compose exec redis redis-cli GET "pipeline:$TOPIC_ID"
docker compose exec redis redis-cli XLEN jobs:stream
docker compose exec redis redis-cli XPENDING jobs:stream jobs:workers
docker compose exec redis redis-cli LLEN jobs:failed
```

Inspect the active vector alias and application logs:

```bash
curl -sS http://localhost:6333/aliases | jq
find logs -type f -name '*.jsonl' -print
tail -f logs/app/*.jsonl
```

For a failed run, capture the HTTP state, the matching run log, and the checkpoint error before retrying. A restart resumes incomplete persisted pipelines; it does not restart a pipeline already marked `failed`.

## Stop and reset

```bash
just down
just clean
```

`just clean` removes PostgreSQL, Redis, Qdrant, and application-log volumes. Treat it as destructive local-data reset.
