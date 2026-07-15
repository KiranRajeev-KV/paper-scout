# Configuration

The server loads `config/default.yaml`, then overlays environment variables. Use uppercase keys with nesting separated by `__`, for example `PIPELINE__MAX_PAPERS=25`. Only the roots `SERVER`, `DATABASE`, `GENERATION`, `EMBEDDING`, `ACCELERATOR`, `APIS`, `PIPELINE`, and `LOGGING` are read. Copy `.env.example` to `.env` for local development; never commit `.env`.

Durations use Go syntax (`500ms`, `30s`, `2m`). `SERVER__ALLOWED_ORIGINS` is the one list-valued override and is comma-separated. All other overrides are parsed through the configuration library, so keep scalar values in their natural form.

## Server

| Key | Default | Meaning |
|---|---:|---|
| `SERVER__HOST` | `localhost` | Bind host. Use `0.0.0.0` in a container. |
| `SERVER__PORT` | `8080` | HTTP port. |
| `SERVER__READ_TIMEOUT` | `30s` | HTTP request read timeout. |
| `SERVER__WRITE_TIMEOUT` | `30s` | HTTP response write timeout. |
| `SERVER__ALLOWED_ORIGINS` | local ports 3000, 8080 | Comma-separated CORS allow-list. |
| `SERVER__SUBMISSION_RATE` | `0.1` | New-run tokens added per second. |
| `SERVER__SUBMISSION_BURST` | `2` | Maximum immediate submissions. |

## Databases and vector store

| Key | Default | Meaning |
|---|---:|---|
| `DATABASE__POSTGRES__HOST/PORT` | `localhost` / `5432` | PostgreSQL endpoint. |
| `DATABASE__POSTGRES__DATABASE` | `research_agent` | Database name. |
| `DATABASE__POSTGRES__USER/PASSWORD` | `research` / `research123` | Database credentials; replace outside local development. |
| `DATABASE__POSTGRES__SSLMODE` | `disable` | pgx SSL mode. |
| `DATABASE__POSTGRES__MAX_CONNECTIONS` | `10` | PostgreSQL pool maximum. |
| `DATABASE__POSTGRES__MAX_IDLE` | `5` | PostgreSQL pool minimum connections; cannot exceed maximum. |
| `DATABASE__POSTGRES__CONN_TIMEOUT` | `30s` | Connect timeout. |
| `DATABASE__REDIS__HOST/PORT` | `localhost` / `6379` | Redis endpoint. |
| `DATABASE__REDIS__PASSWORD` | empty | Redis password. |
| `DATABASE__REDIS__DB` | `0` | Redis logical database. |
| `DATABASE__REDIS__POOL_SIZE` | `10` | Normal Redis client pool. |
| `DATABASE__REDIS__WORKER_POOL_SIZE` | `10` | Connections reserved for blocking Stream consumers; at least the worker count when Redis queueing is enabled. |
| `DATABASE__QDRANT__HOST/PORT` | `localhost` / `6334` | Qdrant gRPC endpoint. |
| `DATABASE__QDRANT__ALIAS` | `paper_embeddings_current` | Stable alias for the active embedding generation. |
| `DATABASE__QDRANT__COLLECTION_PREFIX` | `paper_embeddings` | Prefix for physical generation collections. |
| `DATABASE__QDRANT__API_KEY` | empty | Qdrant API key; requires TLS. |
| `DATABASE__QDRANT__USE_TLS` | `false` | Enable TLS for Qdrant. |

## Generation and embeddings

`GENERATION__PROVIDER` must be `ollama` or `gemini`. Embeddings must use Ollama.

| Key | Default | Meaning |
|---|---:|---|
| `GENERATION__PROVIDER` | `ollama` | Text-generation backend. |
| `GENERATION__OLLAMA__BASE_URL` | `http://localhost:11434` | Ollama endpoint for generation. |
| `GENERATION__OLLAMA__MODEL` | `qwen3.5:4b-q4_K_M` | Installed generation model. |
| `GENERATION__OLLAMA__TIMEOUT/KEEP_ALIVE` | `2m` / `5m` | Per-call deadline and Ollama model retention. |
| `GENERATION__OLLAMA__CONCURRENCY` | `1` | Per-client concurrent calls. |
| `GENERATION__OLLAMA__THINK` | `false` | Enable model thinking output if supported. |
| `GENERATION__OLLAMA__MAX_OUTPUT_TOKENS` | `1024` | Generation cap. |
| `GENERATION__OLLAMA__TEMPERATURE` | `0` | Sampling temperature (0–2). |
| `GENERATION__GEMINI__API_KEY` | empty | Required when `GENERATION__PROVIDER=gemini`. |
| `GENERATION__GEMINI__MODEL` | `gemini-2.5-flash` | Gemini model name. |
| `GENERATION__GEMINI__MAX_RETRIES` | `3` | Retry count after the first attempt. |
| `GENERATION__GEMINI__BASE_BACKOFF/MAX_BACKOFF` | `500ms` / `2s` | Exponential retry bounds. |
| `GENERATION__GEMINI__TIMEOUT` | `30s` | Per-attempt deadline. |
| `GENERATION__GEMINI__MAX_OUTPUT_TOKENS` | `512` | Generation cap. |
| `GENERATION__GEMINI__REQUESTS_PER_MINUTE/DAY` | `15` / `1000` | In-process Gemini quota guard. |
| `EMBEDDING__PROVIDER` | `ollama` | Must be `ollama`. |
| `EMBEDDING__BASE_URL/MODEL` | local Ollama / `qwen3-embedding:8b` | Ollama embedding endpoint and installed model. |
| `EMBEDDING__TIMEOUT/KEEP_ALIVE/CONCURRENCY` | `2m` / `0` / `1` | Embedding deadline, model retention, and client concurrency. |
| `EMBEDDING__DIMENSIONS` | `4096` | Expected vector dimension; must match the model output. |
| `EMBEDDING__QUERY_INSTRUCTION` | retrieval instruction | Query prefix used for topic/gap retrieval. |
| `EMBEDDING__INSTRUCTION_VERSION` | `qwen3-retrieval-v1` | Compatibility identifier for the instruction. |
| `EMBEDDING__INDEXING_VERSION` | `v1` | Compatibility identifier for indexing rules. |
| `ACCELERATOR__MAX_CONCURRENT` | `1` | Shared cap for Ollama and Docling GPU-memory work. |

Changing any embedding identity field creates an incompatible generation. Run `just reindex` to build and activate a matching collection.

When using the Compose `app` profile with Gemini, set `GEMINI_API_KEY` in the shell or `.env`; Compose maps it to `GENERATION__GEMINI__API_KEY` inside the container. For a locally run server, use `GENERATION__GEMINI__API_KEY` directly.

## External APIs and document conversion

Each `RATE_LIMIT` block has `REQUESTS_PER_SECOND` and `BURST`. Each `RESILIENCE` block has `MAX_RETRIES`, `BASE_BACKOFF`, `MAX_BACKOFF`, `FAILURE_THRESHOLD`, and `OPEN_TIMEOUT`.

| Key group | Defaults | Meaning |
|---|---|---|
| `APIS__SEMANTIC_SCHOLAR__API_KEY` | empty | Optional Semantic Scholar key. |
| `APIS__SEMANTIC_SCHOLAR__BASE_URL/TIMEOUT` | Graph API / `30s` | Source endpoint and request timeout. |
| `APIS__SEMANTIC_SCHOLAR__RATE_LIMIT` | `0.33` RPS, burst `1` | Client-side request pacing. |
| `APIS__SEMANTIC_SCHOLAR__RESILIENCE` | 3 retries, 500ms–5s, threshold 5, 30s open | Retry/circuit-breaker policy. |
| `APIS__ARXIV__BASE_URL/TIMEOUT` | export API / `60s` | arXiv endpoint and request timeout. |
| `APIS__ARXIV__RATE_LIMIT/RESILIENCE` | same as Semantic Scholar | arXiv pacing and retry policy. |
| `APIS__DOCLING__BASE_URL` | `http://localhost:8000` | Docling service endpoint. |
| `APIS__DOCLING__REQUEST_TIMEOUT/DOCUMENT_TIMEOUT` | `10m` / `9m` | Outer request and Docling conversion deadline. |
| `APIS__DOCLING__OCR_BEHAVIOR` | `fallback` | `fallback`, `always`, or `never`. |
| `APIS__DOCLING__OUTPUT_FORMAT` | `md` | Supported output is Markdown only. |
| `APIS__DOCLING__CONCURRENCY` | `1` | Per-client conversion limit. |
| `APIS__DOCLING__VERSION` | `1.21.0` | Persisted parser provenance. |
| `APIS__DOCLING__MAX_RESPONSE_BYTES` | `33554432` | Maximum conversion response size. |
| `APIS__DOCLING__MIN_EXTRACTED_CHARACTERS` | `200` | Minimum useful extracted text. |

## Pipeline and logging

| Key | Default | Meaning |
|---|---:|---|
| `PIPELINE__MAX_PAPERS` | `50` | Maximum reconciled discovery results. |
| `PIPELINE__MIN_PAPERS_FOR_ANALYSIS` | `5` | Minimum discovery count; lower results fail the pipeline. |
| `PIPELINE__PAPERS_TO_ANALYZE` | `20` | Cap on ranked papers sent to PDF/index/analysis work. |
| `PIPELINE__WORKER_POOL_SIZE` | `10` | Worker count. |
| `PIPELINE__JOB_TIMEOUT` | `10m` | Paper-analysis job deadline. |
| `PIPELINE__PDF_DOWNLOAD_TIMEOUT` | `15s` | PDF download client timeout. |
| `PIPELINE__PDF_MAX_BYTES` | `52428800` | Maximum downloaded PDF size. |
| `PIPELINE__PDF_RATE_LIMIT__REQUESTS_PER_SECOND/BURST` | `1` / `2` | PDF request pacing. |
| `PIPELINE__PDF_RESILIENCE__*` | 2 retries, 500ms–5s, threshold 5, 30s open | PDF retry/circuit-breaker policy. |
| `PIPELINE__CHUNK_MAX_WORDS/CHUNK_OVERLAP` | `350` / `50` | PDF Markdown chunk size and overlap. |
| `PIPELINE__MAX_RETRIEVED_CHUNKS` | `12` | PDF chunks used as gap evidence. |
| `PIPELINE__PDF_INDEXING_TIMEOUT` | `10m` | Deadline for a paper-indexing batch. |
| `PIPELINE__EMBEDDING_BATCH_SIZE` | `10` | Chunks per embedding job. |
| `PIPELINE__USE_REDIS_QUEUE` | `true` | Use Redis Streams instead of an in-memory queue. |
| `LOGGING__LEVEL` | `info` | `debug`, `info`, `warn`, or `error`. |
| `LOGGING__FORMAT` | `console` | `console`, `json`, `development`, or `production`. |
| `LOGGING__DIRECTORY` | `logs` | Directory for application/run logs. |
