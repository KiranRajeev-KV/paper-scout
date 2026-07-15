# Limitations

## Research quality

- Results depend on Semantic Scholar, arXiv, available abstracts, accessible PDFs, Docling extraction, and the configured model. The service can miss relevant work, duplicate work, or include unsuitable work.
- LLM-produced analyses, rankings, gaps, feasibility assessments, and summaries are generated content. They require human review before citation, publication, funding, clinical, legal, or product decisions.
- A gap's evidence is derived from the stored paper IDs and retrieved chunks; it is not an exhaustive systematic-review methodology or proof that no prior work exists.
- Relevance scoring is model- and configuration-dependent. With an LLM enabled, the score combines vector similarity and generated ranking judgments.
- Only papers with usable abstracts are rankable. Full-text evidence is available only when a paper supplies a downloadable, accepted PDF that Docling can extract sufficiently.

## Operational behavior

- A completed stage is durable, but an in-flight worker job can be retried after interruption. Processing is therefore at-least-once at the job layer; persistence paths are designed to be idempotent where possible.
- Redis pipeline snapshots expire after 24 hours. PostgreSQL remains authoritative for completed and recoverable topic state.
- SSE is best-effort. Subscriber channels are bounded; a slow client can miss intermediate events and should re-fetch `/status`.
- Submission throttling is in-memory and per process. It is not a distributed rate limit.
- The built-in health check verifies configured dependencies and models. It does not prove that a full research run will complete or that third-party source results are useful.

## Deployment and security

- The checked-in defaults contain local development credentials and plaintext service endpoints. They are not production secrets or production-safe database settings.
- The application does not implement authentication or authorization. Put it behind an authenticated reverse proxy or add an auth layer before exposing it beyond a trusted network.
- CORS is an origin allow-list, not an authentication boundary.
- PDF URLs and source data come from external services. Downloads have MIME, signature, and size checks, but still involve processing untrusted documents. Run Docling and the service with appropriate isolation and resource limits.
- The Compose Docling service requests an NVIDIA GPU. A Linux host without compatible GPU/container support needs a compatible Docling deployment or a modified Compose configuration.

## Scope

- The embedding provider is currently Ollama only, even if generation uses Gemini.
- `cmd/reindex` rebuilds embeddings from persisted chunks; it does not rediscover papers or regenerate analyses.
- The API exposes only research creation, lookup, progress streaming, Markdown reports, BibTeX, and health checks. There are no endpoints to list, cancel, delete, or edit research topics.
