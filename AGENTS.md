# Paper Scout agent guide

## Scope

Paper Scout is a Go 1.24 asynchronous research API. Read the relevant implementation and tests before changing behavior; do not infer contracts from file names. HTTP behavior lives in `internal/api`, pipeline behavior in `internal/orchestrator` and `internal/agent`, and persistence contracts in `migrations/` plus `internal/storage/postgres/queries.sql`.

## Working safely

- Preserve unrelated worktree changes. Do not reset, checkout, remove volumes, or delete data unless the user explicitly asks.
- Never commit `.env`, API keys, passwords, logs, generated binaries, or research data.
- Treat external paper metadata, PDFs, and model output as untrusted input. Keep existing size bounds, validation, and structured-output checks when changing those paths.
- Keep PostgreSQL authoritative; Redis snapshots and queues are recoverability/performance mechanisms, not the source of truth.
- Maintain durable stage checkpoints and idempotent/retry-safe persistence when changing pipeline stages or worker jobs.

## Configuration and schema changes

- Add a configuration field consistently: `config/default.yaml`, `.env.example`, config structs/validation, and `docs/CONFIG.md`.
- Update `migrations/` with a new numbered Goose migration; do not rewrite an applied migration.
- After changing `internal/storage/postgres/queries.sql` or `sqlc.yaml`, run `just sqlc` and commit the generated `queries.sql.go` changes.
- An embedding identity change requires a fresh `just reindex`; do not silently reuse an incompatible Qdrant generation.

## Commands

Use the repository task runner when practical:

```bash
just test
just check
just fmt
just sqlc
just migrate
just reindex
```

`just check` runs formatting verification, `go vet`, tests, and a build. Integration-style tests may need PostgreSQL, Redis, Qdrant, Docling, and local Ollama as described in `docs/USAGE.md`.

## Tests and documentation

- Add or update focused tests for behavior changes, including error/retry paths where applicable.
- Run the narrowest relevant test first, then `just check` when feasible.
- Keep public endpoint changes synchronized with `docs/API.md`; keep operational changes synchronized with `README.md`, `docs/USAGE.md`, and `docs/ARCH.md` as applicable.
- Keep documentation concise, concrete, and limited to verified behavior.

## Code conventions

- Format Go with `gofmt`/`just fmt`; preserve the existing package layout and dependency direction.
- Pass `context.Context` through network, database, and worker operations; bound external calls with configured timeouts.
- Use the existing structured logger and avoid logging secrets, full prompts, or document bodies.
- Prefer existing resilience, queue, embedding, and storage helpers over duplicate implementations.
