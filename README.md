# Paper Scout

Paper Scout is a self-hosted API that turns a research topic into a cited literature report. It discovers papers from Semantic Scholar and arXiv, ranks and analyzes them, indexes available PDFs, identifies research gaps, and produces Markdown and BibTeX output.

It is written in Go and uses PostgreSQL for durable state, Redis for work queues and live snapshots, Qdrant for embeddings, Ollama or Gemini for generation, and Docling for PDF extraction.

## Quick start

On Linux with Docker, Go 1.24+, and Ollama installed:

```bash
cp .env.example .env
docker compose up -d
ollama pull qwen3.5:4b-q4_K_M
ollama pull qwen3-embedding:8b
go install github.com/pressly/goose/v3/cmd/goose@latest
just migrate
just run
```

In another terminal, submit a topic:

```bash
curl -sS http://localhost:8080/api/v1/research \
  -H 'Content-Type: application/json' \
  -d '{"topic":"Methods for reducing hallucinations in retrieval-augmented generation systems"}'
```

Use the returned `topic_id` with the status, stream, report, and BibTeX endpoints. See [Usage](docs/USAGE.md), [configuration](docs/CONFIG.md), and the complete [API reference](docs/API.md).

## Documentation

- [Architecture](docs/ARCH.md)
- [Usage and Linux debugging](docs/USAGE.md)
- [Configuration reference](docs/CONFIG.md)
- [HTTP API reference](docs/API.md)
- [Known limitations](docs/LIMITATIONS.md)
- [Contributor and agent guide](AGENTS.md)

## Development

```bash
just test
just check
just fmt
```

`just up` starts backing services; `just down` stops them; `just clean` also removes their volumes. Do not run `clean` if you need to retain local research data.
