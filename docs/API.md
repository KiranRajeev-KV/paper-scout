# HTTP API

Base URL: `http://localhost:8080`. The research API is under `/api/v1`. Requests and normal responses are JSON unless stated otherwise. There is no built-in authentication; protect a deployed service at the network or proxy layer.

## Conventions

- Topic IDs and run IDs are UUID strings.
- Create requests require `Content-Type: application/json`.
- `status` is `pending`, `processing`, `completed`, or `failed`.
- Pipeline stages are `pending`, `query_expansion`, `paper_discovery`, `ranking`, `paper_analysis`, `gap_detection`, `feasibility_evaluation`, `report_generation`, `completed`, or `failed`.
- Common error body: `{"error":"…"}`.

## Create research

### `POST /api/v1/research`

Starts an asynchronous pipeline. Requests are governed by the process-local submission token bucket.

Request:

```json
{"topic":"Methods for reducing hallucinations in retrieval-augmented generation systems"}
```

`topic` is required and must be 10–500 characters.

Success — `202 Accepted`:

```json
{
  "topic_id":"b1f1d4a6-9bf4-4a1f-98d6-865721ed0d7b",
  "run_id":"37de27bb-461d-4d0b-ae8e-9ef9cd5cf5d3",
  "status":"pending",
  "message":"Research started. Use the topic_id to track progress."
}
```

Errors: `400` invalid/missing JSON or invalid topic; `429` submission limit exceeded (includes `Retry-After: 10`); `503` the research pipeline could not be started.

## Look up research

### `GET /api/v1/research/{id}`

Returns current pipeline state. While incomplete, report fields are empty and collection fields are empty arrays. Once completed, it also includes the generated report.

Success — `200 OK`:

```json
{
  "topic_id":"…", "run_id":"…", "topic":"…",
  "status":"completed", "stage":"completed", "progress":1,
  "started_at":"2026-07-15T10:00:00Z", "generated_at":"2026-07-15T10:05:00Z",
  "executive_summary":"# Executive Summary…",
  "literature_review":"# Literature Review…",
  "papers":[{"id":"…","title":"…","authors":["…"],"year":2025,"venue":"…","abstract":"…","problem_statement":"…","methodology":"…","key_findings":"…","limitations":"…","relevance_score":0.91}],
  "research_gaps":[{"gap_type":"unexplored","title":"…","description":"…","evidence":"paper-id,…"}],
  "novel_directions":[{"title":"…","description":"…","difficulty":"medium","estimated_cost":"…","industry_viability":"…","time_to_mvp":"…","feasibility_score":0.6}],
  "bibtex":"@article{…}"
}
```

If failed, `error` is included. Errors: `400` malformed UUID; `404` unknown topic; `503` state or completed report temporarily unavailable.

## Poll status

### `GET /api/v1/research/{id}/status`

Returns the lightweight progress representation.

```json
{"topic_id":"…","run_id":"…","status":"processing","stage":"paper_analysis","progress":0.53}
```

When present, `error` explains terminal failure. Errors: `400`, `404`, and `503` as above.

## Stream progress

### `GET /api/v1/research/{id}/stream`

Opens a `text/event-stream` connection. The server first sends a `status` event with the current state. It later sends `status` events when stages change, `progress` events during paper analysis, and a `ping` event every 30 seconds.

Example:

```text
event: status
data: {"topic_id":"…","run_id":"…","status":"processing","stage":"ranking","progress":0.25}

event: progress
data: {"topic_id":"…","stage":"paper_analysis","progress":0.5}

event: ping
data: {"time":1784102400}
```

Clients must tolerate dropped intermediate events and poll `/status` after reconnecting. Errors before the stream opens: `400` missing ID, `404` unknown topic, `503` state unavailable, `500` when streaming/write-deadline support is unavailable.

## Download Markdown report

### `GET /api/v1/research/{id}/report`

Returns `200 OK` with `Content-Type: text/markdown` and attachment filename `research-report-{id}.md`. The Markdown includes executive summary, literature review, gaps, directions, and a fenced BibTeX references section.

Errors: `400` malformed ID, `404` unknown topic, `409` pipeline is not completed, `503` report unavailable.

## Download BibTeX

### `GET /api/v1/research/{id}/bibtex`

Returns `200 OK` with `Content-Type: text/plain` and attachment filename `references-{id}.bib`.

Errors: `400` malformed ID, `404` unknown topic, `409` pipeline is not completed, `503` BibTeX unavailable.

## Health endpoints

### `GET /health/live`

Process liveness only. Always returns `200 OK` while the HTTP process can serve the route:

```json
{"status":"ok"}
```

### `GET /health` and `GET /health/ready`

Readiness check. PostgreSQL, Redis, Qdrant, the selected generator, Ollama embeddings, and Docling are checked concurrently with a two-second limit per dependency.

Healthy response — `200 OK`:

```json
{"status":"ok","services":{"postgres":"ok","redis":"ok","qdrant":"ok","generation":"ok","embedding":"ok","docling":"ok"}}
```

Any unavailable dependency produces `503 Service Unavailable` with `status: "degraded"` and the same per-service map.
