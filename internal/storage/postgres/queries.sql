-- name: CreateResearchTopic :one
INSERT INTO research_topics (topic, expanded_queries, status, config)
VALUES ($1, $2, $3, $4)
RETURNING *;

-- name: GetResearchTopic :one
SELECT * FROM research_topics WHERE id = $1;

-- name: GetResearchTopicByStatus :many
SELECT * FROM research_topics WHERE status = $1 ORDER BY created_at DESC;

-- name: ListRecoverableResearchTopics :many
SELECT * FROM research_topics
WHERE status NOT IN ('completed', 'failed')
ORDER BY created_at;

-- name: UpdateResearchTopicStatus :one
UPDATE research_topics 
SET status = $2, updated_at = NOW(), completed_at = COALESCE($3, completed_at)
WHERE id = $1
RETURNING *;

-- name: UpdateResearchTopicState :one
UPDATE research_topics
SET status = $2,
    current_stage = $3,
    progress = $4,
    error_message = $5,
    updated_at = NOW(),
    completed_at = CASE
        WHEN $2 = 'completed' THEN COALESCE(completed_at, NOW())
        ELSE completed_at
    END
WHERE id = $1
RETURNING *;

-- name: UpdateResearchTopicExpandedQueries :one
UPDATE research_topics 
SET expanded_queries = $2, updated_at = NOW()
WHERE id = $1
RETURNING *;

-- name: ListResearchTopics :many
SELECT * FROM research_topics ORDER BY created_at DESC LIMIT $1 OFFSET $2;

-- name: UpsertAuthorBySemanticScholarID :one
INSERT INTO authors (name, semantic_scholar_id)
VALUES ($1, $2)
ON CONFLICT (semantic_scholar_id) WHERE semantic_scholar_id IS NOT NULL DO UPDATE SET
    name = EXCLUDED.name
RETURNING *;

-- name: UpsertAuthorByName :one
INSERT INTO authors (name)
VALUES ($1)
ON CONFLICT (lower(btrim(name))) WHERE semantic_scholar_id IS NULL DO UPDATE SET
    name = EXCLUDED.name
RETURNING *;

-- name: CreatePaper :one
WITH paper AS (
    INSERT INTO papers (
        source, external_id, source_url, title, abstract,
        publication_date, venue, pdf_url
    )
    VALUES ($2, $3, $4, $5, $6, $7, $8, $9)
    ON CONFLICT (source, external_id) DO UPDATE SET
        title = EXCLUDED.title,
        abstract = EXCLUDED.abstract,
        source_url = EXCLUDED.source_url,
        publication_date = EXCLUDED.publication_date,
        venue = EXCLUDED.venue,
        pdf_url = EXCLUDED.pdf_url,
        updated_at = NOW()
    RETURNING id
)
INSERT INTO topic_papers (topic_id, paper_id, discovery_source)
SELECT $1, id, $2
FROM paper
ON CONFLICT (topic_id, paper_id) DO UPDATE SET
    discovery_source = EXCLUDED.discovery_source,
    updated_at = NOW()
RETURNING paper_id;

-- name: GetNextPaperAuthorPosition :one
SELECT COALESCE(MAX(position) + 1, 0)::int
FROM paper_authors
WHERE paper_id = $1;

-- name: GetPaper :one
SELECT * FROM papers WHERE id = $1;

-- name: GetPaperByExternalID :one
SELECT * FROM papers WHERE source = $1 AND external_id = $2;

-- name: GetPapersByTopic :many
SELECT p.* FROM papers p
JOIN topic_papers tp ON tp.paper_id = p.id
WHERE tp.topic_id = $1
ORDER BY tp.relevance_score DESC NULLS LAST;

-- name: GetPapersByTopicForAnalysis :many
SELECT p.*,
       tp.analysis AS topic_analysis,
       tp.relevance_score AS topic_relevance_score,
       ARRAY(
           SELECT a.name
           FROM paper_authors pa
           JOIN authors a ON a.id = pa.author_id
           WHERE pa.paper_id = p.id
           ORDER BY pa.position
       )::text[] AS authors
FROM papers p
JOIN topic_papers tp ON tp.paper_id = p.id
WHERE tp.topic_id = $1 AND tp.analysis IS NOT NULL
ORDER BY tp.relevance_score DESC NULLS LAST;

-- name: UpdatePaperAnalysis :exec
UPDATE topic_papers
SET analysis = $3, analysis_status = 'completed', updated_at = NOW()
WHERE topic_id = $1 AND paper_id = $2;

-- name: UpdatePaperRelevanceScore :exec
UPDATE topic_papers
SET relevance_score = $3, updated_at = NOW()
WHERE topic_id = $1 AND paper_id = $2;

-- name: UpdatePaperEmbeddingStatus :one
UPDATE papers 
SET embedding_status = $2, updated_at = NOW()
WHERE id = $1
RETURNING *;

-- name: UpdatePaperPDFStatus :one
UPDATE papers 
SET pdf_downloaded = $2, pdf_parsed = $3, updated_at = NOW()
WHERE id = $1
RETURNING *;

-- name: CountPapersByTopic :one
SELECT COUNT(*) FROM topic_papers WHERE topic_id = $1;

-- name: DeletePapersByTopic :exec
WITH removed AS (
    DELETE FROM topic_papers
    WHERE topic_papers.topic_id = $1
    RETURNING paper_id
)
DELETE FROM papers p
USING removed r
WHERE p.id = r.paper_id
  AND NOT EXISTS (
      SELECT 1 FROM topic_papers tp WHERE tp.paper_id = p.id
  );

-- name: AddPaperAuthor :exec
INSERT INTO paper_authors (paper_id, author_id, position)
VALUES ($1, $2, $3)
ON CONFLICT DO NOTHING;

-- name: GetPaperAuthors :many
SELECT a.* FROM authors a
JOIN paper_authors pa ON a.id = pa.author_id
WHERE pa.paper_id = $1
ORDER BY pa.position;

-- name: AddCitation :exec
INSERT INTO citations (citing_paper_id, cited_paper_id)
VALUES ($1, $2)
ON CONFLICT DO NOTHING;

-- name: GetPaperCitations :many
SELECT p.* FROM papers p
JOIN citations c ON p.id = c.cited_paper_id
WHERE c.citing_paper_id = $1;

-- name: GetPaperCitedBy :many
SELECT p.* FROM papers p
JOIN citations c ON p.id = c.citing_paper_id
WHERE c.cited_paper_id = $1;

-- name: CreateResearchGap :one
INSERT INTO research_gaps (topic_id, gap_type, title, description, related_paper_ids, evidence, feasibility)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (topic_id, title) DO UPDATE SET
    gap_type = EXCLUDED.gap_type,
    description = EXCLUDED.description,
    related_paper_ids = EXCLUDED.related_paper_ids,
    evidence = EXCLUDED.evidence,
    feasibility = EXCLUDED.feasibility
RETURNING *;

-- name: GetResearchGap :one
SELECT * FROM research_gaps WHERE id = $1;

-- name: GetResearchGapsByTopic :many
SELECT * FROM research_gaps WHERE topic_id = $1 ORDER BY created_at;

-- name: CreateNovelDirection :one
INSERT INTO novel_directions (
    topic_id, gap_id, title, description, rationale,
    feasibility_score, implementation_complexity, estimated_cost, 
    industry_viability, time_to_mvp
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (topic_id, title) DO UPDATE SET
    gap_id = EXCLUDED.gap_id,
    description = EXCLUDED.description,
    rationale = EXCLUDED.rationale,
    feasibility_score = EXCLUDED.feasibility_score,
    implementation_complexity = EXCLUDED.implementation_complexity,
    estimated_cost = EXCLUDED.estimated_cost,
    industry_viability = EXCLUDED.industry_viability,
    time_to_mvp = EXCLUDED.time_to_mvp
RETURNING *;

-- name: GetNovelDirection :one
SELECT * FROM novel_directions WHERE id = $1;

-- name: GetNovelDirectionsByTopic :many
SELECT * FROM novel_directions WHERE topic_id = $1 ORDER BY feasibility_score DESC NULLS LAST;

-- name: CreatePipelineRun :one
INSERT INTO pipeline_runs (topic_id, stage, status)
VALUES ($1, $2, $3)
RETURNING *;

-- name: GetPipelineRun :one
SELECT * FROM pipeline_runs WHERE id = $1;

-- name: GetLatestPipelineRun :one
SELECT * FROM pipeline_runs 
WHERE topic_id = $1 
ORDER BY started_at DESC 
LIMIT 1;

-- name: GetPipelineRunsByTopic :many
SELECT * FROM pipeline_runs WHERE topic_id = $1 ORDER BY started_at;

-- name: UpdatePipelineRunStatus :one
UPDATE pipeline_runs 
SET status = $2, completed_at = $3, error_message = $4, metrics = $5
WHERE id = $1
RETURNING *;

-- name: StartPipelineStage :one
INSERT INTO pipeline_stage_checkpoints (run_id, topic_id, stage, status, attempt, started_at, completed_at, error_message)
VALUES ($1, $2, $3, 'running', 1, NOW(), NULL, NULL)
ON CONFLICT (run_id, stage) DO UPDATE SET
    status = 'running',
    attempt = pipeline_stage_checkpoints.attempt + 1,
    started_at = NOW(),
    completed_at = NULL,
    error_message = NULL,
    updated_at = NOW()
RETURNING *;

-- name: CompletePipelineStage :one
UPDATE pipeline_stage_checkpoints
SET status = 'completed', output = $3, completed_at = NOW(), error_message = NULL, updated_at = NOW()
WHERE run_id = $1 AND stage = $2
RETURNING *;

-- name: FailPipelineStage :one
UPDATE pipeline_stage_checkpoints
SET status = 'failed', error_message = $3, updated_at = NOW()
WHERE run_id = $1 AND stage = $2
RETURNING *;

-- name: GetPipelineStage :one
SELECT * FROM pipeline_stage_checkpoints
WHERE run_id = $1 AND stage = $2;

-- name: GetPipelineStages :many
SELECT * FROM pipeline_stage_checkpoints
WHERE run_id = $1 ORDER BY started_at;

-- name: GetCompletedPaperIDsByTopic :many
SELECT paper_id FROM topic_papers
WHERE topic_id = $1 AND analysis_status = 'completed';

-- name: DeleteResearchTopic :exec
DELETE FROM research_topics WHERE id = $1;
