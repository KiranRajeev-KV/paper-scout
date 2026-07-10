-- name: CreateResearchTopic :one
INSERT INTO research_topics (topic, expanded_queries, status, config)
VALUES ($1, $2, $3, $4)
RETURNING *;

-- name: GetResearchTopic :one
SELECT * FROM research_topics WHERE id = $1;

-- name: GetResearchTopicByStatus :many
SELECT * FROM research_topics WHERE status = $1 ORDER BY created_at DESC;

-- name: UpdateResearchTopicStatus :one
UPDATE research_topics 
SET status = $2, updated_at = NOW(), completed_at = COALESCE($3, completed_at)
WHERE id = $1
RETURNING *;

-- name: UpdateResearchTopicExpandedQueries :one
UPDATE research_topics 
SET expanded_queries = $2, updated_at = NOW()
WHERE id = $1
RETURNING *;

-- name: ListResearchTopics :many
SELECT * FROM research_topics ORDER BY created_at DESC LIMIT $1 OFFSET $2;

-- name: CreateAuthor :one
INSERT INTO authors (name, semantic_scholar_id, orcid)
VALUES ($1, $2, $3)
ON CONFLICT DO NOTHING
RETURNING *;

-- name: GetAuthorBySemanticScholarID :one
SELECT * FROM authors WHERE semantic_scholar_id = $1;

-- name: CreatePaper :exec
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
    updated_at = NOW();

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
SELECT p.*, tp.analysis AS topic_analysis, tp.relevance_score AS topic_relevance_score
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

-- name: DeleteResearchTopic :exec
DELETE FROM research_topics WHERE id = $1;
