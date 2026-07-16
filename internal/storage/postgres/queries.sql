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

-- name: UpdatePaperPDFStatus :one
UPDATE papers 
SET pdf_downloaded = $2, pdf_parsed = $3, updated_at = NOW()
WHERE id = $1
RETURNING *;

-- name: UpsertPaperChunk :one
INSERT INTO paper_chunks (
    topic_id, paper_id, chunk_type, chunk_index, text, content_hash, source,
    embedding_status, error_message, section_heading
)
VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', NULL, $8)
ON CONFLICT (topic_id, paper_id, chunk_type, chunk_index) DO UPDATE SET
    text = EXCLUDED.text,
    content_hash = EXCLUDED.content_hash,
    source = EXCLUDED.source,
    section_heading = EXCLUDED.section_heading,
    embedding_status = CASE
        WHEN paper_chunks.content_hash = EXCLUDED.content_hash THEN paper_chunks.embedding_status
        ELSE 'pending'
    END,
	 embedded_content_hash = CASE WHEN paper_chunks.content_hash = EXCLUDED.content_hash THEN paper_chunks.embedded_content_hash ELSE NULL END,
	 embedding_provider = CASE WHEN paper_chunks.content_hash = EXCLUDED.content_hash THEN paper_chunks.embedding_provider ELSE NULL END,
	 embedding_model = CASE WHEN paper_chunks.content_hash = EXCLUDED.content_hash THEN paper_chunks.embedding_model ELSE NULL END,
	 embedding_dimensions = CASE WHEN paper_chunks.content_hash = EXCLUDED.content_hash THEN paper_chunks.embedding_dimensions ELSE NULL END,
	 embedding_instruction_version = CASE WHEN paper_chunks.content_hash = EXCLUDED.content_hash THEN paper_chunks.embedding_instruction_version ELSE NULL END,
	 embedding_indexing_version = CASE WHEN paper_chunks.content_hash = EXCLUDED.content_hash THEN paper_chunks.embedding_indexing_version ELSE NULL END,
	 qdrant_collection = CASE WHEN paper_chunks.content_hash = EXCLUDED.content_hash THEN paper_chunks.qdrant_collection ELSE NULL END,
	 qdrant_point_id = CASE WHEN paper_chunks.content_hash = EXCLUDED.content_hash THEN paper_chunks.qdrant_point_id ELSE NULL END,
    error_message = CASE
        WHEN paper_chunks.content_hash = EXCLUDED.content_hash THEN paper_chunks.error_message
        ELSE NULL
    END,
    updated_at = NOW()
RETURNING *;

-- name: DeleteStalePaperChunks :many
DELETE FROM paper_chunks
WHERE topic_id = $1
  AND paper_id = $2
  AND chunk_type = $3
  AND chunk_index >= $4
RETURNING *;

-- name: GetPaperChunks :many
SELECT * FROM paper_chunks
WHERE topic_id = $1 AND paper_id = $2
ORDER BY chunk_type, chunk_index;

-- name: GetCompletedPaperChunks :many
SELECT * FROM paper_chunks
WHERE topic_id = $1
  AND paper_id = $2
  AND embedding_status = 'completed'
ORDER BY chunk_type, chunk_index;

-- name: UpdatePaperChunkEmbeddingStatus :one
UPDATE paper_chunks
SET embedding_status = $3,
    error_message = $4,
    updated_at = NOW()
WHERE topic_id = $1 AND id = $2
RETURNING *;

-- name: MarkPaperChunkEmbeddingIndexing :one
UPDATE paper_chunks
SET embedding_status = 'indexing',
    embedding_provider = $3,
    embedding_model = $4,
    embedding_dimensions = $5,
    embedding_instruction_version = $6,
    embedding_indexing_version = $7,
    qdrant_collection = $8,
    qdrant_point_id = $9,
    error_message = NULL,
    updated_at = NOW()
WHERE topic_id = $1 AND id = $2
RETURNING *;

-- name: CompletePaperChunkEmbedding :one
UPDATE paper_chunks
SET embedding_status = 'completed',
    embedded_content_hash = content_hash,
    error_message = NULL,
    updated_at = NOW()
WHERE topic_id = $1 AND id = $2 AND qdrant_point_id = $3
RETURNING *;

-- name: ListPaperChunksForReindex :many
SELECT * FROM paper_chunks
ORDER BY topic_id, paper_id, chunk_type, chunk_index;

-- name: LockPaperChunksForEmbeddingActivation :exec
LOCK TABLE paper_chunks IN SHARE ROW EXCLUSIVE MODE;

-- name: CreateEmbeddingGeneration :one
INSERT INTO embedding_generations (
    id, provider, model, dimensions, instruction_version, indexing_version, collection_name, status
) VALUES ($1, $2, $3, $4, $5, $6, $7, 'building')
RETURNING *;

-- name: MarkEmbeddingGenerationReady :one
UPDATE embedding_generations
SET status = 'ready', indexed_chunks = $2, error_message = NULL, updated_at = NOW()
WHERE id = $1 AND status = 'building'
RETURNING *;

-- name: FailEmbeddingGeneration :one
UPDATE embedding_generations
SET status = 'failed', error_message = $2, updated_at = NOW()
WHERE id = $1 AND status IN ('building', 'ready')
RETURNING *;

-- name: CreateEmbeddingActivationIntent :one
INSERT INTO embedding_activation_intents (
    generation_id, target_collection, alias_name, previous_collection,
    expected_point_count, chunk_snapshot_digest, status, attempts, last_error
) VALUES ($1, $2, $3, $4, $5, $6, 'pending', 0, NULL)
RETURNING *;

-- name: GetPendingEmbeddingActivationIntent :one
SELECT * FROM embedding_activation_intents
WHERE status IN ('pending', 'alias_switched', 'failed')
ORDER BY created_at
LIMIT 1;

-- name: GetEmbeddingActivationIntent :one
SELECT * FROM embedding_activation_intents WHERE id = $1;

-- name: MarkEmbeddingActivationAliasSwitched :one
UPDATE embedding_activation_intents
SET status = 'alias_switched', attempts = attempts + 1, last_error = NULL, updated_at = NOW()
WHERE id = $1 RETURNING *;

-- name: CompleteEmbeddingActivationIntent :one
UPDATE embedding_activation_intents
SET status = 'completed', last_error = NULL, completed_at = COALESCE(completed_at, NOW()), updated_at = NOW()
WHERE id = $1 RETURNING *;

-- name: FailEmbeddingActivationIntent :one
UPDATE embedding_activation_intents
SET status = 'failed', last_error = $2, updated_at = NOW()
WHERE id = $1 RETURNING *;

-- name: SupersedeEmbeddingActivationIntent :one
UPDATE embedding_activation_intents
SET status = 'superseded', last_error = $2, updated_at = NOW()
WHERE id = $1 RETURNING *;

-- name: GetActiveEmbeddingGeneration :one
SELECT * FROM embedding_generations WHERE status = 'active';

-- name: GetEmbeddingGeneration :one
SELECT * FROM embedding_generations WHERE id = $1;

-- name: UpdateEmbeddingGenerationProgress :one
UPDATE embedding_generations SET indexed_chunks = $2, error_message = $3, updated_at = NOW()
WHERE id = $1 RETURNING *;

-- name: RetireActiveEmbeddingGenerations :exec
UPDATE embedding_generations SET status = 'retired', updated_at = NOW()
WHERE status = 'active' AND id <> $1;

-- name: ActivateEmbeddingGeneration :one
UPDATE embedding_generations
SET status = 'active', activated_at = COALESCE(activated_at, NOW()), error_message = NULL, updated_at = NOW()
WHERE id = $1 RETURNING *;

-- name: MarkPaperChunkForActiveGeneration :one
UPDATE paper_chunks
SET embedding_status = 'completed',
    embedded_content_hash = content_hash,
    embedding_provider = $4,
    embedding_model = $5,
    embedding_dimensions = $6,
    embedding_instruction_version = $7,
    embedding_indexing_version = $8,
    qdrant_collection = $9,
    qdrant_point_id = $10,
    error_message = NULL,
    updated_at = NOW()
WHERE topic_id = $1 AND id = $2 AND content_hash = $3
RETURNING *;

-- name: CreateEmbeddingCleanupTask :one
INSERT INTO embedding_cleanup_tasks (collection_name, point_id, topic_id, paper_id, chunk_id, reason)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (collection_name, point_id) DO UPDATE SET
    status = CASE WHEN embedding_cleanup_tasks.status = 'completed' THEN 'completed' ELSE 'pending' END,
    reason = EXCLUDED.reason,
    error_message = NULL,
    updated_at = NOW()
RETURNING *;

-- name: ListPendingEmbeddingCleanupTasks :many
SELECT * FROM embedding_cleanup_tasks
WHERE status IN ('pending', 'failed')
ORDER BY created_at LIMIT $1;

-- name: CompleteEmbeddingCleanupTask :one
UPDATE embedding_cleanup_tasks
SET status = 'completed', attempts = attempts + 1, error_message = NULL, completed_at = NOW(), updated_at = NOW()
WHERE id = $1 RETURNING *;

-- name: FailEmbeddingCleanupTask :one
UPDATE embedding_cleanup_tasks
SET status = 'failed', attempts = attempts + 1, error_message = $2, updated_at = NOW()
WHERE id = $1 RETURNING *;

-- name: UpsertPaperDocument :one
INSERT INTO paper_documents (
    paper_id, pdf_hash, parser_provider, parser_version, status, duration_ms, warnings, error_message, markdown, parser_json
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (paper_id) DO UPDATE SET
    pdf_hash = EXCLUDED.pdf_hash,
    parser_provider = EXCLUDED.parser_provider,
    parser_version = EXCLUDED.parser_version,
    status = EXCLUDED.status,
    duration_ms = EXCLUDED.duration_ms,
    warnings = EXCLUDED.warnings,
    error_message = EXCLUDED.error_message,
    markdown = EXCLUDED.markdown,
    parser_json = EXCLUDED.parser_json,
    updated_at = NOW()
RETURNING *;

-- name: GetPaperDocument :one
SELECT * FROM paper_documents WHERE paper_id = $1;

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
