-- +goose Up
CREATE EXTENSION IF NOT EXISTS pgcrypto;

ALTER TABLE paper_chunks
    DROP CONSTRAINT paper_chunks_embedding_status_check;

ALTER TABLE paper_chunks
    ADD CONSTRAINT paper_chunks_embedding_status_check
        CHECK (embedding_status IN ('pending', 'indexing', 'completed', 'failed')),
    ADD COLUMN embedded_content_hash TEXT,
    ADD COLUMN section_heading TEXT,
    ADD COLUMN embedding_provider TEXT,
    ADD COLUMN embedding_model TEXT,
    ADD COLUMN embedding_dimensions INT CHECK (embedding_dimensions IS NULL OR embedding_dimensions > 0),
    ADD COLUMN embedding_instruction_version TEXT,
    ADD COLUMN embedding_indexing_version TEXT,
    ADD COLUMN qdrant_collection TEXT,
    ADD COLUMN qdrant_point_id UUID;

UPDATE paper_chunks
SET embedding_status = 'pending', error_message = NULL
WHERE embedding_status = 'completed';

INSERT INTO paper_chunks (topic_id, paper_id, chunk_type, chunk_index, text, content_hash, source)
SELECT tp.topic_id, tp.paper_id, 'abstract', 0, p.abstract,
       encode(digest(p.abstract, 'sha256'), 'hex'), 'paper_metadata'
FROM topic_papers tp
JOIN papers p ON p.id = tp.paper_id
WHERE p.abstract IS NOT NULL AND btrim(p.abstract) <> ''
ON CONFLICT (topic_id, paper_id, chunk_type, chunk_index) DO NOTHING;

CREATE TABLE embedding_generations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    dimensions INT NOT NULL CHECK (dimensions > 0),
    instruction_version TEXT NOT NULL,
    indexing_version TEXT NOT NULL,
    collection_name TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL CHECK (status IN ('building', 'active', 'retired', 'failed')),
    indexed_chunks BIGINT NOT NULL DEFAULT 0 CHECK (indexed_chunks >= 0),
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    activated_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (provider, model, dimensions, instruction_version, indexing_version)
);

CREATE UNIQUE INDEX idx_embedding_generations_one_active
    ON embedding_generations ((status)) WHERE status = 'active';

CREATE TABLE embedding_cleanup_tasks (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    collection_name TEXT NOT NULL,
    point_id UUID NOT NULL,
    topic_id UUID NOT NULL REFERENCES research_topics(id) ON DELETE CASCADE,
    paper_id UUID NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
    chunk_id UUID,
    reason TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'completed', 'failed')),
    attempts INT NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    UNIQUE (collection_name, point_id)
);

CREATE INDEX idx_embedding_cleanup_tasks_status ON embedding_cleanup_tasks(status, created_at);

CREATE TABLE paper_documents (
    paper_id UUID PRIMARY KEY REFERENCES papers(id) ON DELETE CASCADE,
    pdf_hash TEXT NOT NULL,
    parser_provider TEXT NOT NULL,
    parser_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('processing', 'completed', 'failed')),
    duration_ms BIGINT NOT NULL DEFAULT 0 CHECK (duration_ms >= 0),
    warnings JSONB NOT NULL DEFAULT '[]'::jsonb,
    error_message TEXT,
    markdown TEXT,
    parser_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TRIGGER update_embedding_generations_updated_at BEFORE UPDATE ON embedding_generations
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_embedding_cleanup_tasks_updated_at BEFORE UPDATE ON embedding_cleanup_tasks
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_paper_documents_updated_at BEFORE UPDATE ON paper_documents
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- +goose Down
DROP TRIGGER IF EXISTS update_paper_documents_updated_at ON paper_documents;
DROP TRIGGER IF EXISTS update_embedding_cleanup_tasks_updated_at ON embedding_cleanup_tasks;
DROP TRIGGER IF EXISTS update_embedding_generations_updated_at ON embedding_generations;
DROP TABLE IF EXISTS paper_documents;
DROP TABLE IF EXISTS embedding_cleanup_tasks;
DROP TABLE IF EXISTS embedding_generations;
ALTER TABLE paper_chunks
    DROP COLUMN IF EXISTS section_heading,
    DROP COLUMN IF EXISTS qdrant_point_id,
    DROP COLUMN IF EXISTS qdrant_collection,
    DROP COLUMN IF EXISTS embedding_indexing_version,
    DROP COLUMN IF EXISTS embedding_instruction_version,
    DROP COLUMN IF EXISTS embedding_dimensions,
    DROP COLUMN IF EXISTS embedding_model,
    DROP COLUMN IF EXISTS embedding_provider,
    DROP COLUMN IF EXISTS embedded_content_hash;
ALTER TABLE paper_chunks DROP CONSTRAINT IF EXISTS paper_chunks_embedding_status_check;
UPDATE paper_chunks SET embedding_status = 'pending' WHERE embedding_status = 'indexing';
ALTER TABLE paper_chunks ADD CONSTRAINT paper_chunks_embedding_status_check
    CHECK (embedding_status IN ('pending', 'completed', 'failed'));
