-- +goose Up
-- Remove unused contracts superseded by topic memberships, stage checkpoints,
-- paper chunks, and embedding generations.
DROP TABLE IF EXISTS citations;
DROP TABLE IF EXISTS pipeline_runs;

ALTER TABLE papers
    DROP COLUMN IF EXISTS embedding_status;

-- +goose Down
-- Recreate legacy contracts for rollback compatibility. Dropped legacy data
-- cannot be reconstructed and these objects remain non-authoritative.
ALTER TABLE papers
    ADD COLUMN embedding_status TEXT DEFAULT 'pending'
        CHECK (embedding_status IN ('pending', 'completed', 'failed'));

CREATE TABLE citations (
    citing_paper_id UUID NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
    cited_paper_id UUID NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
    PRIMARY KEY (citing_paper_id, cited_paper_id)
);

CREATE TABLE pipeline_runs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    topic_id UUID NOT NULL REFERENCES research_topics(id) ON DELETE CASCADE,
    stage TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed')),
    started_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    error_message TEXT,
    metrics JSONB
);

CREATE INDEX idx_pipeline_topic_id ON pipeline_runs(topic_id);
CREATE INDEX idx_pipeline_stage ON pipeline_runs(topic_id, stage);
