-- 003_pipeline_stage_checkpoints.up.sql
-- Persist workflow identity and stage checkpoints for crash-safe resumption.

ALTER TABLE research_topics
    ADD COLUMN run_id UUID DEFAULT uuid_generate_v4();

UPDATE research_topics
SET run_id = uuid_generate_v4()
WHERE run_id IS NULL;

ALTER TABLE research_topics
    ALTER COLUMN run_id SET NOT NULL;

ALTER TABLE research_topics
    ADD CONSTRAINT research_topics_run_id_key UNIQUE (run_id);

CREATE TABLE pipeline_stage_checkpoints (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    run_id UUID NOT NULL REFERENCES research_topics(run_id) ON DELETE CASCADE,
    topic_id UUID NOT NULL REFERENCES research_topics(id) ON DELETE CASCADE,
    stage TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed')),
    output JSONB,
    attempt INT NOT NULL DEFAULT 1,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(run_id, stage)
);

CREATE INDEX idx_pipeline_stage_checkpoints_run ON pipeline_stage_checkpoints(run_id, stage);
CREATE INDEX idx_pipeline_stage_checkpoints_topic ON pipeline_stage_checkpoints(topic_id, stage);

DELETE FROM research_gaps older
USING research_gaps newer
WHERE older.topic_id = newer.topic_id
  AND older.title = newer.title
  AND older.id > newer.id;

DELETE FROM novel_directions older
USING novel_directions newer
WHERE older.topic_id = newer.topic_id
  AND older.title = newer.title
  AND older.id > newer.id;

CREATE UNIQUE INDEX idx_research_gaps_topic_title ON research_gaps(topic_id, title);
CREATE UNIQUE INDEX idx_novel_directions_topic_title ON novel_directions(topic_id, title);

CREATE TRIGGER update_pipeline_stage_checkpoints_updated_at BEFORE UPDATE ON pipeline_stage_checkpoints
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
