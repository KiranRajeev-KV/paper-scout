-- 003_pipeline_stage_checkpoints.down.sql

DROP TRIGGER IF EXISTS update_pipeline_stage_checkpoints_updated_at ON pipeline_stage_checkpoints;
DROP INDEX IF EXISTS idx_novel_directions_topic_title;
DROP INDEX IF EXISTS idx_research_gaps_topic_title;
DROP INDEX IF EXISTS idx_pipeline_stage_checkpoints_topic;
DROP INDEX IF EXISTS idx_pipeline_stage_checkpoints_run;
DROP TABLE IF EXISTS pipeline_stage_checkpoints;
ALTER TABLE research_topics DROP CONSTRAINT IF EXISTS research_topics_run_id_key;
ALTER TABLE research_topics DROP COLUMN IF EXISTS run_id;
