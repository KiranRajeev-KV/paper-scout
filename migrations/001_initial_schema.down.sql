-- 001_initial_schema.down.sql
-- Rollback initial schema

DROP TRIGGER IF EXISTS update_papers_updated_at ON papers;
DROP TRIGGER IF EXISTS update_research_topics_updated_at ON research_topics;
DROP FUNCTION IF EXISTS update_updated_at_column();

DROP INDEX IF EXISTS idx_papers_abstract_fts;
DROP INDEX IF EXISTS idx_pipeline_stage;
DROP INDEX IF EXISTS idx_pipeline_topic_id;
DROP INDEX IF EXISTS idx_directions_topic_id;
DROP INDEX IF EXISTS idx_gaps_topic_id;
DROP INDEX IF EXISTS idx_papers_status;
DROP INDEX IF EXISTS idx_papers_relevance;
DROP INDEX IF EXISTS idx_papers_source_external;
DROP INDEX IF EXISTS idx_papers_topic_id;

DROP TABLE IF EXISTS pipeline_runs;
DROP TABLE IF EXISTS novel_directions;
DROP TABLE IF EXISTS research_gaps;
DROP TABLE IF EXISTS citations;
DROP TABLE IF EXISTS paper_authors;
DROP TABLE IF EXISTS papers;
DROP TABLE IF EXISTS authors;
DROP TABLE IF EXISTS research_topics;

DROP EXTENSION IF EXISTS "uuid-ossp";
