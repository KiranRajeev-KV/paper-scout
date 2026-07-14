-- 002_topic_paper_membership.up.sql
-- +goose Up
-- Normalize global paper metadata and topic-specific membership state.

CREATE TABLE topic_papers (
    topic_id UUID NOT NULL REFERENCES research_topics(id) ON DELETE CASCADE,
    paper_id UUID NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
    discovery_source TEXT NOT NULL,
    relevance_score FLOAT,
    analysis JSONB,
    analysis_status TEXT NOT NULL DEFAULT 'pending' CHECK (analysis_status IN ('pending', 'processing', 'completed', 'failed')),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (topic_id, paper_id)
);

INSERT INTO topic_papers (
    topic_id, paper_id, discovery_source, relevance_score, analysis, analysis_status
)
SELECT topic_id, id, source, relevance_score, analysis,
       CASE WHEN analysis IS NULL THEN 'pending' ELSE 'completed' END
FROM papers;

CREATE INDEX idx_topic_papers_topic_id ON topic_papers(topic_id);
CREATE INDEX idx_topic_papers_relevance ON topic_papers(topic_id, relevance_score DESC);
CREATE INDEX idx_topic_papers_analysis_status ON topic_papers(topic_id, analysis_status);

CREATE TRIGGER update_topic_papers_updated_at BEFORE UPDATE ON topic_papers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

ALTER TABLE papers
    DROP COLUMN topic_id,
    DROP COLUMN analysis,
    DROP COLUMN relevance_score;

DROP INDEX IF EXISTS idx_papers_topic_id;
DROP INDEX IF EXISTS idx_papers_relevance;
DROP INDEX IF EXISTS idx_papers_status;

-- +goose Down
-- Restore the original single-topic paper ownership model.

ALTER TABLE papers
    ADD COLUMN topic_id UUID REFERENCES research_topics(id) ON DELETE CASCADE,
    ADD COLUMN analysis JSONB,
    ADD COLUMN relevance_score FLOAT;

UPDATE papers p
SET topic_id = tp.topic_id,
    analysis = tp.analysis,
    relevance_score = tp.relevance_score
FROM topic_papers tp
WHERE tp.paper_id = p.id
  AND tp.topic_id = (
      SELECT MIN(topic_id::text)::uuid
      FROM topic_papers first_topic
      WHERE first_topic.paper_id = p.id
  );

DELETE FROM papers WHERE topic_id IS NULL;

ALTER TABLE papers
    ALTER COLUMN topic_id SET NOT NULL;

CREATE INDEX idx_papers_topic_id ON papers(topic_id);
CREATE INDEX idx_papers_relevance ON papers(topic_id, relevance_score DESC);
CREATE INDEX idx_papers_status ON papers(topic_id, embedding_status);

DROP TRIGGER IF EXISTS update_topic_papers_updated_at ON topic_papers;
DROP INDEX IF EXISTS idx_topic_papers_analysis_status;
DROP INDEX IF EXISTS idx_topic_papers_relevance;
DROP INDEX IF EXISTS idx_topic_papers_topic_id;
DROP TABLE topic_papers;
