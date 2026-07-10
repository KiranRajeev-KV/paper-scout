-- 002_topic_paper_membership.down.sql
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
