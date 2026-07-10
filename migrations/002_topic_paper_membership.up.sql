-- 002_topic_paper_membership.up.sql
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
