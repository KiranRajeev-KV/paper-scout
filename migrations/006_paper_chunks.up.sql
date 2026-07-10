CREATE TABLE paper_chunks (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    topic_id UUID NOT NULL REFERENCES research_topics(id) ON DELETE CASCADE,
    paper_id UUID NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
    chunk_type TEXT NOT NULL,
    chunk_index INT NOT NULL CHECK (chunk_index >= 0),
    text TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'pdf',
    embedding_status TEXT NOT NULL DEFAULT 'pending'
        CHECK (embedding_status IN ('pending', 'completed', 'failed')),
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (topic_id, paper_id, chunk_type, chunk_index)
);

CREATE INDEX idx_paper_chunks_topic_paper ON paper_chunks(topic_id, paper_id);
CREATE INDEX idx_paper_chunks_topic_embedding_status ON paper_chunks(topic_id, embedding_status);
CREATE INDEX idx_paper_chunks_paper_chunk_index ON paper_chunks(paper_id, chunk_index);

CREATE TRIGGER update_paper_chunks_updated_at BEFORE UPDATE ON paper_chunks
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
