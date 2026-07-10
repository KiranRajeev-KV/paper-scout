-- 001_initial_schema.up.sql
-- Research AI Agent Database Schema

-- Enable UUID extension
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Research topics table
CREATE TABLE research_topics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    run_id UUID NOT NULL DEFAULT uuid_generate_v4() UNIQUE,
    topic TEXT NOT NULL,
    expanded_queries JSONB,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'processing', 'completed', 'failed')),
    current_stage TEXT NOT NULL DEFAULT 'pending',
    progress DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (progress >= 0 AND progress <= 1),
    error_message TEXT,
    config JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

-- Authors table
CREATE TABLE authors (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL,
    semantic_scholar_id TEXT,
    orcid TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Papers table
CREATE TABLE papers (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    -- Source metadata
    source TEXT NOT NULL CHECK (source IN ('semantic_scholar', 'arxiv')),
    external_id TEXT NOT NULL,
    source_url TEXT,
    
    -- Bibliographic data
    title TEXT NOT NULL,
    abstract TEXT,
    publication_date DATE,
    venue TEXT,
    
    embedding_status TEXT DEFAULT 'pending' CHECK (embedding_status IN ('pending', 'completed', 'failed')),
    
    -- PDF handling
    pdf_url TEXT,
    pdf_downloaded BOOLEAN DEFAULT FALSE,
    pdf_parsed BOOLEAN DEFAULT FALSE,
    
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    UNIQUE(source, external_id)
);

-- Topic membership and topic-specific analysis/ranking state
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

-- Paper-Authors junction table
CREATE TABLE paper_authors (
    paper_id UUID NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
    author_id UUID NOT NULL REFERENCES authors(id) ON DELETE CASCADE,
    position INT NOT NULL,
    PRIMARY KEY (paper_id, author_id)
);

-- Citations table
CREATE TABLE citations (
    citing_paper_id UUID NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
    cited_paper_id UUID NOT NULL REFERENCES papers(id) ON DELETE CASCADE,
    PRIMARY KEY (citing_paper_id, cited_paper_id)
);

-- Research gaps table
CREATE TABLE research_gaps (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    topic_id UUID NOT NULL REFERENCES research_topics(id) ON DELETE CASCADE,
    
    gap_type TEXT NOT NULL CHECK (gap_type IN ('unexplored', 'conflicting', 'limitation')),
    title TEXT NOT NULL,
    description TEXT,
    
    -- Related papers
    related_paper_ids UUID[],
    
    -- LLM analysis
    evidence TEXT,
    feasibility JSONB,
    
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Novel research directions table
CREATE TABLE novel_directions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    topic_id UUID NOT NULL REFERENCES research_topics(id) ON DELETE CASCADE,
    gap_id UUID REFERENCES research_gaps(id) ON DELETE SET NULL,
    
    title TEXT NOT NULL,
    description TEXT,
    rationale TEXT,
    
    -- Feasibility scoring
    feasibility_score FLOAT,
    implementation_complexity TEXT CHECK (implementation_complexity IN ('low', 'medium', 'high')),
    estimated_cost TEXT,
    industry_viability TEXT,
    time_to_mvp TEXT,
    
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Pipeline runs table (for tracking execution)
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

-- Indexes for common queries
CREATE INDEX idx_papers_source_external ON papers(source, external_id);
CREATE INDEX idx_topic_papers_topic_id ON topic_papers(topic_id);
CREATE INDEX idx_topic_papers_relevance ON topic_papers(topic_id, relevance_score DESC);
CREATE INDEX idx_topic_papers_analysis_status ON topic_papers(topic_id, analysis_status);
CREATE INDEX idx_gaps_topic_id ON research_gaps(topic_id);
CREATE INDEX idx_directions_topic_id ON novel_directions(topic_id);
CREATE INDEX idx_pipeline_topic_id ON pipeline_runs(topic_id);
CREATE INDEX idx_pipeline_stage ON pipeline_runs(topic_id, stage);

CREATE UNIQUE INDEX idx_research_gaps_topic_title ON research_gaps(topic_id, title);
CREATE UNIQUE INDEX idx_novel_directions_topic_title ON novel_directions(topic_id, title);

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

-- Full-text search on paper abstracts
CREATE INDEX idx_papers_abstract_fts ON papers USING gin(to_tsvector('english', abstract));

-- Updated_at trigger function
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply updated_at trigger to relevant tables
CREATE TRIGGER update_research_topics_updated_at BEFORE UPDATE ON research_topics
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_papers_updated_at BEFORE UPDATE ON papers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_pipeline_stage_checkpoints_updated_at BEFORE UPDATE ON pipeline_stage_checkpoints
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_topic_papers_updated_at BEFORE UPDATE ON topic_papers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
