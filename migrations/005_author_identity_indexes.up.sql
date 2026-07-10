-- Make author upserts deterministic for source IDs and ID-less arXiv names.

CREATE UNIQUE INDEX IF NOT EXISTS idx_authors_semantic_scholar_id
    ON authors (semantic_scholar_id)
    WHERE semantic_scholar_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_authors_fallback_name
    ON authors (lower(btrim(name)))
    WHERE semantic_scholar_id IS NULL;
