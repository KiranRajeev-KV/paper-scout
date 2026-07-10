-- Persist the current workflow snapshot in PostgreSQL, independent of Redis.

ALTER TABLE research_topics
    ADD COLUMN current_stage TEXT NOT NULL DEFAULT 'pending',
    ADD COLUMN progress DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (progress >= 0 AND progress <= 1),
    ADD COLUMN error_message TEXT;

