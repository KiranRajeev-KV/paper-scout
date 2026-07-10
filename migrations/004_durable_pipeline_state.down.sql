ALTER TABLE research_topics
    DROP COLUMN IF EXISTS error_message,
    DROP COLUMN IF EXISTS progress,
    DROP COLUMN IF EXISTS current_stage;

