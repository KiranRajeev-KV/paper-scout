-- +goose Up
ALTER TABLE embedding_generations
    DROP CONSTRAINT IF EXISTS embedding_generations_provider_model_dimensions_instruction_version_indexing_version_key,
    DROP CONSTRAINT IF EXISTS embedding_generations_status_check;

ALTER TABLE embedding_generations
    ADD CONSTRAINT embedding_generations_status_check
        CHECK (status IN ('building', 'ready', 'active', 'retired', 'failed'));

ALTER TABLE embedding_activation_intents
    ADD COLUMN expected_point_count BIGINT CHECK (expected_point_count >= 0),
    ADD COLUMN chunk_snapshot_digest TEXT;

ALTER TABLE embedding_activation_intents
    DROP CONSTRAINT IF EXISTS embedding_activation_intents_status_check;

UPDATE embedding_activation_intents
SET status = CASE status
    WHEN 'switching' THEN 'pending'
    WHEN 'finalizing' THEN 'alias_switched'
    ELSE status
END;

ALTER TABLE embedding_activation_intents
    ADD CONSTRAINT embedding_activation_intents_status_check
        CHECK (status IN ('pending', 'alias_switched', 'completed', 'failed'));

DROP INDEX IF EXISTS idx_embedding_activation_intents_pending;
DROP INDEX IF EXISTS idx_embedding_activation_intents_one_pending_alias;

CREATE INDEX idx_embedding_activation_intents_pending
    ON embedding_activation_intents(status, created_at)
    WHERE status IN ('pending', 'alias_switched', 'failed');

CREATE UNIQUE INDEX idx_embedding_activation_intents_one_pending_alias
    ON embedding_activation_intents(alias_name)
    WHERE status IN ('pending', 'alias_switched', 'failed');

-- +goose Down
ALTER TABLE embedding_activation_intents
    DROP CONSTRAINT IF EXISTS embedding_activation_intents_status_check;

DROP INDEX IF EXISTS idx_embedding_activation_intents_pending;
DROP INDEX IF EXISTS idx_embedding_activation_intents_one_pending_alias;

UPDATE embedding_activation_intents
SET status = CASE status
    WHEN 'alias_switched' THEN 'finalizing'
    ELSE status
END;

ALTER TABLE embedding_activation_intents
    ADD CONSTRAINT embedding_activation_intents_status_check
        CHECK (status IN ('pending', 'switching', 'finalizing', 'completed', 'failed'));

CREATE INDEX idx_embedding_activation_intents_pending
    ON embedding_activation_intents(status, created_at)
    WHERE status IN ('pending', 'switching', 'finalizing', 'failed');

CREATE UNIQUE INDEX idx_embedding_activation_intents_one_pending_alias
    ON embedding_activation_intents(alias_name)
    WHERE status IN ('pending', 'switching', 'finalizing', 'failed');

ALTER TABLE embedding_activation_intents
    DROP COLUMN IF EXISTS chunk_snapshot_digest,
    DROP COLUMN IF EXISTS expected_point_count;

ALTER TABLE embedding_generations
    DROP CONSTRAINT IF EXISTS embedding_generations_status_check;

UPDATE embedding_generations
SET status = 'failed',
    error_message = COALESCE(error_message, 'generation was ready when recoverable activation support was removed'),
    updated_at = NOW()
WHERE status = 'ready';

DELETE FROM embedding_generations
WHERE id IN (
    SELECT id
    FROM (
        SELECT id,
               row_number() OVER (
                   PARTITION BY provider, model, dimensions, instruction_version, indexing_version
                   ORDER BY (status = 'active') DESC,
                            activated_at DESC NULLS LAST,
                            updated_at DESC,
                            created_at DESC,
                            id DESC
               ) AS duplicate_rank
        FROM embedding_generations
    ) duplicates
    WHERE duplicate_rank > 1
);

ALTER TABLE embedding_generations
    ADD CONSTRAINT embedding_generations_status_check
        CHECK (status IN ('building', 'active', 'retired', 'failed')),
    ADD CONSTRAINT embedding_generations_provider_model_dimensions_instruction_version_indexing_version_key
        UNIQUE (provider, model, dimensions, instruction_version, indexing_version);
