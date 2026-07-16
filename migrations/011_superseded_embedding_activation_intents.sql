-- +goose Up
ALTER TABLE embedding_activation_intents
    DROP CONSTRAINT IF EXISTS embedding_activation_intents_status_check;

ALTER TABLE embedding_activation_intents
    ADD CONSTRAINT embedding_activation_intents_status_check
        CHECK (status IN ('pending', 'alias_switched', 'completed', 'failed', 'superseded'));

DROP INDEX IF EXISTS idx_embedding_activation_intents_pending;
DROP INDEX IF EXISTS idx_embedding_activation_intents_one_pending_alias;

CREATE INDEX idx_embedding_activation_intents_pending
    ON embedding_activation_intents(status, created_at)
    WHERE status IN ('pending', 'alias_switched', 'failed');

CREATE UNIQUE INDEX idx_embedding_activation_intents_one_pending_alias
    ON embedding_activation_intents(alias_name)
    WHERE status IN ('pending', 'alias_switched', 'failed');

-- +goose Down
UPDATE embedding_activation_intents
SET status = 'failed'
WHERE status = 'superseded';

ALTER TABLE embedding_activation_intents
    DROP CONSTRAINT IF EXISTS embedding_activation_intents_status_check;

ALTER TABLE embedding_activation_intents
    ADD CONSTRAINT embedding_activation_intents_status_check
        CHECK (status IN ('pending', 'alias_switched', 'completed', 'failed'));
