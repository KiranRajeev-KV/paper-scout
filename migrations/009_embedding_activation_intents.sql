-- +goose Up
CREATE TABLE embedding_activation_intents (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    generation_id UUID NOT NULL REFERENCES embedding_generations(id) ON DELETE CASCADE,
    target_collection TEXT NOT NULL,
    alias_name TEXT NOT NULL,
    previous_collection TEXT,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'switching', 'finalizing', 'completed', 'failed')),
    attempts INT NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    UNIQUE (generation_id)
);

CREATE INDEX idx_embedding_activation_intents_pending
    ON embedding_activation_intents(status, created_at)
    WHERE status IN ('pending', 'switching', 'finalizing', 'failed');

CREATE UNIQUE INDEX idx_embedding_activation_intents_one_pending_alias
    ON embedding_activation_intents(alias_name)
    WHERE status IN ('pending', 'switching', 'finalizing', 'failed');

CREATE TRIGGER update_embedding_activation_intents_updated_at BEFORE UPDATE ON embedding_activation_intents
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- +goose Down
DROP TRIGGER IF EXISTS update_embedding_activation_intents_updated_at ON embedding_activation_intents;
DROP INDEX IF EXISTS idx_embedding_activation_intents_one_pending_alias;
DROP TABLE IF EXISTS embedding_activation_intents;
