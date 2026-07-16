package postgres

import (
	"os"
	"strings"
	"testing"
)

// Protects rollback normalizes indexing rows before restoring the legacy constraint.
func TestEmbeddingGenerationRollbackNormalizesIndexingState(t *testing.T) {
	sql, err := os.ReadFile("../../../migrations/007_embedding_generations.sql")
	if err != nil {
		t.Fatalf("read embedding generation down migration: %v", err)
	}
	downAt := strings.Index(string(sql), "-- +goose Down")
	if downAt < 0 {
		t.Fatal("embedding generation migration is missing a Goose Down block")
	}
	downSQL := string(sql[downAt:])
	normalizeAt := strings.Index(downSQL, "UPDATE paper_chunks SET embedding_status = 'pending' WHERE embedding_status = 'indexing'")
	constraintAt := strings.Index(downSQL, "ADD CONSTRAINT paper_chunks_embedding_status_check")
	if normalizeAt < 0 || constraintAt < 0 || normalizeAt > constraintAt {
		t.Fatal("down migration must normalize indexing rows before restoring the legacy status constraint")
	}
}

// Protects the ownership cleanup removes legacy contracts from forward migrations.
func TestOwnershipCleanupRemovesLegacyContracts(t *testing.T) {
	up, err := os.ReadFile("../../../migrations/008_remove_legacy_ownership.sql")
	if err != nil {
		t.Fatalf("read ownership cleanup migration: %v", err)
	}
	downAt := strings.Index(string(up), "-- +goose Down")
	if downAt < 0 {
		t.Fatal("ownership cleanup migration is missing a Goose Down block")
	}
	forwardSQL := string(up[:downAt])
	for _, statement := range []string{
		"DROP TABLE IF EXISTS citations",
		"DROP TABLE IF EXISTS pipeline_runs",
		"DROP COLUMN IF EXISTS embedding_status",
	} {
		if !strings.Contains(forwardSQL, statement) {
			t.Fatalf("ownership cleanup migration is missing %q", statement)
		}
	}
}

// Protects Compose leaves PostgreSQL application schema creation to Goose.
func TestComposeDoesNotMountPostgresInitializationSQL(t *testing.T) {
	compose, err := os.ReadFile("../../../docker-compose.yml")
	if err != nil {
		t.Fatalf("read compose file: %v", err)
	}
	if strings.Contains(string(compose), "docker-entrypoint-initdb.d") || strings.Contains(string(compose), "postgres-init") {
		t.Fatal("Compose must not mount application SQL into PostgreSQL initialization")
	}
}

// Protects Goose migrations use one numbered file with both direction blocks.
func TestGooseMigrationFileLayout(t *testing.T) {
	entries, err := os.ReadDir("../../../migrations")
	if err != nil {
		t.Fatalf("read migrations directory: %v", err)
	}
	versions := make(map[string]string)
	for _, entry := range entries {
		name := entry.Name()
		if strings.HasSuffix(name, ".up.sql") || strings.HasSuffix(name, ".down.sql") {
			t.Fatalf("Goose does not support split migration file %q", name)
		}
		if entry.IsDir() || !strings.HasSuffix(name, ".sql") {
			continue
		}
		version, _, ok := strings.Cut(name, "_")
		if !ok {
			t.Fatalf("migration %q is missing a version separator", name)
		}
		if previous, exists := versions[version]; exists {
			t.Fatalf("migration version %s is duplicated by %q and %q", version, previous, name)
		}
		versions[version] = name
		sql, err := os.ReadFile("../../../migrations/" + name)
		if err != nil {
			t.Fatalf("read migration %q: %v", name, err)
		}
		if !strings.Contains(string(sql), "-- +goose Up") || !strings.Contains(string(sql), "-- +goose Down") {
			t.Fatalf("migration %q must contain Goose Up and Down blocks", name)
		}
	}
	if len(versions) == 0 {
		t.Fatal("no Goose migrations found")
	}
}

// Protects Goose parses the initial PL/pgSQL trigger function as one statement.
func TestInitialMigrationDelimitsTriggerFunction(t *testing.T) {
	sql, err := os.ReadFile("../../../migrations/001_initial_schema.sql")
	if err != nil {
		t.Fatalf("read initial migration: %v", err)
	}
	text := string(sql)
	beginAt := strings.Index(text, "-- +goose StatementBegin")
	functionAt := strings.Index(text, "CREATE OR REPLACE FUNCTION update_updated_at_column()")
	endAt := strings.Index(text, "-- +goose StatementEnd")
	if beginAt < 0 || functionAt < beginAt || endAt < functionAt {
		t.Fatal("initial PL/pgSQL trigger function must be inside a Goose statement block")
	}
}

// Protects recoverable generations use distinct collections and durable validation facts.
func TestRecoverableEmbeddingGenerationMigration(t *testing.T) {
	sql, err := os.ReadFile("../../../migrations/010_recoverable_embedding_generations.sql")
	if err != nil {
		t.Fatalf("read recoverable generation migration: %v", err)
	}
	upAt := strings.Index(string(sql), "-- +goose Up")
	downAt := strings.Index(string(sql), "-- +goose Down")
	if upAt < 0 || downAt < upAt {
		t.Fatal("recoverable generation migration is missing Goose direction blocks")
	}
	up := string(sql[upAt:downAt])
	for _, statement := range []string{
		"DROP CONSTRAINT IF EXISTS embedding_generations_provider_model_dimensions_instruction_version_indexing_version_key",
		"'ready'",
		"expected_point_count BIGINT",
		"chunk_snapshot_digest TEXT",
		"'alias_switched'",
	} {
		if !strings.Contains(up, statement) {
			t.Fatalf("recoverable generation migration is missing %q", statement)
		}
	}
}

// Protects recoverable generation rollback normalizes newer rows before legacy constraints return.
func TestRecoverableEmbeddingGenerationRollbackNormalizesNewerRows(t *testing.T) {
	sql, err := os.ReadFile("../../../migrations/010_recoverable_embedding_generations.sql")
	if err != nil {
		t.Fatalf("read recoverable generation migration: %v", err)
	}
	downAt := strings.Index(string(sql), "-- +goose Down")
	if downAt < 0 {
		t.Fatal("recoverable generation migration is missing a Goose Down block")
	}
	down := string(sql[downAt:])
	readyAt := strings.Index(down, "WHERE status = 'ready'")
	duplicatesAt := strings.Index(down, "row_number() OVER")
	constraintAt := strings.Index(down, "ADD CONSTRAINT embedding_generations_status_check")
	uniqueAt := strings.Index(down, "embedding_generations_provider_model_dimensions_instruction_version_indexing_version_key")
	if readyAt < 0 || duplicatesAt < readyAt || constraintAt < duplicatesAt || uniqueAt < constraintAt {
		t.Fatal("recoverable generation rollback must normalize ready and duplicate rows before restoring legacy constraints")
	}
}

// Protects stale activation intents become non-retryable after their target is discarded.
func TestSupersededActivationIntentMigration(t *testing.T) {
	sql, err := os.ReadFile("../../../migrations/011_superseded_embedding_activation_intents.sql")
	if err != nil {
		t.Fatalf("read superseded activation migration: %v", err)
	}
	upAt := strings.Index(string(sql), "-- +goose Up")
	downAt := strings.Index(string(sql), "-- +goose Down")
	if upAt < 0 || downAt < upAt {
		t.Fatal("superseded activation migration is missing Goose direction blocks")
	}
	up := string(sql[upAt:downAt])
	if !strings.Contains(up, "'superseded'") || !strings.Contains(up, "WHERE status IN ('pending', 'alias_switched', 'failed')") {
		t.Fatal("superseded activation migration must remove discarded intents from reconciliation")
	}
}
