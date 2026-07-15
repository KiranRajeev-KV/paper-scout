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
