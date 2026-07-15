package logger

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// Protects run logger writes separate structured file.
func TestRunLoggerWritesSeparateStructuredFile(t *testing.T) {
	manager, err := NewManager(Config{Directory: t.TempDir(), Level: "debug"})
	if err != nil {
		t.Fatalf("NewManager returned error: %v", err)
	}
	defer manager.Close()
	if err := manager.StartRun("run-1", "topic-1"); err != nil {
		t.Fatalf("StartRun returned error: %v", err)
	}
	ctx := manager.ContextForTopic(context.Background(), "topic-1")
	From(ctx).Info().Str("stage", "ranking").Msg("stage event")
	if err := manager.CloseRun("run-1"); err != nil {
		t.Fatalf("CloseRun returned error: %v", err)
	}
	data, err := os.ReadFile(manager.RunLogPath("run-1"))
	if err != nil {
		t.Fatalf("read run log: %v", err)
	}
	text := string(data)
	for _, required := range []string{`"run_id":"run-1"`, `"topic_id":"topic-1"`, `"stage":"ranking"`} {
		if !strings.Contains(text, required) {
			t.Fatalf("run log %q does not contain %q", text, required)
		}
	}
}

// Protects run logger handles concurrent events.
func TestRunLoggerHandlesConcurrentEvents(t *testing.T) {
	manager, err := NewManager(Config{Directory: t.TempDir(), Level: "info"})
	if err != nil {
		t.Fatalf("NewManager returned error: %v", err)
	}
	defer manager.Close()
	if err := manager.StartRun("run-concurrent", "topic-concurrent"); err != nil {
		t.Fatalf("StartRun returned error: %v", err)
	}
	ctx := manager.ContextForTopic(context.Background(), "topic-concurrent")
	var group sync.WaitGroup
	for i := 0; i < 50; i++ {
		group.Add(1)
		go func(value int) { defer group.Done(); From(ctx).Info().Int("value", value).Msg("event") }(i)
	}
	group.Wait()
	if err := manager.CloseRun("run-concurrent"); err != nil {
		t.Fatalf("CloseRun returned error: %v", err)
	}
	data, err := os.ReadFile(manager.RunLogPath("run-concurrent"))
	if err != nil {
		t.Fatalf("read run log: %v", err)
	}
	if lines := strings.Count(strings.TrimSpace(string(data)), "\n") + 1; lines != 50 {
		t.Fatalf("run log lines = %d, want 50", lines)
	}
}

// Protects log closure is idempotent.
func TestLogClosureIsIdempotent(t *testing.T) {
	manager, err := NewManager(Config{Directory: t.TempDir(), Level: "info"})
	if err != nil {
		t.Fatalf("NewManager returned error: %v", err)
	}
	if err := manager.StartRun("run-close", "topic-close"); err != nil {
		t.Fatalf("StartRun returned error: %v", err)
	}
	if err := manager.CloseRun("run-close"); err != nil {
		t.Fatalf("CloseRun returned error: %v", err)
	}
	if err := manager.CloseRun("run-close"); err != nil {
		t.Fatalf("second CloseRun returned error: %v", err)
	}
	if err := manager.Close(); err != nil {
		t.Fatalf("Close returned error: %v", err)
	}
	if err := manager.Close(); err != nil {
		t.Fatalf("second Close returned error: %v", err)
	}
}

// Protects startup from silently redirecting logs when the configured path is unusable.
func TestLogCreationFailureIsExplicit(t *testing.T) {
	root := t.TempDir()
	file := filepath.Join(root, "not-a-directory")
	if err := os.WriteFile(file, []byte("occupied"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewManager(Config{Directory: file, Level: "info"}); err == nil {
		t.Fatal("NewManager accepted an unusable log directory")
	}
}
