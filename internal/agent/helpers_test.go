package agent

import "testing"

// Protects truncate text preserves utf8.
func TestTruncateTextPreservesUTF8(t *testing.T) {
	got := truncateText("研究abc", 3)
	if got != "研究a" {
		t.Fatalf("truncateText = %q, want %q", got, "研究a")
	}
}
