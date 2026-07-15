package agent

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/paper-scout/internal/ollama"
)

// Protects Qwen feasibility output remains structured across representative gaps.
func TestOllamaFeasibilityStructuredOutput(t *testing.T) {
	if os.Getenv("PAPER_SCOUT_RUN_OLLAMA_INTEGRATION") != "1" {
		t.Skip("set PAPER_SCOUT_RUN_OLLAMA_INTEGRATION=1 to run against local Ollama")
	}
	model := os.Getenv("PAPER_SCOUT_OLLAMA_MODEL")
	if model == "" {
		model = "qwen3.5:4b-q4_K_M"
	}
	generator, err := ollama.NewGenerator(ollama.GenerationConfig{
		BaseURL: "http://localhost:11434", Model: model, Timeout: 2 * time.Minute,
		KeepAlive: "5m", Concurrency: 1, Think: false, MaxOutputTokens: 1024, Temperature: 0,
	})
	if err != nil {
		t.Fatalf("NewGenerator: %v", err)
	}
	evaluator := NewFeasibilityEvaluator(generator, nil)
	for _, gap := range []ResearchGap{
		{Title: "Domain Generalization", Description: "Most benchmarks use English news; results may not apply to other domains.", Evidence: "Representative papers use narrow evaluation domains."},
		{Title: "Real-World Generalizability", Description: "Offline benchmarks may not predict performance in deployed workflows.", Evidence: "Reported evaluation omits operational feedback loops."},
		{Title: "Multimodal Integration", Description: "Text-only methods omit relevant visual and tabular evidence.", Evidence: "Current baselines exclude multimodal inputs."},
	} {
		result, err := evaluator.evaluateGap(context.Background(), gap)
		if err != nil {
			t.Fatalf("evaluate %q: %v", gap.Title, err)
		}
		if result.Difficulty == "" || result.Recommendation == "" || len(result.RequiredExpertise) == 0 || len(result.PotentialChallenges) == 0 {
			t.Fatalf("evaluate %q returned incomplete result: %#v", gap.Title, result)
		}
	}
}
