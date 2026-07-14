package llm

import (
	"testing"

	"google.golang.org/genai"
)

// Protects infer schema preserves field name for option only json tag.
func TestInferSchemaPreservesFieldNameForOptionOnlyJSONTag(t *testing.T) {
	type response struct {
		Result string `json:",omitempty"`
	}

	schema, err := inferSchema(response{})
	if err != nil {
		t.Fatalf("inferSchema returned error: %v", err)
	}
	if _, ok := schema.Properties["Result"]; !ok {
		t.Fatalf("schema properties = %#v, want Result field", schema.Properties)
	}
	if _, ok := schema.Properties[""]; ok {
		t.Fatal("schema contains an empty property name")
	}
}

// Protects infer schema rerank scores.
func TestInferSchemaRerankScores(t *testing.T) {
	schema, err := inferSchema(map[string]interface{}{
		"scores": []map[string]interface{}{
			{"index": 0, "score": 0.0, "reason": ""},
		},
	})
	if err != nil {
		t.Fatalf("inferSchema returned error: %v", err)
	}

	scores, ok := schema.Properties["scores"]
	if !ok {
		t.Fatal("schema is missing scores property")
	}
	if scores.Type != genai.TypeArray {
		t.Fatalf("scores type = %q, want array", scores.Type)
	}
	if scores.Items == nil || scores.Items.Type != genai.TypeObject {
		t.Fatalf("scores item schema = %#v, want object", scores.Items)
	}
	for _, field := range []string{"index", "score", "reason"} {
		if _, ok := scores.Items.Properties[field]; !ok {
			t.Errorf("score item is missing %q property", field)
		}
	}
}
