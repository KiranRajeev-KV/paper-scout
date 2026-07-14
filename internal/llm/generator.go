package llm

import "context"

// Generator produces free-form or schema-constrained text without owning embeddings.
type Generator interface {
	Generate(ctx context.Context, prompt string) (string, error)
	GenerateStructured(ctx context.Context, prompt string, schema any) (string, error)
	Health(ctx context.Context) error
	Provider() string
	Model() string
}
