// Package accelerator coordinates bounded access to a shared local accelerator.
package accelerator

import "context"

// Gate bounds concurrent operations that compete for the same GPU memory.
type Gate struct {
	tokens chan struct{}
}

// NewGate creates a gate with at least one permit.
func NewGate(limit int) *Gate {
	if limit < 1 {
		limit = 1
	}
	return &Gate{tokens: make(chan struct{}, limit)}
}

// Acquire waits for a permit or context cancellation.
func (g *Gate) Acquire(ctx context.Context) error {
	if g == nil {
		return nil
	}
	select {
	case g.tokens <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release returns one acquired permit.
func (g *Gate) Release() {
	if g != nil {
		<-g.tokens
	}
}
