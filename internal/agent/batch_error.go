package agent

import (
	"fmt"
	"strings"
)

// ItemFailure identifies one terminal item failure within a batch operation.
type ItemFailure struct {
	Kind       string
	Identifier string
	Err        error
}

// BatchError reports partial completion without discarding the underlying causes.
type BatchError struct {
	Operation string
	Total     int
	Succeeded int
	Failures  []ItemFailure
}

func (e *BatchError) Error() string {
	if e == nil {
		return ""
	}
	parts := make([]string, 0, len(e.Failures))
	for _, failure := range e.Failures {
		parts = append(parts, fmt.Sprintf("%s %s: %v", failure.Kind, failure.Identifier, failure.Err))
	}
	return fmt.Sprintf("%s completed with %d/%d successful: %s", e.Operation, e.Succeeded, e.Total, strings.Join(parts, "; "))
}

// Unwrap exposes every terminal cause to errors.Is and errors.As.
func (e *BatchError) Unwrap() []error {
	if e == nil {
		return nil
	}
	causes := make([]error, 0, len(e.Failures))
	for _, failure := range e.Failures {
		if failure.Err != nil {
			causes = append(causes, failure.Err)
		}
	}
	return causes
}

func newBatchError(operation string, total int, failures []ItemFailure) error {
	if len(failures) == 0 {
		return nil
	}
	return &BatchError{
		Operation: operation,
		Total:     total,
		Succeeded: total - len(failures),
		Failures:  append([]ItemFailure(nil), failures...),
	}
}
