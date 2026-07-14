package agent

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/paper-scout/internal/worker"
)

// Protects indexer terminal failures complete batch.
func TestIndexerTerminalFailuresCompleteBatch(t *testing.T) {
	indexer := NewIndexer(nil, nil)
	batchID := "batch"
	first := worker.NewJob(worker.TypePDFDownload, map[string]interface{}{"paper_id": "paper-1"})
	second := worker.NewJob(worker.TypeEmbedding, map[string]interface{}{"paper_id": "paper-1"})
	batch := &indexBatch{topicID: "topic", total: 2, done: make(chan struct{})}

	indexer.batches[batchID] = batch
	indexer.jobToBatch[first.ID] = batchID
	indexer.jobToBatch[second.ID] = batchID

	indexer.HandleJobCompletion(first, errors.New("PDF unavailable"), true)
	indexer.HandleJobCompletion(second, errors.New("embedding unavailable"), true)

	select {
	case <-batch.done:
	case <-context.Background().Done():
		t.Fatal("unreachable")
	default:
		t.Fatal("terminal failures did not finish indexing batch")
	}
	if len(batch.failures) != 2 || batch.completed != 2 {
		t.Fatalf("batch = %+v, want two terminal failures", batch)
	}

	err := indexer.wait(context.Background(), batchID)
	var batchErr *BatchError
	if !errors.As(err, &batchErr) {
		t.Fatalf("wait error = %v, want *BatchError", err)
	}
	if batchErr.Total != 2 || batchErr.Succeeded != 0 || len(batchErr.Failures) != 2 {
		t.Fatalf("batch error = %#v, want two terminal failures", batchErr)
	}
	if !strings.Contains(batchErr.Error(), "embedding unavailable") {
		t.Fatalf("batch error = %q, want underlying cause", batchErr.Error())
	}
}
