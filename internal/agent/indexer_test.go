package agent

import (
	"context"
	"errors"
	"testing"

	"github.com/paper-scout/internal/worker"
)

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
	if batch.failures != 2 || batch.completed != 2 {
		t.Fatalf("batch = %+v, want two terminal failures", batch)
	}
}
