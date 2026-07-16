package orchestrator

import (
	"context"
	"sync"
	"testing"
)

// Protects concurrent pipeline status reads.
func TestConcurrentPipelineStatusReads(t *testing.T) {
	state := &PipelineStateService{pipelines: make(map[string]*Pipeline)}
	state.Remember(&Pipeline{TopicID: "topic-1", Status: "processing", Stage: StagePending})
	var wg sync.WaitGroup
	const readers = 8
	stages := []Stage{StageQueryExpand, StageDiscovery, StageRanking, StageAnalysis, StageGapDetection, StageFeasibility, StageReport, StageCompleted}
	wg.Add(readers + 1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			state.Remember(&Pipeline{TopicID: "topic-1", Status: "processing", Progress: float64(i) / 1000, Stage: stages[i%len(stages)]})
		}
	}()
	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				pipeline, err := state.Get(context.Background(), "topic-1")
				if err != nil {
					t.Errorf("Get returned error: %v", err)
					return
				}
				if pipeline.TopicID != "topic-1" || pipeline.Progress < 0 || pipeline.Progress >= 1 {
					t.Errorf("invalid pipeline snapshot: %+v", pipeline)
					return
				}
			}
		}()
	}
	wg.Wait()
}
