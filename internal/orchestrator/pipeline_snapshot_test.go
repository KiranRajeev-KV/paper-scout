package orchestrator

import (
	"sync"
	"testing"
)

func TestConcurrentPipelineStatusReads(t *testing.T) {
	o := &Orchestrator{pipelines: make(map[string]*Pipeline)}
	working := &Pipeline{TopicID: "topic-1", Status: "processing", Stage: StagePending}
	o.publishPipeline(working)

	var wg sync.WaitGroup
	const readers = 8
	stages := []Stage{StageQueryExpand, StageDiscovery, StageRanking, StageAnalysis, StageGapDetection, StageFeasibility, StageReport, StageCompleted}
	wg.Add(readers + 1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			working.Progress = float64(i) / 1000
			working.Stage = stages[i%len(stages)]
			o.publishPipeline(working)
		}
	}()

	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				pipeline, err := o.GetPipeline("topic-1")
				if err != nil {
					t.Errorf("GetPipeline returned error: %v", err)
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
