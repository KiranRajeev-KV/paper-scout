package orchestrator

import (
	"sync"
	"testing"
)

func TestSSEBroadcastConcurrentUnsubscribe(t *testing.T) {
	manager := NewSSEManager()
	const subscribers = 8

	channels := make([]chan []byte, 0, subscribers)
	for i := 0; i < subscribers; i++ {
		channels = append(channels, manager.Subscribe("topic-1"))
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			manager.Broadcast(progressEvent{TopicID: "topic-1", Progress: float64(i)})
		}
	}()
	go func() {
		defer wg.Done()
		for _, ch := range channels {
			manager.Unsubscribe("topic-1", ch)
		}
	}()
	wg.Wait()

	if got := manager.GetSubscriberCount("topic-1"); got != 0 {
		t.Fatalf("subscriber count = %d, want 0", got)
	}
}
