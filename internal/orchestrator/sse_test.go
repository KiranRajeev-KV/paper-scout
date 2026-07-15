package orchestrator

import (
	"sync"
	"testing"
	"time"
)

// Protects sse broadcast concurrent unsubscribe.
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

// Protects sse slow subscriber does not block broadcast.
func TestSSESlowSubscriberDoesNotBlockBroadcast(t *testing.T) {
	manager := NewSSEManager()
	channel := manager.Subscribe("topic-1")
	defer manager.Unsubscribe("topic-1", channel)
	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			manager.Broadcast(progressEvent{TopicID: "topic-1", Progress: float64(i)})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("broadcast blocked on a slow subscriber")
	}
}

// Protects shutdown from leaving an SSE handler blocked on its subscription.
func TestSSECloseAllClosesExistingAndFutureSubscriptions(t *testing.T) {
	manager := NewSSEManager()
	active := manager.Subscribe("topic-1")

	manager.CloseAll()
	if _, ok := <-active; ok {
		t.Fatal("active subscription remained open after shutdown")
	}

	future := manager.Subscribe("topic-1")
	if _, ok := <-future; ok {
		t.Fatal("subscription opened after shutdown")
	}
}
