package orchestrator

import (
	"encoding/json"
	"sync"

	"github.com/paper-scout/internal/logger"
)

type SSEManager struct {
	clients map[string]map[chan []byte]struct{}
	mu      sync.RWMutex
}

func NewSSEManager() *SSEManager {
	return &SSEManager{
		clients: make(map[string]map[chan []byte]struct{}),
	}
}

func (s *SSEManager) Subscribe(topicID string) chan []byte {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.clients[topicID] == nil {
		s.clients[topicID] = make(map[chan []byte]struct{})
	}

	ch := make(chan []byte, 100)
	s.clients[topicID][ch] = struct{}{}

	logger.Debug().Str("topic_id", topicID).Int("subscribers", len(s.clients[topicID])).Msg("SSE client subscribed")

	return ch
}

func (s *SSEManager) Unsubscribe(topicID string, ch chan []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if clients, ok := s.clients[topicID]; ok {
		delete(clients, ch)
		close(ch)

		if len(clients) == 0 {
			delete(s.clients, topicID)
		}
	}

	logger.Debug().Str("topic_id", topicID).Msg("SSE client unsubscribed")
}

func (s *SSEManager) Broadcast(event interface{}) {
	var topicID string
	var eventType string

	switch e := event.(type) {
	case statusEvent:
		topicID = e.TopicID
		eventType = "status"
	case progressEvent:
		topicID = e.TopicID
		eventType = "progress"
	default:
		return
	}

	data, err := json.Marshal(event)
	if err != nil {
		logger.Warn().Err(err).Msg("Failed to marshal SSE event")
		return
	}

	message := formatSSE(eventType, data)

	s.mu.RLock()
	clients := s.clients[topicID]
	s.mu.RUnlock()

	for ch := range clients {
		select {
		case ch <- []byte(message):
		default:
			logger.Warn().Str("topic_id", topicID).Msg("SSE channel full, dropping message")
		}
	}
}

func (s *SSEManager) BroadcastToAll(event interface{}) {
	var eventType string

	switch event.(type) {
	case statusEvent:
		eventType = "status"
	case progressEvent:
		eventType = "progress"
	default:
		return
	}

	data, err := json.Marshal(event)
	if err != nil {
		logger.Warn().Err(err).Msg("Failed to marshal SSE event")
		return
	}

	message := formatSSE(eventType, data)

	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, clients := range s.clients {
		for ch := range clients {
			select {
			case ch <- []byte(message):
			default:
			}
		}
	}
}

func formatSSE(eventType string, data []byte) string {
	return "event: " + eventType + "\ndata: " + string(data) + "\n\n"
}

func (s *SSEManager) GetSubscriberCount(topicID string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if clients, ok := s.clients[topicID]; ok {
		return len(clients)
	}
	return 0
}
