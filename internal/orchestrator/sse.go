package orchestrator

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/paper-scout/internal/logger"
	"github.com/rs/zerolog"
)

// SSEManager coordinates non-blocking event delivery to topic subscribers.
type SSEManager struct {
	clients map[string]map[chan []byte]struct{}
	closed  bool
	mu      sync.RWMutex
	log     zerolog.Logger
}

// NewSSEManager constructs an SSE manager that writes lifecycle events through ctx's logger.
func NewSSEManager(ctx context.Context) *SSEManager {
	return &SSEManager{
		clients: make(map[string]map[chan []byte]struct{}),
		log:     *logger.From(ctx),
	}
}

func (s *SSEManager) Subscribe(topicID string) chan []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch := make(chan []byte, 100)
	if s.closed {
		close(ch)
		return ch
	}

	if s.clients[topicID] == nil {
		s.clients[topicID] = make(map[chan []byte]struct{})
	}

	s.clients[topicID][ch] = struct{}{}

	s.log.Debug().Str("topic_id", topicID).Int("subscribers", len(s.clients[topicID])).Msg("SSE client subscribed")

	return ch
}

// CloseAll releases every active stream during HTTP server shutdown. New
// subscribers receive a closed channel so a handler racing shutdown exits.
func (s *SSEManager) CloseAll() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return
	}
	for _, clients := range s.clients {
		for ch := range clients {
			close(ch)
		}
	}
	s.clients = make(map[string]map[chan []byte]struct{})
	s.closed = true
}

func (s *SSEManager) Unsubscribe(topicID string, ch chan []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if clients, ok := s.clients[topicID]; ok {
		if _, subscribed := clients[ch]; subscribed {
			delete(clients, ch)
			close(ch)
		}

		if len(clients) == 0 {
			delete(s.clients, topicID)
		}
	}

	s.log.Debug().Str("topic_id", topicID).Msg("SSE client unsubscribed")
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

	if err := s.Send(topicID, eventType, event); err != nil {
		s.log.Warn().Err(err).Msg("Failed to marshal SSE event")
	}
}

// Send publishes a typed event without blocking on slow subscribers.
func (s *SSEManager) Send(topicID, eventType string, event interface{}) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	message := formatSSE(eventType, data)

	s.mu.RLock()
	defer s.mu.RUnlock()

	for ch := range s.clients[topicID] {
		select {
		case ch <- []byte(message):
		default:
			s.log.Warn().Str("topic_id", topicID).Msg("SSE channel full, dropping message")
		}
	}
	return nil
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
		s.log.Warn().Err(err).Msg("Failed to marshal SSE event")
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
