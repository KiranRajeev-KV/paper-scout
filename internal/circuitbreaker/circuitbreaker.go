package circuitbreaker

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrCircuitOpen = errors.New("circuit breaker is open")
)

type State int

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

type Config struct {
	FailureThreshold int
	SuccessThreshold int
	OpenTimeout      time.Duration
	OnStateChange    func(name string, from, to State)
}

type CircuitBreaker struct {
	name            string
	config          Config
	state           State
	failures        int
	successes       int
	lastFailureTime time.Time
	mu              sync.RWMutex
}

func New(name string, config Config) *CircuitBreaker {
	if config.FailureThreshold <= 0 {
		config.FailureThreshold = 5
	}
	if config.SuccessThreshold <= 0 {
		config.SuccessThreshold = 2
	}
	if config.OpenTimeout <= 0 {
		config.OpenTimeout = 30 * time.Second
	}

	return &CircuitBreaker{
		name:   name,
		config: config,
		state:  StateClosed,
	}
}

func (cb *CircuitBreaker) Execute(ctx context.Context, fn func(ctx context.Context) error) error {
	if err := cb.beforeRequest(); err != nil {
		return err
	}

	err := fn(ctx)

	cb.afterRequest(err)

	return err
}

func (cb *CircuitBreaker) beforeRequest() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		return nil

	case StateOpen:
		if time.Since(cb.lastFailureTime) > cb.config.OpenTimeout {
			cb.setState(StateHalfOpen)
			return nil
		}
		return ErrCircuitOpen

	case StateHalfOpen:
		return nil
	}

	return nil
}

func (cb *CircuitBreaker) afterRequest(err error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err == nil {
		cb.onSuccess()
	} else {
		cb.onFailure()
	}
}

func (cb *CircuitBreaker) onSuccess() {
	cb.failures = 0

	switch cb.state {
	case StateClosed:
	case StateHalfOpen:
		cb.successes++
		if cb.successes >= cb.config.SuccessThreshold {
			cb.setState(StateClosed)
		}
	}
}

func (cb *CircuitBreaker) onFailure() {
	cb.successes = 0
	cb.failures++
	cb.lastFailureTime = time.Now()

	switch cb.state {
	case StateClosed:
		if cb.failures >= cb.config.FailureThreshold {
			cb.setState(StateOpen)
		}
	case StateHalfOpen:
		cb.setState(StateOpen)
	}
}

func (cb *CircuitBreaker) setState(newState State) {
	if cb.state == newState {
		return
	}

	oldState := cb.state
	cb.state = newState

	if newState == StateClosed {
		cb.failures = 0
		cb.successes = 0
	}

	if cb.config.OnStateChange != nil {
		go cb.config.OnStateChange(cb.name, oldState, newState)
	}
}

func (cb *CircuitBreaker) State() State {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

func (cb *CircuitBreaker) Failures() int {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.failures
}

func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = StateClosed
	cb.failures = 0
	cb.successes = 0
}
