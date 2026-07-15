// Package logger owns application and per-run structured log lifecycles.
package logger

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"
)

type Config struct {
	Directory   string
	Level       string
	Development bool
}

type Manager struct {
	app       zerolog.Logger
	appFile   *os.File
	appWriter io.Writer
	directory string
	level     zerolog.Level
	mu        sync.Mutex
	runs      map[string]*runLog
	topics    map[string]string
	closed    bool
}

type runLog struct {
	file   *os.File
	logger zerolog.Logger
	topic  string
}

type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (w *lockedWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.w.Write(data)
}

var installed atomic.Pointer[Manager]
var fallback = zerolog.New(os.Stderr).With().Timestamp().Logger()

func NewManager(cfg Config) (*Manager, error) {
	if cfg.Directory == "" {
		return nil, fmt.Errorf("log directory is required")
	}
	level, err := zerolog.ParseLevel(cfg.Level)
	if err != nil {
		return nil, fmt.Errorf("invalid log level %q: %w", cfg.Level, err)
	}
	appDir := filepath.Join(cfg.Directory, "app")
	runDir := filepath.Join(cfg.Directory, "runs")
	if err := os.MkdirAll(appDir, 0o750); err != nil {
		return nil, fmt.Errorf("create application log directory: %w", err)
	}
	if err := os.MkdirAll(runDir, 0o750); err != nil {
		return nil, fmt.Errorf("create run log directory: %w", err)
	}
	name := fmt.Sprintf("%s-%d.jsonl", time.Now().UTC().Format("20060102T150405.000000000Z"), os.Getpid())
	file, err := os.OpenFile(filepath.Join(appDir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		return nil, fmt.Errorf("create application log file: %w", err)
	}
	fileWriter := &lockedWriter{w: file}
	var output io.Writer = fileWriter
	if cfg.Development {
		console := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.Kitchen}
		output = zerolog.MultiLevelWriter(fileWriter, console)
	}
	app := zerolog.New(output).Level(level).With().Timestamp().Logger()
	return &Manager{app: app, appFile: file, appWriter: fileWriter, directory: cfg.Directory, level: level, runs: make(map[string]*runLog), topics: make(map[string]string)}, nil
}

// Install makes one fully initialized manager available to legacy process-level call sites.
func Install(manager *Manager) error {
	if manager == nil {
		return fmt.Errorf("logger manager is nil")
	}
	if !installed.CompareAndSwap(nil, manager) {
		return fmt.Errorf("logger manager is already installed")
	}
	return nil
}

func (m *Manager) App() *zerolog.Logger { return &m.app }

func (m *Manager) StartRun(runID, topicID string) error {
	if runID == "" || topicID == "" {
		return fmt.Errorf("run ID and topic ID are required for run logging")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return fmt.Errorf("log manager is closed")
	}
	if existing := m.runs[runID]; existing != nil {
		m.topics[topicID] = runID
		return nil
	}
	path := filepath.Join(m.directory, "runs", runID+".jsonl")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o640)
	if err != nil {
		return fmt.Errorf("open run log %s: %w", runID, err)
	}
	writer := &lockedWriter{w: file}
	output := zerolog.MultiLevelWriter(m.appWriter, writer)
	runLogger := zerolog.New(output).Level(m.level).With().Timestamp().Str("run_id", runID).Str("topic_id", topicID).Logger()
	m.runs[runID] = &runLog{file: file, logger: runLogger, topic: topicID}
	m.topics[topicID] = runID
	return nil
}

func (m *Manager) ContextForTopic(ctx context.Context, topicID string) context.Context {
	m.mu.Lock()
	defer m.mu.Unlock()
	runID := m.topics[topicID]
	if run := m.runs[runID]; run != nil {
		return run.logger.WithContext(ctx)
	}
	return m.app.WithContext(ctx)
}

// RunIDForTopic returns the active run associated with a topic.
func (m *Manager) RunIDForTopic(topicID string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	runID, ok := m.topics[topicID]
	return runID, ok
}

func (m *Manager) CloseRun(runID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	run := m.runs[runID]
	if run == nil {
		return nil
	}
	delete(m.runs, runID)
	delete(m.topics, run.topic)
	return run.file.Close()
}

func (m *Manager) RunLogPath(runID string) string {
	return filepath.Join(m.directory, "runs", runID+".jsonl")
}

func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	var first error
	for runID, run := range m.runs {
		if err := run.file.Close(); err != nil && first == nil {
			first = fmt.Errorf("close run log %s: %w", runID, err)
		}
	}
	m.runs = make(map[string]*runLog)
	m.topics = make(map[string]string)
	if err := m.appFile.Close(); err != nil && first == nil {
		first = fmt.Errorf("close application log: %w", err)
	}
	return first
}

func From(ctx context.Context) *zerolog.Logger {
	if ctx != nil {
		if value := zerolog.Ctx(ctx); value != nil && value.GetLevel() != zerolog.Disabled {
			return value
		}
	}
	return Get()
}

func Get() *zerolog.Logger {
	if manager := installed.Load(); manager != nil {
		return manager.App()
	}
	return &fallback
}
func Debug() *zerolog.Event { return Get().Debug() }
func Info() *zerolog.Event  { return Get().Info() }
func Warn() *zerolog.Event  { return Get().Warn() }
func Error() *zerolog.Event { return Get().Error() }
