package worker

import (
	"time"

	"github.com/google/uuid"
)

type Type string

const (
	TypePaperAnalysis  Type = "paper_analysis"
	TypePDFDownload    Type = "pdf_download"
	TypeEmbedding      Type = "embedding"
	TypeEmbeddingBatch Type = "embedding_batch"
)

type Job struct {
	ID        string                 `json:"id"`
	Type      Type                   `json:"type"`
	Payload   map[string]interface{} `json:"payload"`
	Priority  int                    `json:"priority"`
	Timeout   time.Duration          `json:"timeout"`
	CreatedAt time.Time              `json:"created_at"`
	Retries   int                    `json:"retries"`
	MaxRetry  int                    `json:"max_retry"`
}

func NewJob(jobType Type, payload map[string]interface{}) Job {
	return Job{
		ID:        uuid.NewString(),
		Type:      jobType,
		Payload:   payload,
		Priority:  0,
		Timeout:   10 * time.Minute,
		CreatedAt: time.Now(),
		Retries:   0,
		MaxRetry:  3,
	}
}

func NewJobWithTimeout(jobType Type, payload map[string]interface{}, timeout time.Duration) Job {
	job := NewJob(jobType, payload)
	job.Timeout = timeout
	return job
}

func (j Job) WithPriority(priority int) Job {
	j.Priority = priority
	return j
}

func (j Job) WithMaxRetry(max int) Job {
	j.MaxRetry = max
	return j
}

func (j Job) CanRetry() bool {
	return j.Retries < j.MaxRetry
}

func (j Job) IncrementRetry() Job {
	j.Retries++
	return j
}

func (j Job) GetString(key string) string {
	if v, ok := j.Payload[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func (j Job) GetInt(key string) int {
	if v, ok := j.Payload[key]; ok {
		switch n := v.(type) {
		case int:
			return n
		case int64:
			return int(n)
		case float64:
			return int(n)
		}
	}
	return 0
}

func (j Job) GetStrings(key string) []string {
	if v, ok := j.Payload[key]; ok {
		if arr, ok := v.([]string); ok {
			return arr
		}
		if arr, ok := v.([]interface{}); ok {
			result := make([]string, 0, len(arr))
			for _, item := range arr {
				if s, ok := item.(string); ok {
					result = append(result, s)
				}
			}
			return result
		}
	}
	return nil
}

func (j Job) GetMaps(key string) []map[string]interface{} {
	v, ok := j.Payload[key]
	if !ok {
		return nil
	}
	switch values := v.(type) {
	case []map[string]interface{}:
		return values
	case []interface{}:
		maps := make([]map[string]interface{}, 0, len(values))
		for _, value := range values {
			if item, ok := value.(map[string]interface{}); ok {
				maps = append(maps, item)
			}
		}
		return maps
	default:
		return nil
	}
}
