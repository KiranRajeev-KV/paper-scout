package agent

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/paper-scout/internal/llm"
	"github.com/paper-scout/internal/logger"
	"github.com/paper-scout/internal/storage/postgres"
)

type QueryExpander struct {
	llm        llm.Generator
	postgres   *postgres.Client
	structured *llm.StructuredOutput
}

func NewQueryExpander(llmClient llm.Generator, pg *postgres.Client) *QueryExpander {
	return &QueryExpander{
		llm:        llmClient,
		postgres:   pg,
		structured: llm.NewStructuredOutput(llmClient),
	}
}

type ExpandedQuery struct {
	Queries         []string `json:"expanded_queries"`
	RelatedConcepts []string `json:"related_concepts"`
	Subtopics       []string `json:"subtopics"`
	Keywords        []string `json:"search_keywords"`
}

func (e *QueryExpander) Expand(ctx context.Context, topicID string, topic string) (*ExpandedQuery, error) {
	logger.From(ctx).Info().
		Str("topic_id", topicID).
		Int("topic_chars", len(topic)).
		Msg("Expanding research topic")

	prompt := fmt.Sprintf(`You are a research assistant. Given a research topic, generate:
1. Multiple search queries that would help find relevant academic papers
2. Related concepts and keywords
3. Potential subtopics to explore

Research Topic: %s

Respond in JSON format:
{
  "expanded_queries": ["query1", "query2", ...],
  "related_concepts": ["concept1", "concept2", ...],
  "subtopics": ["subtopic1", "subtopic2", ...],
  "search_keywords": ["keyword1", "keyword2", ...]
}`, topic)

	schema := map[string]interface{}{
		"expanded_queries": []string{},
		"related_concepts": []string{},
		"subtopics":        []string{},
		"search_keywords":  []string{},
	}

	result, err := e.structured.Generate(ctx, prompt, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to expand query: %w", err)
	}

	var expanded ExpandedQuery
	if err := json.Unmarshal([]byte(result), &expanded); err != nil {
		return nil, fmt.Errorf("failed to parse expanded query: %w", err)
	}

	if len(expanded.Queries) == 0 {
		expanded.Queries = []string{topic}
	}

	expandedJSON, err := json.Marshal(expanded)
	if err != nil {
		return nil, fmt.Errorf("marshal expanded query: %w", err)
	}
	id, err := parseID("topic ID", topicID)
	if err != nil {
		return nil, err
	}
	_, err = e.postgres.Queries().UpdateResearchTopicExpandedQueries(ctx, postgres.UpdateResearchTopicExpandedQueriesParams{
		ID:              id,
		ExpandedQueries: expandedJSON,
	})
	if err != nil {
		return nil, fmt.Errorf("persist expanded queries: %w", err)
	}

	logger.From(ctx).Info().
		Int("queries", len(expanded.Queries)).
		Int("concepts", len(expanded.RelatedConcepts)).
		Int("keywords", len(expanded.Keywords)).
		Msg("Query expansion complete")

	return &expanded, nil
}

type QueryLevel int

const (
	QueryLevelFull QueryLevel = iota
	QueryLevelBroad
	QueryLevelMinimal
)

func (e *ExpandedQuery) GetQueriesForLevel(level QueryLevel, originalTopic string) []string {
	switch level {
	case QueryLevelFull:
		return e.Queries
	case QueryLevelBroad:
		if len(e.Queries) <= 1 {
			return []string{originalTopic}
		}
		return e.Queries[:max(2, len(e.Queries)/2)]
	case QueryLevelMinimal:
		return []string{originalTopic}
	default:
		return e.Queries
	}
}

func (e *ExpandedQuery) GetKeywordsForLevel(level QueryLevel) []string {
	switch level {
	case QueryLevelFull:
		return e.Keywords
	case QueryLevelBroad:
		if len(e.Keywords) <= 2 {
			return nil
		}
		return e.Keywords[:max(1, len(e.Keywords)/2)]
	case QueryLevelMinimal:
		return nil
	default:
		return e.Keywords
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
