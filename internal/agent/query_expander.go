package agent

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/research-agent/internal/llm"
	"github.com/research-agent/internal/logger"
	"github.com/research-agent/internal/storage/postgres"
)

type QueryExpander struct {
	llm        *llm.Client
	postgres   *postgres.Client
	structured *llm.StructuredOutput
}

func NewQueryExpander(llmClient *llm.Client, pg *postgres.Client) *QueryExpander {
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
	logger.Info().
		Str("topic_id", topicID).
		Str("topic", topic).
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

	expandedJSON, _ := json.Marshal(expanded)
	_, err = e.postgres.Queries().UpdateResearchTopicExpandedQueries(ctx, postgres.UpdateResearchTopicExpandedQueriesParams{
		ID:              pgUUID(topicID),
		ExpandedQueries: expandedJSON,
	})
	if err != nil {
		logger.Warn().Err(err).Msg("Failed to update expanded queries in DB")
	}

	logger.Info().
		Int("queries", len(expanded.Queries)).
		Int("concepts", len(expanded.RelatedConcepts)).
		Int("keywords", len(expanded.Keywords)).
		Msg("Query expansion complete")

	return &expanded, nil
}
