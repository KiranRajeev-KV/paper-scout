package agent

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/paper-scout/internal/llm"
	"github.com/paper-scout/internal/logger"
	"github.com/paper-scout/internal/storage/postgres"
)

type FeasibilityEvaluator struct {
	llm        *llm.Client
	postgres   *postgres.Client
	structured *llm.StructuredOutput
}

func NewFeasibilityEvaluator(llmClient *llm.Client, pg *postgres.Client) *FeasibilityEvaluator {
	return &FeasibilityEvaluator{
		llm:        llmClient,
		postgres:   pg,
		structured: llm.NewStructuredOutput(llmClient),
	}
}

type FeasibilityResult struct {
	Difficulty          string   `json:"difficulty"`
	EstimatedCost       string   `json:"estimated_cost"`
	IndustryViability   string   `json:"industry_viability"`
	TimeToMVP           string   `json:"time_to_mvp"`
	RequiredExpertise   []string `json:"required_expertise"`
	PotentialChallenges []string `json:"potential_challenges"`
	Recommendation      string   `json:"recommendation"`
}

func (f *FeasibilityEvaluator) Evaluate(ctx context.Context, topicID string, gaps []ResearchGap) ([]FeasibilityResult, error) {
	logger.Info().
		Str("topic_id", topicID).
		Int("gaps", len(gaps)).
		Msg("Starting feasibility evaluation")

	results := make([]FeasibilityResult, 0, len(gaps))

	for _, gap := range gaps {
		result, err := f.evaluateGap(ctx, gap)
		if err != nil {
			logger.Warn().Err(err).Str("gap", gap.Title).Msg("Failed to evaluate gap feasibility")
			continue
		}

		results = append(results, *result)

		if err := f.storeDirection(ctx, topicID, gap, result); err != nil {
			logger.Warn().Err(err).Str("gap", gap.Title).Msg("Failed to store direction")
		}
	}

	logger.Info().
		Int("evaluated", len(results)).
		Msg("Feasibility evaluation complete")

	return results, nil
}

func (f *FeasibilityEvaluator) evaluateGap(ctx context.Context, gap ResearchGap) (*FeasibilityResult, error) {
	prompt := fmt.Sprintf(`Evaluate the feasibility of the following research direction.

Research Gap: %s
Description: %s
Evidence: %s

Provide feasibility analysis in JSON format:
{
  "difficulty": "low|medium|high",
  "estimated_cost": "Description of resource requirements",
  "industry_viability": "Potential industry applications",
  "time_to_mvp": "Estimated time to minimum viable research",
  "required_expertise": ["skill1", "skill2"],
  "potential_challenges": ["challenge1", "challenge2"],
  "recommendation": "Brief recommendation"
}`, gap.Title, gap.Description, gap.Evidence)

	schema := map[string]interface{}{
		"difficulty":           "",
		"estimated_cost":       "",
		"industry_viability":   "",
		"time_to_mvp":          "",
		"required_expertise":   []string{},
		"potential_challenges": []string{},
		"recommendation":       "",
	}

	result, err := f.structured.Generate(ctx, prompt, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate feasibility: %w", err)
	}

	var feasibility FeasibilityResult
	if err := json.Unmarshal([]byte(result), &feasibility); err != nil {
		return nil, fmt.Errorf("failed to parse feasibility: %w", err)
	}

	return &feasibility, nil
}

func (f *FeasibilityEvaluator) storeDirection(ctx context.Context, topicID string, gap ResearchGap, feasibility *FeasibilityResult) error {
	var score float64
	switch feasibility.Difficulty {
	case "low":
		score = 0.9
	case "medium":
		score = 0.6
	case "high":
		score = 0.3
	default:
		score = 0.5
	}

	title := fmt.Sprintf("Research Direction: %s", gap.Title)
	description := fmt.Sprintf("%s\n\n%s", gap.Description, feasibility.Recommendation)
	rationale := fmt.Sprintf("Based on gap analysis. Evidence: %s", gap.Evidence)

	_, err := f.postgres.Queries().CreateNovelDirection(ctx, postgres.CreateNovelDirectionParams{
		TopicID:                  pgUUID(topicID),
		Title:                    title,
		Description:              pgText(description),
		Rationale:                pgText(rationale),
		FeasibilityScore:         pgFloat64(score),
		ImplementationComplexity: pgText(feasibility.Difficulty),
		EstimatedCost:            pgText(feasibility.EstimatedCost),
		IndustryViability:        pgText(feasibility.IndustryViability),
		TimeToMvp:                pgText(feasibility.TimeToMVP),
	})

	return err
}

func (f *FeasibilityEvaluator) GetTopDirections(ctx context.Context, topicID string, limit int) ([]*postgres.NovelDirection, error) {
	directions, err := f.postgres.Queries().GetNovelDirectionsByTopic(ctx, pgUUID(topicID))
	if err != nil {
		return nil, err
	}

	if len(directions) > limit {
		directions = directions[:limit]
	}

	return directions, nil
}
