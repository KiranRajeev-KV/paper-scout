package prompts

const (
	QueryExpansionPrompt = `You are a research assistant. Given a research topic, generate:
1. Multiple search queries that would help find relevant academic papers
2. Related concepts and keywords
3. Potential subtopics to explore

Research Topic: {{.Topic}}

Respond in JSON format:
{
  "expanded_queries": ["query1", "query2", ...],
  "related_concepts": ["concept1", "concept2", ...],
  "subtopics": ["subtopic1", "subtopic2", ...],
  "search_keywords": ["keyword1", "keyword2", ...]
}`

	PaperAnalysisPrompt = `Analyze the following academic paper and extract structured information.

Title: {{.Title}}
Abstract: {{.Abstract}}
{{if .FullText}}Full Text Excerpt: {{.FullText}}{{end}}

Extract and respond in JSON format:
{
  "problem_statement": "What problem does this paper address?",
  "methodology": "What methods/approaches are used?",
  "dataset": "What datasets are used (or 'Not specified')?",
  "evaluation_metrics": ["metric1", "metric2"],
  "key_findings": "Main findings in 2-3 sentences",
  "limitations": "Limitations acknowledged by authors",
  "future_work": "Future work suggested by authors"
}`

	GapDetectionPrompt = `You are analyzing a collection of research papers to identify gaps and opportunities.

Topic: {{.Topic}}

Papers Summary:
{{range .Papers}}
- {{.Title}}: {{.KeyFindings}}
{{end}}

Identify research gaps and respond in JSON format:
{
  "gaps": [
    {
      "gap_type": "unexplored|conflicting|limitation",
      "title": "Brief title for the gap",
      "description": "Detailed description",
      "evidence": "Which papers support this finding",
      "related_paper_ids": ["id1", "id2"]
    }
  ]
}`

	FeasibilityPrompt = `Evaluate the feasibility of the following research direction.

Research Gap: {{.GapTitle}}
Description: {{.GapDescription}}
Related Papers: {{.RelatedPapers}}

Provide feasibility analysis in JSON format:
{
  "difficulty": "low|medium|high",
  "estimated_cost": "Description of resource requirements",
  "industry_viability": "Potential industry applications",
  "time_to_mvp": "Estimated time to minimum viable research",
  "required_expertise": ["skill1", "skill2"],
  "potential_challenges": ["challenge1", "challenge2"],
  "recommendation": "Brief recommendation"
}`

	ReportSummaryPrompt = `Generate an executive summary for a research analysis.

Topic: {{.Topic}}
Papers Analyzed: {{.PaperCount}}
Gaps Identified: {{.GapCount}}

Key Findings:
{{range .KeyFindings}}
- {{.}}
{{end}}

Generate a concise executive summary (3-4 paragraphs) suitable for a technical audience.`
)

type QueryExpansionInput struct {
	Topic string
}

type PaperAnalysisInput struct {
	Title    string
	Abstract string
	FullText string
}

type GapDetectionInput struct {
	Topic  string
	Papers []PaperSummary
}

type PaperSummary struct {
	ID          string
	Title       string
	KeyFindings string
}

type FeasibilityInput struct {
	GapTitle       string
	GapDescription string
	RelatedPapers  string
}

type ReportSummaryInput struct {
	Topic       string
	PaperCount  int
	GapCount    int
	KeyFindings []string
}
