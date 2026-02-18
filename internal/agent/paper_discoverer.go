package agent

import (
	"context"
	"strings"

	"github.com/research-agent/internal/logger"
	"github.com/research-agent/internal/storage/postgres"
	"github.com/research-agent/internal/tools/arxiv"
	"github.com/research-agent/internal/tools/semantic_scholar"
)

type PaperDiscoverer struct {
	ssClient  *semantic_scholar.Client
	arxiv     *arxiv.Client
	postgres  *postgres.Client
	maxPapers int
}

func NewPaperDiscoverer(ss *semantic_scholar.Client, arxiv *arxiv.Client, pg *postgres.Client, maxPapers int) *PaperDiscoverer {
	return &PaperDiscoverer{
		ssClient:  ss,
		arxiv:     arxiv,
		postgres:  pg,
		maxPapers: maxPapers,
	}
}

type DiscoveredPaper struct {
	ID         string
	Title      string
	Abstract   string
	Source     string
	ExternalID string
	URL        string
	PDFURL     string
	Year       int
	Venue      string
	Authors    []string
	DOI        string
}

func (d *PaperDiscoverer) Discover(ctx context.Context, topicID string, queries []string, keywords []string) ([]DiscoveredPaper, error) {
	logger.Info().
		Str("topic_id", topicID).
		Int("queries", len(queries)).
		Msg("Starting paper discovery")

	papersMap := make(map[string]DiscoveredPaper)

	for _, query := range queries {
		if len(papersMap) >= d.maxPapers {
			break
		}

		ssPapers, err := d.searchSemanticScholar(ctx, query, d.maxPapers-len(papersMap))
		if err != nil {
			logger.Warn().Err(err).Str("query", query).Msg("Semantic Scholar search failed")
		}

		for _, p := range ssPapers {
			key := "ss:" + p.ExternalID
			if _, exists := papersMap[key]; !exists {
				papersMap[key] = p
			}
		}
	}

	for _, query := range queries {
		if len(papersMap) >= d.maxPapers {
			break
		}

		arxivPapers, err := d.searchArXiv(ctx, query, d.maxPapers-len(papersMap))
		if err != nil {
			logger.Warn().Err(err).Str("query", query).Msg("arXiv search failed")
		}

		for _, p := range arxivPapers {
			key := "arxiv:" + p.ExternalID
			if _, exists := papersMap[key]; !exists {
				papersMap[key] = p
			}
		}
	}

	papers := make([]DiscoveredPaper, 0, len(papersMap))
	for _, p := range papersMap {
		papers = append(papers, p)
	}

	for i := range papers {
		if err := d.storePaper(ctx, topicID, papers[i]); err != nil {
			logger.Warn().Err(err).Str("external_id", papers[i].ExternalID).Msg("Failed to store paper")
		}
	}

	logger.Info().
		Str("topic_id", topicID).
		Int("papers_found", len(papers)).
		Msg("Paper discovery complete")

	return papers, nil
}

func (d *PaperDiscoverer) searchSemanticScholar(ctx context.Context, query string, limit int) ([]DiscoveredPaper, error) {
	resp, err := d.ssClient.Search(ctx, query, limit, 0)
	if err != nil {
		return nil, err
	}

	papers := make([]DiscoveredPaper, 0, len(resp.Data))
	for _, p := range resp.Data {
		paper := DiscoveredPaper{
			ID:         p.PaperID,
			Title:      p.Title,
			Abstract:   p.Abstract,
			Source:     "semantic_scholar",
			ExternalID: p.PaperID,
			URL:        p.URL,
			PDFURL:     p.GetPDFURL(),
			Year:       p.Year,
			Venue:      p.Venue,
			DOI:        p.GetDOI(),
		}

		for _, a := range p.Authors {
			paper.Authors = append(paper.Authors, a.Name)
		}

		papers = append(papers, paper)
	}

	return papers, nil
}

func (d *PaperDiscoverer) searchArXiv(ctx context.Context, query string, limit int) ([]DiscoveredPaper, error) {
	arxivQuery := arxiv.BuildQuery(query, nil)
	feed, err := d.arxiv.Search(ctx, arxivQuery, limit)
	if err != nil {
		return nil, err
	}

	papers := make([]DiscoveredPaper, 0, len(feed.Entries))
	for _, e := range feed.Entries {
		paper := DiscoveredPaper{
			ID:         e.GetArXivID(),
			Title:      strings.TrimSpace(e.Title),
			Abstract:   e.CleanAbstract(),
			Source:     "arxiv",
			ExternalID: e.GetArXivID(),
			URL:        e.GetAbstractURL(),
			PDFURL:     e.GetPDFURL(),
		}

		if t, err := e.GetPublishedTime(); err == nil {
			paper.Year = t.Year()
		}

		for _, a := range e.Authors {
			paper.Authors = append(paper.Authors, a.Name)
		}

		papers = append(papers, paper)
	}

	return papers, nil
}

func (d *PaperDiscoverer) storePaper(ctx context.Context, topicID string, paper DiscoveredPaper) error {
	_, err := d.postgres.Queries().CreatePaper(ctx, postgres.CreatePaperParams{
		TopicID:         pgUUID(topicID),
		Source:          paper.Source,
		ExternalID:      paper.ExternalID,
		SourceUrl:       pgText(paper.URL),
		Title:           paper.Title,
		Abstract:        pgText(paper.Abstract),
		PublicationDate: pgDate(paper.Year),
		Venue:           pgText(paper.Venue),
		PdfUrl:          pgText(paper.PDFURL),
	})

	return err
}
