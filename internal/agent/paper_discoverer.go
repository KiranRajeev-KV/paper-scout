package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode"

	"github.com/paper-scout/internal/logger"
	"github.com/paper-scout/internal/storage/postgres"
	"github.com/paper-scout/internal/tools/arxiv"
	"github.com/paper-scout/internal/tools/semantic_scholar"
	"golang.org/x/sync/errgroup"
)

type PaperDiscoverer struct {
	ssClient  *semantic_scholar.Client
	arxiv     *arxiv.Client
	postgres  *postgres.Client
	maxPapers int

	searchSemanticScholarFn func(context.Context, string, int) ([]DiscoveredPaper, error)
	searchArXivFn           func(context.Context, string, int, []string) ([]DiscoveredPaper, error)
	storePaperFn            func(context.Context, string, DiscoveredPaper) error
}

func NewPaperDiscoverer(ss *semantic_scholar.Client, arxiv *arxiv.Client, pg *postgres.Client, maxPapers int) *PaperDiscoverer {
	d := &PaperDiscoverer{
		ssClient:  ss,
		arxiv:     arxiv,
		postgres:  pg,
		maxPapers: maxPapers,
	}
	d.searchSemanticScholarFn = d.searchSemanticScholar
	d.searchArXivFn = d.searchArXiv
	d.storePaperFn = d.storePaper
	return d
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
	Authors    []DiscoveredAuthor
	DOI        string
	ArXivID    string
}

type DiscoveredAuthor struct {
	Name              string
	SemanticScholarID string
}

type discoverySearchResult struct {
	index  int
	source string
	query  string
	papers []DiscoveredPaper
	err    error
}

func (d *PaperDiscoverer) Discover(ctx context.Context, topicID string, queries []string, keywords []string) ([]DiscoveredPaper, error) {
	logger.Info().
		Str("topic_id", topicID).
		Int("queries", len(queries)).
		Msg("Starting paper discovery")

	if d.maxPapers <= 0 || len(queries) == 0 {
		return nil, nil
	}
	if len(queries) > d.maxPapers {
		queries = queries[:d.maxPapers]
	}

	results := make([]discoverySearchResult, len(queries)*2)
	baseQueryLimit := d.maxPapers / len(queries)
	remainingQueryLimit := d.maxPapers % len(queries)
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(4)
	for queryIndex, query := range queries {
		perQueryLimit := baseQueryLimit
		if queryIndex < remainingQueryLimit {
			perQueryLimit++
		}
		resultIndex := queryIndex * 2
		results[resultIndex] = discoverySearchResult{index: resultIndex, source: "semantic_scholar", query: query}
		results[resultIndex+1] = discoverySearchResult{index: resultIndex + 1, source: "arxiv", query: query}

		ssSearch := d.searchSemanticScholarFn
		if ssSearch == nil {
			ssSearch = func(ctx context.Context, query string, limit int) ([]DiscoveredPaper, error) {
				return d.searchSemanticScholar(ctx, query, limit)
			}
		}
		arxivSearch := d.searchArXivFn
		if arxivSearch == nil {
			arxivSearch = func(ctx context.Context, query string, limit int, keywords []string) ([]DiscoveredPaper, error) {
				return d.searchArXiv(ctx, query, limit, keywords)
			}
		}

		group.Go(func() error {
			papers, err := ssSearch(groupCtx, query, perQueryLimit)
			results[resultIndex].papers = papers
			results[resultIndex].err = err
			return nil
		})
		group.Go(func() error {
			papers, err := arxivSearch(groupCtx, query, perQueryLimit, keywords)
			results[resultIndex+1].papers = papers
			results[resultIndex+1].err = err
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var searchErrors []error
	hadSuccess := false
	for _, result := range results {
		if result.err != nil {
			searchErrors = append(searchErrors, result.err)
			logger.Warn().Err(result.err).Str("source", result.source).Str("query", result.query).Msg("Paper search failed")
			continue
		}
		hadSuccess = true
	}
	if !hadSuccess && len(searchErrors) > 0 {
		return nil, errors.Join(searchErrors...)
	}

	papers := reconcileDiscoveredPapers(results)
	if len(papers) > d.maxPapers {
		papers = papers[:d.maxPapers]
	}

	storePaper := d.storePaperFn
	if storePaper == nil {
		storePaper = d.storePaper
	}
	for i := range papers {
		if err := storePaper(ctx, topicID, papers[i]); err != nil {
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
			ArXivID:    p.GetArXivID(),
			URL:        p.URL,
			PDFURL:     p.GetPDFURL(),
			Year:       p.Year,
			Venue:      p.Venue,
			DOI:        p.GetDOI(),
		}

		for _, a := range p.Authors {
			paper.Authors = append(paper.Authors, DiscoveredAuthor{
				Name:              a.Name,
				SemanticScholarID: a.AuthorID,
			})
		}

		papers = append(papers, paper)
	}

	return papers, nil
}

func (d *PaperDiscoverer) searchArXiv(ctx context.Context, query string, limit int, keywords []string) ([]DiscoveredPaper, error) {
	arxivQuery := arxiv.BuildQuery(query, keywords)
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
			ArXivID:    e.GetArXivID(),
			URL:        e.GetAbstractURL(),
			PDFURL:     e.GetPDFURL(),
			DOI:        strings.TrimSpace(e.DOI),
		}

		if t, err := e.GetPublishedTime(); err == nil {
			paper.Year = t.Year()
		}

		for _, a := range e.Authors {
			paper.Authors = append(paper.Authors, DiscoveredAuthor{Name: a.Name})
		}

		papers = append(papers, paper)
	}

	return papers, nil
}

func reconcileDiscoveredPapers(results []discoverySearchResult) []DiscoveredPaper {
	// This function is kept separate from the concurrent search so reconciliation
	// always follows query/source order, independent of completion timing.
	papers := make([]DiscoveredPaper, 0)
	identityToIndex := make(map[string]int)
	titleYearCandidates := make(map[string][]int)
	for _, source := range []string{"semantic_scholar", "arxiv"} {
		for _, result := range results {
			if result.source != source || result.err != nil {
				continue
			}
			for _, candidate := range result.papers {
				keys := paperIdentityKeys(candidate)
				matched := -1
				for _, key := range keys {
					if index, ok := identityToIndex[key]; ok {
						matched = index
						break
					}
				}
				if matched < 0 {
					candidateKey := titleYearKey(candidate)
					if candidateKey != "" {
						for _, index := range titleYearCandidates[candidateKey] {
							if papers[index].Source != candidate.Source && sharedAuthor(papers[index], candidate) {
								matched = index
								break
							}
						}
					}
				}
				if matched < 0 {
					matched = len(papers)
					papers = append(papers, candidate)
				} else {
					papers[matched] = mergeDiscoveredPaper(papers[matched], candidate)
				}
				for _, key := range paperIdentityKeys(papers[matched]) {
					identityToIndex[key] = matched
				}
				if candidateKey := titleYearKey(papers[matched]); candidateKey != "" {
					titleYearCandidates[candidateKey] = appendUniqueIndex(titleYearCandidates[candidateKey], matched)
				}
			}
		}
	}
	return papers
}

func paperIdentityKeys(paper DiscoveredPaper) []string {
	keys := make([]string, 0, 3)
	if doi := normalizeDOI(paper.DOI); doi != "" {
		keys = append(keys, "doi:"+doi)
	}
	if arxivID := normalizeArXivID(paper.ArXivID); arxivID != "" {
		keys = append(keys, "arxiv:"+arxivID)
	}
	if len(keys) == 0 && strings.TrimSpace(paper.ExternalID) != "" {
		keys = append(keys, "source:"+strings.ToLower(strings.TrimSpace(paper.Source))+":"+strings.ToLower(strings.TrimSpace(paper.ExternalID)))
	}
	return keys
}

func titleYearKey(paper DiscoveredPaper) string {
	title := normalizeTitle(paper.Title)
	if title == "" || paper.Year <= 0 {
		return ""
	}
	return "title:" + title + ":" + formatYear(paper.Year)
}

func sharedAuthor(a, b DiscoveredPaper) bool {
	authors := make(map[string]struct{}, len(a.Authors))
	for _, author := range a.Authors {
		name := normalizeTitle(author.Name)
		if name != "" {
			authors[name] = struct{}{}
		}
	}
	for _, author := range b.Authors {
		if _, ok := authors[normalizeTitle(author.Name)]; ok {
			return true
		}
	}
	return false
}

func appendUniqueIndex(indices []int, index int) []int {
	for _, existing := range indices {
		if existing == index {
			return indices
		}
	}
	return append(indices, index)
}

func mergeDiscoveredPaper(primary, secondary DiscoveredPaper) DiscoveredPaper {
	if primary.ID == "" {
		primary.ID = secondary.ID
	}
	if primary.Title == "" {
		primary.Title = secondary.Title
	}
	if primary.Abstract == "" {
		primary.Abstract = secondary.Abstract
	}
	if primary.URL == "" {
		primary.URL = secondary.URL
	}
	if primary.PDFURL == "" {
		primary.PDFURL = secondary.PDFURL
	}
	if primary.ExternalID == "" {
		primary.ExternalID = secondary.ExternalID
	}
	if primary.Year == 0 {
		primary.Year = secondary.Year
	}
	if primary.Venue == "" {
		primary.Venue = secondary.Venue
	}
	if primary.DOI == "" {
		primary.DOI = secondary.DOI
	}
	if primary.ArXivID == "" {
		primary.ArXivID = secondary.ArXivID
	}
	if len(primary.Authors) == 0 {
		primary.Authors = append([]DiscoveredAuthor(nil), secondary.Authors...)
	} else {
		seen := make(map[string]int, len(primary.Authors))
		for i, author := range primary.Authors {
			if name := normalizeTitle(author.Name); name != "" {
				seen[name] = i
			}
		}
		for _, author := range secondary.Authors {
			name := normalizeTitle(author.Name)
			if name == "" {
				continue
			}
			if existingIndex, exists := seen[name]; exists {
				if primary.Authors[existingIndex].SemanticScholarID == "" && author.SemanticScholarID != "" {
					primary.Authors[existingIndex].SemanticScholarID = author.SemanticScholarID
				}
				continue
			}
			if _, exists := seen[name]; !exists {
				primary.Authors = append(primary.Authors, author)
				seen[name] = len(primary.Authors) - 1
			}
		}
	}
	return primary
}

func normalizeDOI(doi string) string {
	doi = strings.ToLower(strings.TrimSpace(doi))
	doi = strings.TrimPrefix(doi, "https://doi.org/")
	doi = strings.TrimPrefix(doi, "http://doi.org/")
	doi = strings.TrimPrefix(doi, "doi:")
	return strings.Trim(strings.TrimSpace(doi), " .;,)")
}

func normalizeArXivID(id string) string {
	id = strings.ToLower(strings.TrimSpace(id))
	if slash := strings.LastIndex(id, "/"); slash >= 0 {
		id = id[slash+1:]
	}
	id = strings.TrimPrefix(id, "arxiv:")
	if v := strings.LastIndex(id, "v"); v > 0 {
		version := id[v+1:]
		if version != "" && strings.Trim(version, "0123456789") == "" {
			id = id[:v]
		}
	}
	return strings.TrimSpace(id)
}

func normalizeTitle(title string) string {
	var normalized strings.Builder
	for _, r := range strings.ToLower(title) {
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			normalized.WriteRune(r)
		} else {
			normalized.WriteByte(' ')
		}
	}
	return strings.Join(strings.Fields(normalized.String()), " ")
}

func formatYear(year int) string {
	if year <= 0 {
		return "unknown"
	}
	return fmt.Sprintf("%d", year)
}

func (d *PaperDiscoverer) storePaper(ctx context.Context, topicID string, paper DiscoveredPaper) error {
	return d.postgres.WithTx(ctx, func(q *postgres.Queries) error {
		paperID, err := q.CreatePaper(ctx, postgres.CreatePaperParams{
			TopicID:         pgUUID(topicID),
			DiscoverySource: paper.Source,
			ExternalID:      paper.ExternalID,
			SourceUrl:       pgText(paper.URL),
			Title:           paper.Title,
			Abstract:        pgText(paper.Abstract),
			PublicationDate: pgDate(paper.Year),
			Venue:           pgText(paper.Venue),
			PdfUrl:          pgText(paper.PDFURL),
		})
		if err != nil {
			return err
		}

		position, err := q.GetNextPaperAuthorPosition(ctx, paperID)
		if err != nil {
			return err
		}
		seen := make(map[string]struct{}, len(paper.Authors))
		for _, author := range paper.Authors {
			name := strings.TrimSpace(author.Name)
			key := normalizeTitle(name)
			if name == "" || key == "" {
				continue
			}
			if _, exists := seen[key]; exists {
				continue
			}
			seen[key] = struct{}{}

			var persisted *postgres.Author
			if strings.TrimSpace(author.SemanticScholarID) != "" {
				persisted, err = q.UpsertAuthorBySemanticScholarID(ctx, postgres.UpsertAuthorBySemanticScholarIDParams{
					Name:              name,
					SemanticScholarID: pgText(author.SemanticScholarID),
				})
			} else {
				persisted, err = q.UpsertAuthorByName(ctx, name)
			}
			if err != nil {
				return err
			}
			if err := q.AddPaperAuthor(ctx, postgres.AddPaperAuthorParams{
				PaperID:  paperID,
				AuthorID: persisted.ID,
				Position: position,
			}); err != nil {
				return err
			}
			position++
		}
		return nil
	})
}

func (d *PaperDiscoverer) ClearPapers(ctx context.Context, topicID string) error {
	return d.postgres.Queries().DeletePapersByTopic(ctx, pgUUID(topicID))
}

func (d *PaperDiscoverer) CountPapers(ctx context.Context, topicID string) (int64, error) {
	return d.postgres.Queries().CountPapersByTopic(ctx, pgUUID(topicID))
}
