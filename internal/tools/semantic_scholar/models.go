package semantic_scholar

type SearchResponse struct {
	Total   int     `json:"total"`
	Offset  int     `json:"offset"`
	Next    int     `json:"next"`
	Data    []Paper `json:"data"`
	Message string  `json:"message,omitempty"`
}

type Paper struct {
	PaperID         string           `json:"paperId"`
	Title           string           `json:"title"`
	Abstract        string           `json:"abstract"`
	Year            int              `json:"year"`
	Authors         []Author         `json:"authors"`
	Venue           string           `json:"venue"`
	URL             string           `json:"url"`
	OpenAccessPDF   *OpenAccessPDF   `json:"openAccessPdf"`
	CitationCount   int              `json:"citationCount"`
	PublicationDate string           `json:"publicationDate"`
	ExternalIDs     *ExternalIDs     `json:"externalIds"`
	References      []PaperReference `json:"references,omitempty"`
	Citations       []PaperReference `json:"citations,omitempty"`
}

type Author struct {
	AuthorID string `json:"authorId"`
	Name     string `json:"name"`
}

type OpenAccessPDF struct {
	URL string `json:"url"`
}

type ExternalIDs struct {
	DOI    string `json:"DOI"`
	ArXiv  string `json:"ArXiv"`
	PMID   string `json:"PubMed"`
	PMCID  string `json:"PubMedCentral"`
	MAG    int    `json:"MAG"`
	ACL    string `json:"ACL"`
	Corpus int64  `json:"CorpusId"`
}

type PaperReference struct {
	PaperID string `json:"paperId"`
	Title   string `json:"title"`
	Year    int    `json:"year"`
}

func (p *Paper) GetDOI() string {
	if p.ExternalIDs != nil {
		return p.ExternalIDs.DOI
	}
	return ""
}

func (p *Paper) GetArXivID() string {
	if p.ExternalIDs != nil {
		return p.ExternalIDs.ArXiv
	}
	return ""
}

func (p *Paper) GetPDFURL() string {
	if p.OpenAccessPDF != nil {
		return p.OpenAccessPDF.URL
	}
	return ""
}

func (p *Paper) HasPDF() bool {
	return p.OpenAccessPDF != nil && p.OpenAccessPDF.URL != ""
}
