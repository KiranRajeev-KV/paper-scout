package arxiv

import (
	"encoding/xml"
	"strings"
	"time"
)

type Feed struct {
	XMLName xml.Name `xml:"feed"`
	Entries []Entry  `xml:"entry"`
	Title   string   `xml:"title"`
	Total   string   `xml:"totalResults"`
	Start   string   `xml:"startIndex"`
	PerPage string   `xml:"itemsPerPage"`
}

type Entry struct {
	ID              string     `xml:"id"`
	Title           string     `xml:"title"`
	Summary         string     `xml:"summary"`
	Authors         []Author   `xml:"author"`
	Links           []Link     `xml:"link"`
	Categories      []Category `xml:"category"`
	Published       string     `xml:"published"`
	Updated         string     `xml:"updated"`
	DOI             string     `xml:"doi"`
	Comment         string     `xml:"comment"`
	JournalRef      string     `xml:"journal_ref"`
	PrimaryCategory Category   `xml:"primary_category"`
}

type Author struct {
	Name        string `xml:"name"`
	Affiliation string `xml:"affiliation"`
}

type Link struct {
	Href  string `xml:"href,attr"`
	Rel   string `xml:"rel,attr"`
	Type  string `xml:"type,attr"`
	Title string `xml:"title,attr"`
}

type Category struct {
	Term   string `xml:"term,attr"`
	Scheme string `xml:"scheme,attr"`
}

func (e *Entry) GetArXivID() string {
	return ExtractArXivID(e.ID)
}

func (e *Entry) GetPDFURL() string {
	for _, link := range e.Links {
		if link.Title == "pdf" || strings.HasSuffix(link.Href, ".pdf") {
			return link.Href
		}
	}
	id := e.GetArXivID()
	return "https://arxiv.org/pdf/" + id + ".pdf"
}

func (e *Entry) GetAbstractURL() string {
	for _, link := range e.Links {
		if link.Rel == "alternate" && link.Type == "text/html" {
			return link.Href
		}
	}
	return e.ID
}

func (e *Entry) GetPublishedTime() (time.Time, error) {
	return time.Parse(time.RFC3339, e.Published)
}

func (e *Entry) GetCategories() []string {
	cats := make([]string, len(e.Categories))
	for i, cat := range e.Categories {
		cats[i] = cat.Term
	}
	return cats
}

func (e *Entry) CleanAbstract() string {
	return strings.TrimSpace(strings.ReplaceAll(e.Summary, "\n", " "))
}
