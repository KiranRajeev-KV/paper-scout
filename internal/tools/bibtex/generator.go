package bibtex

import (
	"fmt"
	"strings"
	"time"
)

type Entry struct {
	ID        string
	Type      string
	Authors   []string
	Title     string
	Year      int
	Venue     string
	DOI       string
	URL       string
	ArXivID   string
	Abstract  string
	Publisher string
}

type Generator struct{}

func NewGenerator() *Generator {
	return &Generator{}
}

func (g *Generator) Generate(entry *Entry) string {
	var b strings.Builder

	entryType := g.determineType(entry)
	b.WriteString("@" + entryType + "{" + entry.ID + ",\n")

	if len(entry.Authors) > 0 {
		b.WriteString(fmt.Sprintf("  author = {%s},\n", strings.Join(entry.Authors, " and ")))
	}

	if entry.Title != "" {
		b.WriteString(fmt.Sprintf("  title = {%s},\n", entry.Title))
	}

	if entry.Year > 0 {
		b.WriteString(fmt.Sprintf("  year = {%d},\n", entry.Year))
	}

	if entry.Venue != "" {
		switch entryType {
		case "inproceedings":
			b.WriteString(fmt.Sprintf("  booktitle = {%s},\n", entry.Venue))
		case "article":
			b.WriteString(fmt.Sprintf("  journal = {%s},\n", entry.Venue))
		default:
			b.WriteString(fmt.Sprintf("  journal = {%s},\n", entry.Venue))
		}
	}

	if entry.Publisher != "" {
		b.WriteString(fmt.Sprintf("  publisher = {%s},\n", entry.Publisher))
	}

	if entry.DOI != "" {
		b.WriteString(fmt.Sprintf("  doi = {%s},\n", entry.DOI))
	}

	if entry.URL != "" {
		b.WriteString(fmt.Sprintf("  url = {%s},\n", entry.URL))
	}

	if entry.ArXivID != "" {
		b.WriteString(fmt.Sprintf("  eprint = {%s},\n", entry.ArXivID))
		b.WriteString("  archiveprefix = {arXiv},\n")
	}

	if entry.Abstract != "" {
		abstract := strings.ReplaceAll(entry.Abstract, "\n", " ")
		abstract = strings.ReplaceAll(abstract, "{", "\\{")
		abstract = strings.ReplaceAll(abstract, "}", "\\}")
		b.WriteString(fmt.Sprintf("  abstract = {%s},\n", abstract))
	}

	b.WriteString("}\n")

	return b.String()
}

func (g *Generator) GenerateBatch(entries []*Entry) string {
	var b strings.Builder
	for i, entry := range entries {
		if i > 0 {
			b.WriteString("\n")
		}
		b.WriteString(g.Generate(entry))
	}
	return b.String()
}

func (g *Generator) determineType(entry *Entry) string {
	if entry.ArXivID != "" && entry.Venue == "" {
		return "misc"
	}

	lowerVenue := strings.ToLower(entry.Venue)

	conferenceIndicators := []string{"conference", "proceedings", "workshop", "symposium"}
	for _, indicator := range conferenceIndicators {
		if strings.Contains(lowerVenue, indicator) {
			return "inproceedings"
		}
	}

	journalIndicators := []string{"journal", "transactions", "letters", "review", "magazine"}
	for _, indicator := range journalIndicators {
		if strings.Contains(lowerVenue, indicator) {
			return "article"
		}
	}

	if entry.Venue != "" {
		return "article"
	}

	return "misc"
}

func GenerateID(entry *Entry) string {
	var parts []string

	if len(entry.Authors) > 0 {
		author := entry.Authors[0]
		words := strings.Fields(author)
		if len(words) > 0 {
			surname := words[len(words)-1]
			surname = strings.ToLower(surname)
			surname = strings.Map(func(r rune) rune {
				if r >= 'a' && r <= 'z' {
					return r
				}
				return -1
			}, surname)
			parts = append(parts, surname)
		}
	}

	if entry.Year > 0 {
		parts = append(parts, fmt.Sprintf("%d", entry.Year))
	}

	if entry.Title != "" {
		words := strings.Fields(entry.Title)
		for _, word := range words {
			if len(word) > 4 {
				word = strings.ToLower(word)
				word = strings.Map(func(r rune) rune {
					if r >= 'a' && r <= 'z' {
						return r
					}
					return -1
				}, word)
				if word != "" {
					parts = append(parts, word)
					break
				}
			}
		}
	}

	if len(parts) == 0 {
		return fmt.Sprintf("unknown_%d", time.Now().UnixNano())
	}

	return strings.Join(parts, "_")
}

func FromPaper(paper interface {
	GetDOI() string
	GetArXivID() string
}, title string, authors []string, year int, venue, url, abstract string) *Entry {
	id := GenerateID(&Entry{
		Authors: authors,
		Title:   title,
		Year:    year,
	})

	if doiGetter, ok := paper.(interface{ GetDOI() string }); ok {
		if id == "" || strings.HasPrefix(id, "unknown") {
			if doi := doiGetter.GetDOI(); doi != "" {
				id = strings.ReplaceAll(doi, "/", "_")
			}
		}
	}

	return &Entry{
		ID:       id,
		Type:     "article",
		Authors:  authors,
		Title:    title,
		Year:     year,
		Venue:    venue,
		URL:      url,
		Abstract: abstract,
	}
}
