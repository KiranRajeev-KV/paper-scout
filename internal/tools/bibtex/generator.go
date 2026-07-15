package bibtex

import (
	"fmt"
	"strings"
	"unicode"
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
		writeField(&b, "author", strings.Join(entry.Authors, " and "))
	}

	if entry.Title != "" {
		writeField(&b, "title", entry.Title)
	}

	if entry.Year > 0 {
		b.WriteString(fmt.Sprintf("  year = {%d},\n", entry.Year))
	}

	if entry.Venue != "" {
		switch entryType {
		case "inproceedings":
			writeField(&b, "booktitle", entry.Venue)
		case "article":
			writeField(&b, "journal", entry.Venue)
		default:
			writeField(&b, "journal", entry.Venue)
		}
	}

	if entry.Publisher != "" {
		writeField(&b, "publisher", entry.Publisher)
	}

	if entry.DOI != "" {
		writeField(&b, "doi", entry.DOI)
	}

	if entry.URL != "" {
		writeField(&b, "url", entry.URL)
	}

	if entry.ArXivID != "" {
		writeField(&b, "eprint", entry.ArXivID)
		b.WriteString("  archiveprefix = {arXiv},\n")
	}

	if entry.Abstract != "" {
		writeField(&b, "abstract", entry.Abstract)
	}

	b.WriteString("}\n")

	return b.String()
}

func writeField(b *strings.Builder, name, value string) {
	fmt.Fprintf(b, "  %s = {%s},\n", name, sanitizeField(value))
}

// sanitizeField converts untrusted API metadata into a valid, readable BibTeX
// field. Source metadata is plain text, not trusted LaTeX: grouping braces and
// presentational commands are flattened before the remaining TeX-special
// characters are escaped by this serializer.
func sanitizeField(value string) string {
	plain := flattenTeX(value)
	var b strings.Builder
	for _, r := range plain {
		switch r {
		case '\\':
			b.WriteString("\\textbackslash{}")
		case '{':
			b.WriteString("\\textbraceleft{}")
		case '}':
			b.WriteString("\\textbraceright{}")
		case '%', '&', '#', '$', '_':
			b.WriteByte('\\')
			b.WriteRune(r)
		case '^':
			b.WriteString("\\textasciicircum{}")
		case '~':
			b.WriteString("\\textasciitilde{}")
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

func flattenTeX(value string) string {
	var b strings.Builder
	for _, r := range value {
		if unicode.IsSpace(r) {
			if b.Len() > 0 {
				b.WriteByte(' ')
			}
			continue
		}
		b.WriteRune(r)
	}

	return strings.TrimSpace(flattenTeXFragment(b.String()))
}

func flattenTeXFragment(value string) string {
	var b strings.Builder
	for i := 0; i < len(value); {
		switch value[i] {
		case '{', '}':
			// Braces in externally supplied metadata are TeX grouping, not
			// bibliography structure. Dropping them also tolerates malformed
			// unmatched groups from upstream sources.
			i++
		case '\\':
			i++
			if i >= len(value) {
				break
			}
			if isTeXLetter(value[i]) {
				start := i
				for i < len(value) && isTeXLetter(value[i]) {
					i++
				}
				command := value[start:i]
				switch command {
				case "LaTeX":
					b.WriteString("LaTeX")
				case "TeX":
					b.WriteString("TeX")
				case "ldots", "dots":
					b.WriteString("…")
				default:
					// Formatting commands such as \textbf and \emph commonly
					// precede a brace-delimited argument, which the main loop
					// retains as readable text. Unknown commands are discarded
					// rather than emitted as executable LaTeX.
				}
				continue
			}
			switch value[i] {
			case '{':
				b.WriteByte('{')
			case '}':
				b.WriteByte('}')
			case '%', '&', '#', '$', '_', '^', '~', '\\':
				b.WriteByte(value[i])
			case ' ':
				b.WriteByte(' ')
			default:
				b.WriteByte(value[i])
			}
			i++
		default:
			b.WriteByte(value[i])
			i++
		}
	}
	return b.String()
}

func isTeXLetter(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
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
