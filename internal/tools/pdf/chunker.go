package pdf

import (
	"strings"
)

type Chunk struct {
	Text      string
	Index     int
	WordCount int
	Heading   string
}

// ChunkMarkdown preserves the nearest Markdown heading as chunk provenance.
func ChunkMarkdown(markdown string, maxWords, overlap int) []Chunk {
	type section struct {
		heading string
		lines   []string
	}
	sections := []section{{}}
	for _, line := range strings.Split(markdown, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") {
			heading := strings.TrimSpace(strings.TrimLeft(trimmed, "#"))
			if heading != "" {
				sections = append(sections, section{heading: heading})
				continue
			}
		}
		sections[len(sections)-1].lines = append(sections[len(sections)-1].lines, line)
	}
	var result []Chunk
	for _, section := range sections {
		body := strings.TrimSpace(strings.Join(section.lines, "\n"))
		if body == "" {
			continue
		}
		for _, chunk := range ChunkByParagraphsWithOverlap(body, maxWords, overlap) {
			chunk.Index = len(result)
			chunk.Heading = section.heading
			result = append(result, chunk)
		}
	}
	return result
}

func ChunkText(text string, maxWords int, overlap int) []Chunk {
	words := strings.Fields(text)
	if len(words) == 0 {
		return nil
	}

	var chunks []Chunk
	start := 0
	index := 0

	for start < len(words) {
		end := start + maxWords
		if end > len(words) {
			end = len(words)
		}

		chunkWords := words[start:end]
		chunkText := strings.Join(chunkWords, " ")

		chunks = append(chunks, Chunk{
			Text:      chunkText,
			Index:     index,
			WordCount: len(chunkWords),
		})

		index++
		start = end - overlap
		if start < 0 {
			start = 0
		}
		if start >= len(words) {
			break
		}
		if end >= len(words) {
			break
		}
	}

	return chunks
}

// ChunkByParagraphsWithOverlap preserves paragraph boundaries whenever possible
// and uses word chunks only for a paragraph larger than the configured limit.
func ChunkByParagraphsWithOverlap(text string, maxWords, overlap int) []Chunk {
	if maxWords <= 0 {
		return nil
	}
	if overlap < 0 {
		overlap = 0
	}
	if overlap >= maxWords {
		overlap = maxWords - 1
	}

	paragraphs := strings.Split(text, "\n\n")
	var chunks []Chunk
	var currentParts []string
	currentWords := 0
	pendingOverlap := ""

	flush := func() {
		if len(currentParts) == 0 {
			return
		}
		chunks = append(chunks, Chunk{
			Text:      strings.Join(currentParts, "\n\n"),
			Index:     len(chunks),
			WordCount: currentWords,
		})
		currentParts = nil
		currentWords = 0
	}
	startChunk := func(nextWords int) {
		if pendingOverlap == "" || nextWords >= maxWords {
			pendingOverlap = ""
			return
		}
		carry := trailingWords(pendingOverlap, min(overlap, maxWords-nextWords))
		pendingOverlap = ""
		if carry == "" {
			return
		}
		currentParts = append(currentParts, carry)
		currentWords = len(strings.Fields(carry))
	}

	for _, para := range paragraphs {
		para = strings.TrimSpace(para)
		if para == "" {
			continue
		}

		paraWords := len(strings.Fields(para))

		if paraWords > maxWords {
			if currentWords > 0 {
				pendingOverlap = trailingWords(strings.Join(currentParts, "\n\n"), overlap)
				flush()
			}
			if pendingOverlap != "" {
				para = pendingOverlap + "\n\n" + para
				pendingOverlap = ""
			}
			flush()
			for _, chunk := range ChunkText(para, maxWords, overlap) {
				chunk.Index = len(chunks)
				chunks = append(chunks, chunk)
			}
			if len(chunks) > 0 {
				pendingOverlap = trailingWords(chunks[len(chunks)-1].Text, overlap)
			}
			continue
		}

		if currentWords == 0 {
			startChunk(paraWords)
		}
		if currentWords+paraWords > maxWords {
			pendingOverlap = trailingWords(strings.Join(currentParts, "\n\n"), overlap)
			flush()
			startChunk(paraWords)
		}
		currentParts = append(currentParts, para)
		currentWords += paraWords
	}

	flush()
	return chunks
}

func trailingWords(text string, count int) string {
	if count <= 0 {
		return ""
	}
	words := strings.Fields(text)
	if len(words) <= count {
		return strings.Join(words, " ")
	}
	return strings.Join(words[len(words)-count:], " ")
}

func min(first, second int) int {
	if first < second {
		return first
	}
	return second
}
