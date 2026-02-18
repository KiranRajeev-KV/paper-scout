package pdf

import (
	"strings"
)

type Chunk struct {
	Text      string
	Index     int
	WordCount int
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

func ChunkByParagraphs(text string, maxWords int) []Chunk {
	paragraphs := strings.Split(text, "\n\n")
	var chunks []Chunk
	currentChunk := ""
	currentWords := 0
	index := 0

	for _, para := range paragraphs {
		para = strings.TrimSpace(para)
		if para == "" {
			continue
		}

		paraWords := len(strings.Fields(para))

		if currentWords+paraWords > maxWords && currentChunk != "" {
			chunks = append(chunks, Chunk{
				Text:      strings.TrimSpace(currentChunk),
				Index:     index,
				WordCount: currentWords,
			})
			index++
			currentChunk = ""
			currentWords = 0
		}

		if currentChunk != "" {
			currentChunk += "\n\n"
		}
		currentChunk += para
		currentWords += paraWords
	}

	if currentChunk != "" {
		chunks = append(chunks, Chunk{
			Text:      strings.TrimSpace(currentChunk),
			Index:     index,
			WordCount: currentWords,
		})
	}

	return chunks
}
