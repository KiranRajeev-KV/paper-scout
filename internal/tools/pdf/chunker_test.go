package pdf

import (
	"strings"
	"testing"
)

// Protects chunk by paragraphs with overlap preserves paragraphs.
func TestChunkByParagraphsWithOverlapPreservesParagraphs(t *testing.T) {
	chunks := ChunkByParagraphsWithOverlap("one two\n\nthree four\n\nfive six", 4, 1)
	if len(chunks) != 2 {
		t.Fatalf("chunk count = %d, want 2", len(chunks))
	}
	if chunks[0].Text != "one two\n\nthree four" || chunks[1].Text != "four\n\nfive six" {
		t.Fatalf("chunks = %#v", chunks)
	}
	if chunks[1].WordCount != 3 {
		t.Fatalf("second chunk word count = %d, want 3", chunks[1].WordCount)
	}
	for index, chunk := range chunks {
		if chunk.Index != index {
			t.Fatalf("chunk index = %d, want %d", chunk.Index, index)
		}
	}
}

// Protects chunk by paragraphs with overlap splits oversized paragraph.
func TestChunkByParagraphsWithOverlapSplitsOversizedParagraph(t *testing.T) {
	text := strings.Repeat("word ", 10)
	chunks := ChunkByParagraphsWithOverlap(text, 4, 1)
	if len(chunks) != 3 {
		t.Fatalf("chunk count = %d, want 3", len(chunks))
	}
	if chunks[0].WordCount != 4 || chunks[1].WordCount != 4 || chunks[2].WordCount != 4 {
		t.Fatalf("word counts = %#v", chunks)
	}
	if !strings.HasSuffix(chunks[0].Text, "word") || !strings.HasPrefix(chunks[1].Text, "word") {
		t.Fatalf("missing overlapping boundary: %#v", chunks)
	}
}

// Protects chunk by paragraphs with overlap rejects invalid limit.
func TestChunkByParagraphsWithOverlapRejectsInvalidLimit(t *testing.T) {
	if chunks := ChunkByParagraphsWithOverlap("text", 0, 0); chunks != nil {
		t.Fatalf("chunks = %#v, want nil", chunks)
	}
}

// Protects Docling section headings as provenance on generated chunks.
func TestChunkMarkdownPreservesSectionHeadings(t *testing.T) {
	chunks := ChunkMarkdown("# Introduction\nalpha beta\n\n## Methods\ngamma delta", 10, 0)
	if len(chunks) != 2 {
		t.Fatalf("chunk count = %d, want 2", len(chunks))
	}
	if chunks[0].Heading != "Introduction" || chunks[1].Heading != "Methods" {
		t.Fatalf("headings = (%q, %q)", chunks[0].Heading, chunks[1].Heading)
	}
}
