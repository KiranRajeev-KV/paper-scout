package bibtex

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Protects generated fields from malformed or executable TeX supplied by metadata APIs.
func TestGeneratorSanitizesUntrustedTeXAcrossAllFields(t *testing.T) {
	entry := &Entry{
		ID:        "paper-1",
		Authors:   []string{`Ada \textbf{Lovelace}`, "Björk, Åsa"},
		Title:     `A \textit{Study} of C_2 & 100% {Results}`,
		Venue:     `Proceedings of {ACM} #1`,
		Publisher: `A~B ^ C $ D`,
		DOI:       `10.1000/a_b#c`,
		URL:       `https://example.test/a_b?x=1&y=2`,
		Abstract:  `We introduce \textbf{PSRD} (\textbf{Phase-wise \textbf{S}elf-\textbf{R}eward \textbf{D}ecoding) with an unmatched {group.`,
	}

	got := NewGenerator().Generate(entry)
	for _, want := range []string{
		`author = {Ada Lovelace and Björk, Åsa}`,
		`title = {A Study of C\_2 \& 100\% Results}`,
		`booktitle = {Proceedings of ACM \#1}`,
		`publisher = {A\textasciitilde{}B \textasciicircum{} C \$ D}`,
		`doi = {10.1000/a\_b\#c}`,
		`url = {https://example.test/a\_b?x=1\&y=2}`,
		`abstract = {We introduce PSRD (Phase-wise Self-Reward Decoding) with an unmatched group.}`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("generated entry missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, `\textbf`) || strings.Contains(got, `\textit`) {
		t.Errorf("generated entry retains untrusted TeX command:\n%s", got)
	}
}

// Protects generated bibliographies from Biber syntax and data-model failures.
func TestGeneratorOutputValidatesWithBiber(t *testing.T) {
	if _, err := exec.LookPath("biber"); err != nil {
		t.Skip("biber is not installed")
	}

	bib := NewGenerator().Generate(&Entry{
		ID:       "paper-1",
		Authors:  []string{`Ada \textbf{Lovelace}`, "Björk, Åsa"},
		Title:    `A \textit{Study} of C_2 & 100% {Results}`,
		Abstract: `We introduce \textbf{PSRD} (\textbf{Phase-wise \textbf{S}elf-\textbf{R}eward \textbf{D}ecoding) with an unmatched {group.`,
	})

	path := filepath.Join(t.TempDir(), "references.bib")
	if err := os.WriteFile(path, []byte(bib), 0o600); err != nil {
		t.Fatalf("write bibliography: %v", err)
	}
	command := exec.Command("biber", "--tool", "--validate-datamodel", path)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("biber validation failed: %v\n%s\n%s", err, output, bib)
	}
}
