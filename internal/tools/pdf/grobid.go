package pdf

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"time"

	"github.com/research-agent/internal/logger"
)

type GrobidClient struct {
	httpClient *http.Client
	baseURL    string
}

func NewGrobidClient(baseURL string, timeout time.Duration) *GrobidClient {
	return &GrobidClient{
		httpClient: &http.Client{
			Timeout: timeout,
		},
		baseURL: baseURL,
	}
}

// TEI XML structures
type TEI struct {
	XMLName xml.Name `xml:"TEI"`
	Text    Text     `xml:"text"`
}

type Text struct {
	Body Body `xml:"body"`
}

type Body struct {
	Divs []Div `xml:"div"`
}

type Div struct {
	Head string   `xml:"head"`
	Ps   []string `xml:"p"`
}

func (g *GrobidClient) Parse(ctx context.Context, filename string, data []byte) (*TEI, error) {
	start := time.Now()

	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	part, err := writer.CreateFormFile("input", filename)
	if err != nil {
		return nil, fmt.Errorf("failed to create form file: %w", err)
	}
	if _, err := part.Write(data); err != nil {
		return nil, fmt.Errorf("failed to write file data: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("failed to close writer: %w", err)
	}

	url := fmt.Sprintf("%s/api/processFulltextDocument", g.baseURL)
	req, err := http.NewRequestWithContext(ctx, "POST", url, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", writer.FormDataContentType())
	req.Header.Set("Accept", "application/xml")

	resp, err := g.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("grobid request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("grobid failed with status %d: %s", resp.StatusCode, string(respBody))
	}

	var tei TEI
	if err := xml.Unmarshal(respBody, &tei); err != nil {
		return nil, fmt.Errorf("failed to parse XML response: %w", err)
	}

	logger.Debug().
		Int("divs", len(tei.Text.Body.Divs)).
		Dur("duration", time.Since(start)).
		Msg("PDF parsed with Grobid")

	return &tei, nil
}

func (g *GrobidClient) ExtractText(tei *TEI) string {
	var sb strings.Builder
	for _, div := range tei.Text.Body.Divs {
		if div.Head != "" {
			sb.WriteString(div.Head + "\n")
		}
		for _, p := range div.Ps {
			sb.WriteString(p + "\n")
		}
		sb.WriteString("\n")
	}
	return sb.String()
}

func (g *GrobidClient) ExtractSections(tei *TEI) map[string]string {
	sections := make(map[string]string)
	for _, div := range tei.Text.Body.Divs {
		if div.Head == "" {
			continue
		}
		
		var content strings.Builder
		for _, p := range div.Ps {
			content.WriteString(p + "\n")
		}
		sections[div.Head] = content.String()
	}
	return sections
}
