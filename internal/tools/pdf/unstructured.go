package pdf

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/research-agent/internal/logger"
)

type UnstructuredClient struct {
	httpClient *http.Client
	baseURL    string
}

func NewUnstructuredClient(baseURL string, timeout time.Duration) *UnstructuredClient {
	return &UnstructuredClient{
		httpClient: &http.Client{
			Timeout: timeout,
		},
		baseURL: baseURL,
	}
}

type ParsedElement struct {
	Type     string                 `json:"type"`
	Text     string                 `json:"text"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

type ParseResponse struct {
	Elements []ParsedElement `json:"elements"`
}

func (u *UnstructuredClient) Parse(ctx context.Context, filename string, data []byte) (*ParseResponse, error) {
	start := time.Now()

	reqBody := map[string]interface{}{
		"files": []map[string]interface{}{
			{
				"filename":     filename,
				"content":      data,
				"content_type": "application/pdf",
			},
		},
		"strategy": "hi_res",
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	url := u.baseURL + "/general/v0/general/parsed"
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := u.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("parse request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("parse failed with status %d: %s", resp.StatusCode, string(body))
	}

	var parseResp ParseResponse
	if err := json.Unmarshal(body, &parseResp); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	logger.Debug().
		Int("elements", len(parseResp.Elements)).
		Dur("duration", time.Since(start)).
		Msg("PDF parsed")

	return &parseResp, nil
}

func (u *UnstructuredClient) ExtractText(resp *ParseResponse) string {
	var textParts []string
	for _, elem := range resp.Elements {
		if elem.Text != "" {
			switch elem.Type {
			case "NarrativeText", "ListItem", "Title", "Header", "Footer":
				textParts = append(textParts, elem.Text)
			}
		}
	}
	return joinText(textParts)
}

func (u *UnstructuredClient) ExtractSections(resp *ParseResponse) map[string]string {
	sections := make(map[string]string)
	var currentSection string
	var currentText []string

	for _, elem := range resp.Elements {
		if elem.Type == "Title" && elem.Text != "" {
			if currentSection != "" && len(currentText) > 0 {
				sections[currentSection] = joinText(currentText)
			}
			currentSection = elem.Text
			currentText = nil
		} else if elem.Text != "" {
			currentText = append(currentText, elem.Text)
		}
	}

	if currentSection != "" && len(currentText) > 0 {
		sections[currentSection] = joinText(currentText)
	}

	return sections
}

func joinText(parts []string) string {
	result := ""
	for i, part := range parts {
		if i > 0 {
			result += "\n"
		}
		result += part
	}
	return result
}
