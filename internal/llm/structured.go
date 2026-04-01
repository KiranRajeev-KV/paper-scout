package llm

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/paper-scout/internal/logger"
	"google.golang.org/genai"
)

type StructuredOutput struct {
	client *Client
}

func NewStructuredOutput(client *Client) *StructuredOutput {
	return &StructuredOutput{client: client}
}

func (s *StructuredOutput) Generate(ctx context.Context, prompt string, responseSchema interface{}) (string, error) {
	schema, err := s.buildSchema(responseSchema)
	if err != nil {
		return "", fmt.Errorf("failed to build schema: %w", err)
	}

	cfg := &genai.GenerateContentConfig{
		ResponseMIMEType: "application/json",
		ResponseSchema:   schema,
	}

	result, err := s.client.GenerateWithConfig(ctx, prompt, cfg)
	if err != nil {
		return "", fmt.Errorf("structured generation failed: %w", err)
	}

	logger.Debug().Msg("Structured LLM output generated")
	return result, nil
}

func (s *StructuredOutput) GenerateInto(ctx context.Context, prompt string, responseSchema interface{}, dest interface{}) error {
	result, err := s.Generate(ctx, prompt, responseSchema)
	if err != nil {
		return err
	}

	if err := json.Unmarshal([]byte(result), dest); err != nil {
		return fmt.Errorf("failed to unmarshal structured output: %w", err)
	}

	return nil
}

func (s *StructuredOutput) buildSchema(v interface{}) (*genai.Schema, error) {
	return inferSchema(v)
}

func inferSchema(v interface{}) (*genai.Schema, error) {
	switch t := v.(type) {
	case map[string]interface{}:
		props := make(map[string]*genai.Schema)
		for key, val := range t {
			propSchema, err := inferSchema(val)
			if err != nil {
				return nil, err
			}
			props[key] = propSchema
		}
		return &genai.Schema{
			Type:       genai.TypeObject,
			Properties: props,
		}, nil

	case string:
		return &genai.Schema{Type: genai.TypeString}, nil

	case int, int32, int64:
		return &genai.Schema{Type: genai.TypeInteger}, nil

	case float32, float64:
		return &genai.Schema{Type: genai.TypeNumber}, nil

	case bool:
		return &genai.Schema{Type: genai.TypeBoolean}, nil

	case []string:
		return &genai.Schema{
			Type: genai.TypeArray,
			Items: &genai.Schema{
				Type: genai.TypeString,
			},
		}, nil

	case []interface{}:
		if len(t) == 0 {
			return &genai.Schema{
				Type: genai.TypeArray,
				Items: &genai.Schema{
					Type: genai.TypeString,
				},
			}, nil
		}
		itemSchema, err := inferSchema(t[0])
		if err != nil {
			return nil, err
		}
		return &genai.Schema{
			Type:  genai.TypeArray,
			Items: itemSchema,
		}, nil

	default:
		return &genai.Schema{Type: genai.TypeString}, nil
	}
}

func MustSchema(v interface{}) *genai.Schema {
	schema, err := inferSchema(v)
	if err != nil {
		panic(err)
	}
	return schema
}

func StringSchema() *genai.Schema {
	return &genai.Schema{Type: genai.TypeString}
}

func IntegerSchema() *genai.Schema {
	return &genai.Schema{Type: genai.TypeInteger}
}

func NumberSchema() *genai.Schema {
	return &genai.Schema{Type: genai.TypeNumber}
}

func BooleanSchema() *genai.Schema {
	return &genai.Schema{Type: genai.TypeBoolean}
}

func ArraySchema(itemSchema *genai.Schema) *genai.Schema {
	return &genai.Schema{
		Type:  genai.TypeArray,
		Items: itemSchema,
	}
}

func ObjectSchema(properties map[string]*genai.Schema) *genai.Schema {
	return &genai.Schema{
		Type:       genai.TypeObject,
		Properties: properties,
	}
}
