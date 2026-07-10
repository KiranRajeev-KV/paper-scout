package llm

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

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
	return inferSchemaValue(reflect.ValueOf(v))
}

func inferSchemaValue(value reflect.Value) (*genai.Schema, error) {
	if !value.IsValid() {
		return &genai.Schema{Type: genai.TypeString}, nil
	}

	for value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface {
		if value.IsNil() {
			return &genai.Schema{Type: genai.TypeString}, nil
		}
		value = value.Elem()
	}

	switch value.Kind() {
	case reflect.String:
		return &genai.Schema{Type: genai.TypeString}, nil
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return &genai.Schema{Type: genai.TypeInteger}, nil
	case reflect.Float32, reflect.Float64:
		return &genai.Schema{Type: genai.TypeNumber}, nil
	case reflect.Bool:
		return &genai.Schema{Type: genai.TypeBoolean}, nil
	case reflect.Slice, reflect.Array:
		itemSchema := &genai.Schema{Type: genai.TypeString}
		if value.Len() > 0 {
			var err error
			itemSchema, err = inferSchemaValue(value.Index(0))
			if err != nil {
				return nil, err
			}
		}
		return &genai.Schema{Type: genai.TypeArray, Items: itemSchema}, nil
	case reflect.Map:
		if value.Type().Key().Kind() != reflect.String {
			return nil, fmt.Errorf("schema maps must have string keys, got %s", value.Type().Key())
		}
		props := make(map[string]*genai.Schema, value.Len())
		iter := value.MapRange()
		for iter.Next() {
			propSchema, err := inferSchemaValue(iter.Value())
			if err != nil {
				return nil, err
			}
			props[iter.Key().String()] = propSchema
		}
		return &genai.Schema{Type: genai.TypeObject, Properties: props}, nil
	case reflect.Struct:
		props := make(map[string]*genai.Schema)
		for i := 0; i < value.NumField(); i++ {
			field := value.Type().Field(i)
			if field.PkgPath != "" { // unexported
				continue
			}
			name := field.Name
			if tag := field.Tag.Get("json"); tag != "" {
				tagName := strings.Split(tag, ",")[0]
				if tagName == "-" {
					continue
				}
				if tagName != "" {
					name = tagName
				}
			}
			propSchema, err := inferSchemaValue(value.Field(i))
			if err != nil {
				return nil, err
			}
			props[name] = propSchema
		}
		return &genai.Schema{Type: genai.TypeObject, Properties: props}, nil
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
