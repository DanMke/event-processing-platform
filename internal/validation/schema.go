package validation

import (
	"bytes"
	"embed"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/santhosh-tekuri/jsonschema/v5"

	"github.com/DanMke/event-processing-platform/internal/domain"
)

//go:embed schemas/*.json
var schemaFiles embed.FS

var ErrUnknownSchema = errors.New("unknown schema")

var schemaRegistry = map[string]string{
	"contract.created/1.0":   "schemas/contract_created_v1.json",
	"contract.cancelled/1.0": "schemas/contract_cancelled_v1.json",
}

type SchemaValidator struct {
	schemas map[string]*jsonschema.Schema
}

func NewSchemaValidator() (*SchemaValidator, error) {
	compiler := jsonschema.NewCompiler()
	compiled := make(map[string]*jsonschema.Schema, len(schemaRegistry))

	for key, filename := range schemaRegistry {
		data, err := schemaFiles.ReadFile(filename)
		if err != nil {
			return nil, fmt.Errorf("read schema %s: %w", filename, err)
		}
		id := "https://schemas.local/" + key
		if err := compiler.AddResource(id, bytes.NewReader(data)); err != nil {
			return nil, fmt.Errorf("register schema %s: %w", key, err)
		}
		schema, err := compiler.Compile(id)
		if err != nil {
			return nil, fmt.Errorf("compile schema %s: %w", key, err)
		}
		compiled[key] = schema
	}

	return &SchemaValidator{schemas: compiled}, nil
}

func (v *SchemaValidator) ValidatePayload(event domain.Event) error {
	key := event.EventType + "/" + event.SchemaVersion
	schema, ok := v.schemas[key]
	if !ok {
		return fmt.Errorf("%w: event_type=%s schema_version=%s", ErrUnknownSchema, event.EventType, event.SchemaVersion)
	}

	var payload any
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("invalid payload json: %w", err)
	}

	if err := schema.Validate(payload); err != nil {
		return fmt.Errorf("schema validation: %w", err)
	}
	return nil
}
