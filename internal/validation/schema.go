package validation

import (
	"bytes"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v5"

	"github.com/DanMke/event-processing-platform/internal/domain"
)

//go:embed schemas/*.json
var schemaFiles embed.FS

var ErrUnknownSchema = errors.New("unknown schema")

type SchemaValidator struct {
	schemas map[string]*jsonschema.Schema
}

func NewSchemaValidator() (*SchemaValidator, error) {
	schemas, err := loadSchemas(schemaFiles)
	if err != nil {
		return nil, err
	}
	return &SchemaValidator{schemas: schemas}, nil
}

func (v *SchemaValidator) ValidatePayload(event domain.Event) error {
	key := schemaKey(event.EventType, event.SchemaVersion)

	schema, ok := v.schemas[key]
	if !ok {
		return fmt.Errorf("%w: event_type=%s schema_version=%s",
			ErrUnknownSchema, event.EventType, event.SchemaVersion)
	}

	var payload any
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		return fmt.Errorf("invalid payload json event_type=%s schema_version=%s: %w",
			event.EventType, event.SchemaVersion, err)
	}

	if err := schema.Validate(payload); err != nil {
		return fmt.Errorf("schema validation failed event_type=%s schema_version=%s: %w",
			event.EventType, event.SchemaVersion, err)
	}
	return nil
}

func schemaKey(eventType, schemaVersion string) string {
	return eventType + "/" + schemaVersion
}

// loadSchemas reads all *.json files from the schemas/ directory and compiles them.
// Naming convention: {event_type}_{schema_version}.json
// The last underscore separates event_type from schema_version.
// Example: contract.created_1.0.json → key "contract.created/1.0"
func loadSchemas(fs embed.FS) (map[string]*jsonschema.Schema, error) {
	entries, err := fs.ReadDir("schemas")
	if err != nil {
		return nil, fmt.Errorf("read schemas dir: %w", err)
	}

	compiler := jsonschema.NewCompiler()
	compiled := make(map[string]*jsonschema.Schema, len(entries))

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		key, err := keyFromFilename(entry.Name())
		if err != nil {
			return nil, err
		}

		data, err := fs.ReadFile("schemas/" + entry.Name())
		if err != nil {
			return nil, fmt.Errorf("read schema %s: %w", entry.Name(), err)
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

	return compiled, nil
}

// keyFromFilename converts a schema filename to its lookup key.
// It splits on the last underscore so event types with dots are handled correctly.
func keyFromFilename(filename string) (string, error) {
	name := strings.TrimSuffix(filename, ".json")
	idx := strings.LastIndex(name, "_")
	if idx < 0 {
		return "", fmt.Errorf("schema %q does not follow naming convention {event_type}_{schema_version}.json", filename)
	}
	return name[:idx] + "/" + name[idx+1:], nil
}
