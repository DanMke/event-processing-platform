package validation_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/DanMke/event-processing-platform/internal/domain"
	"github.com/DanMke/event-processing-platform/internal/validation"
)

func TestValidatePayload(t *testing.T) {
	v, err := validation.NewSchemaValidator()
	if err != nil {
		t.Fatalf("NewSchemaValidator: %v", err)
	}

	t.Run("contract.created valid payload", func(t *testing.T) {
		event := newEvent("contract.created", "1.0", map[string]any{
			"contract_id": "c-123",
			"amount":      1000,
			"currency":    "BRL",
		})
		if err := v.ValidatePayload(event); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("contract.created missing required fields", func(t *testing.T) {
		event := newEvent("contract.created", "1.0", map[string]any{
			"contract_id": "c-123",
		})
		if err := v.ValidatePayload(event); err == nil {
			t.Error("expected validation error, got nil")
		}
	})

	t.Run("contract.created wrong field type", func(t *testing.T) {
		event := newEvent("contract.created", "1.0", map[string]any{
			"contract_id": "c-123",
			"amount":      "not-a-number",
			"currency":    "BRL",
		})
		if err := v.ValidatePayload(event); err == nil {
			t.Error("expected validation error, got nil")
		}
	})

	t.Run("contract.cancelled valid payload", func(t *testing.T) {
		event := newEvent("contract.cancelled", "1.0", map[string]any{
			"contract_id": "c-123",
			"reason":      "customer_request",
		})
		if err := v.ValidatePayload(event); err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	})

	t.Run("unknown schema", func(t *testing.T) {
		event := newEvent("unknown.event", "1.0", map[string]any{"foo": "bar"})
		err := v.ValidatePayload(event)
		if !errors.Is(err, validation.ErrUnknownSchema) {
			t.Errorf("expected ErrUnknownSchema, got: %v", err)
		}
	})

	t.Run("invalid JSON payload", func(t *testing.T) {
		event := domain.Event{
			EventType:     "contract.created",
			SchemaVersion: "1.0",
			Payload:       json.RawMessage(`not-valid-json`),
		}
		if err := v.ValidatePayload(event); err == nil {
			t.Error("expected error for invalid JSON, got nil")
		}
	})
}

func newEvent(eventType, schemaVersion string, payload any) domain.Event {
	p, _ := json.Marshal(payload)
	return domain.Event{
		EventID:       "test-id",
		TenantID:      "test-tenant",
		EventType:     eventType,
		SchemaVersion: schemaVersion,
		Payload:       json.RawMessage(p),
	}
}
