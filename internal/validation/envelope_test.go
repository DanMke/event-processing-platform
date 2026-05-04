package validation_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/DanMke/event-processing-platform/internal/domain"
	"github.com/DanMke/event-processing-platform/internal/validation"
)

func TestValidateEnvelope(t *testing.T) {
	validPayload := json.RawMessage(`{"key":"value"}`)

	base := domain.Event{
		EventID:       "evt-001",
		TenantID:      "tenant-a",
		EventType:     "contract.created",
		SchemaVersion: "1.0",
		OccurredAt:    time.Now(),
		Producer:      "test-producer",
		Payload:       validPayload,
	}

	tests := []struct {
		name    string
		modify  func(domain.Event) domain.Event
		wantErr bool
		wantIn  string
	}{
		{
			name:    "valid event",
			modify:  func(e domain.Event) domain.Event { return e },
			wantErr: false,
		},
		{
			name:    "missing event_id",
			modify:  func(e domain.Event) domain.Event { e.EventID = ""; return e },
			wantErr: true,
			wantIn:  "event_id",
		},
		{
			name:    "missing tenant_id",
			modify:  func(e domain.Event) domain.Event { e.TenantID = ""; return e },
			wantErr: true,
			wantIn:  "tenant_id",
		},
		{
			name:    "missing event_type",
			modify:  func(e domain.Event) domain.Event { e.EventType = ""; return e },
			wantErr: true,
			wantIn:  "event_type",
		},
		{
			name:    "missing schema_version",
			modify:  func(e domain.Event) domain.Event { e.SchemaVersion = ""; return e },
			wantErr: true,
			wantIn:  "schema_version",
		},
		{
			name:    "missing producer",
			modify:  func(e domain.Event) domain.Event { e.Producer = ""; return e },
			wantErr: true,
			wantIn:  "producer",
		},
		{
			name:    "zero occurred_at",
			modify:  func(e domain.Event) domain.Event { e.OccurredAt = time.Time{}; return e },
			wantErr: true,
			wantIn:  "occurred_at",
		},
		{
			name:    "nil payload",
			modify:  func(e domain.Event) domain.Event { e.Payload = nil; return e },
			wantErr: true,
			wantIn:  "payload",
		},
		{
			name:    "empty object payload",
			modify:  func(e domain.Event) domain.Event { e.Payload = json.RawMessage(`{}`); return e },
			wantErr: true,
			wantIn:  "payload",
		},
		{
			name:    "null payload",
			modify:  func(e domain.Event) domain.Event { e.Payload = json.RawMessage(`null`); return e },
			wantErr: true,
			wantIn:  "payload",
		},
		{
			name: "multiple missing fields all reported",
			modify: func(e domain.Event) domain.Event {
				e.EventID = ""
				e.TenantID = ""
				e.Producer = ""
				return e
			},
			wantErr: true,
			wantIn:  "event_id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validation.ValidateEnvelope(tt.modify(base))

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !errors.Is(err, validation.ErrInvalidEnvelope) {
					t.Errorf("expected ErrInvalidEnvelope in chain, got: %v", err)
				}
				if tt.wantIn != "" && !strings.Contains(err.Error(), tt.wantIn) {
					t.Errorf("error %q should contain %q", err.Error(), tt.wantIn)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

func TestValidateEnvelope_MultipleFieldsMissingAllReported(t *testing.T) {
	event := domain.Event{EventID: "evt-001"} // all other fields missing

	err := validation.ValidateEnvelope(event)
	if err == nil {
		t.Fatal("expected error, got nil")
	}

	for _, field := range []string{"tenant_id", "event_type", "schema_version", "producer", "occurred_at", "payload"} {
		if !strings.Contains(err.Error(), field) {
			t.Errorf("expected %q in error message, got: %s", field, err.Error())
		}
	}
}
