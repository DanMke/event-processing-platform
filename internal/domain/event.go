package domain

import (
	"encoding/json"
	"time"
)

type Event struct {
	EventID       string          `json:"event_id"`
	TenantID      string          `json:"tenant_id"`
	EventType     string          `json:"event_type"`
	SchemaVersion string          `json:"schema_version"`
	OccurredAt    time.Time       `json:"occurred_at"`
	Producer      string          `json:"producer"`
	TraceID       string          `json:"trace_id"`
	Payload       json.RawMessage `json:"payload"`
}
