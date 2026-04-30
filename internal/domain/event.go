package domain

import "time"

type Event struct {
	EventID       string         `json:"event_id"`
	TenantID      string         `json:"tenant_id"`
	EventType     string         `json:"event_type"`
	SchemaVersion string         `json:"schema_version"`
	OccurredAt    time.Time      `json:"occurred_at"`
	Producer      string         `json:"producer"`
	TraceID       string         `json:"trace_id"`
	Payload       map[string]any `json:"payload"`
}
