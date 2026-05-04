package validation

import (
	"errors"
	"fmt"
	"strings"

	"github.com/DanMke/event-processing-platform/internal/domain"
)

var ErrInvalidEnvelope = errors.New("invalid envelope")

func ValidateEnvelope(event domain.Event) error {
	var missing []string

	if event.EventID == "" {
		missing = append(missing, "event_id")
	}
	if event.TenantID == "" {
		missing = append(missing, "tenant_id")
	}
	if event.EventType == "" {
		missing = append(missing, "event_type")
	}
	if event.SchemaVersion == "" {
		missing = append(missing, "schema_version")
	}
	if event.Producer == "" {
		missing = append(missing, "producer")
	}
	if event.OccurredAt.IsZero() {
		missing = append(missing, "occurred_at")
	}
	if isEmptyPayload(event.Payload) {
		missing = append(missing, "payload")
	}

	if len(missing) > 0 {
		return fmt.Errorf("%w: missing=%s", ErrInvalidEnvelope, strings.Join(missing, ","))
	}
	return nil
}

func isEmptyPayload(p []byte) bool {
	if len(p) == 0 {
		return true
	}
	t := strings.TrimSpace(string(p))
	return t == "null" || t == "{}"
}
