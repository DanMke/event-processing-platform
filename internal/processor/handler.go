package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/DanMke/event-processing-platform/internal/domain"
)

type Handler struct{}

func NewHandler() *Handler {
	return &Handler{}
}

func (h *Handler) Handle(_ context.Context, key, value []byte) error {
	var event domain.Event
	if err := json.Unmarshal(value, &event); err != nil {
		return fmt.Errorf("unmarshal event: %w", err)
	}

	log.Printf("event processed key=%s type=%s tenant=%s trace=%s occurred_at=%s",
		key, event.EventType, event.TenantID, event.TraceID,
		event.OccurredAt.Format("2006-01-02T15:04:05Z07:00"),
	)
	return nil
}
