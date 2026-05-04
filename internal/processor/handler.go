package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/DanMke/event-processing-platform/internal/domain"
)

type Repository interface {
	Save(ctx context.Context, event domain.Event) error
}

type Handler struct {
	repo Repository
}

func NewHandler(repo Repository) *Handler {
	return &Handler{repo: repo}
}

func (h *Handler) Handle(ctx context.Context, _ []byte, value []byte) error {
	var event domain.Event
	// TODO: review errors to invalid events
	if err := json.Unmarshal(value, &event); err != nil {
		return fmt.Errorf("unmarshal event: %w", err)
	}

	log.Printf("event received id=%s type=%s tenant=%s schema=%s producer=%s trace=%s",
		event.EventID, event.EventType, event.TenantID,
		event.SchemaVersion, event.Producer, event.TraceID,
	)

	if err := h.repo.Save(ctx, event); err != nil {
		return fmt.Errorf("save event: %w", err)
	}

	log.Printf("event persisted id=%s type=%s tenant=%s", event.EventID, event.EventType, event.TenantID)
	return nil
}
