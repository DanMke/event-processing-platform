package processor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/DanMke/event-processing-platform/internal/domain"
	"github.com/DanMke/event-processing-platform/internal/validation"
)

type Repository interface {
	Save(ctx context.Context, event domain.Event) error
}

type Validator interface {
	ValidatePayload(event domain.Event) error
}

type Handler struct {
	repo      Repository
	validator Validator
}

func NewHandler(repo Repository, validator Validator) *Handler {
	return &Handler{repo: repo, validator: validator}
}

func (h *Handler) Handle(ctx context.Context, _ []byte, value []byte) error {
	var event domain.Event
	if err := json.Unmarshal(value, &event); err != nil {
		return fmt.Errorf("unmarshal event: %w", err)
	}

	log.Printf("event received id=%s type=%s tenant=%s schema=%s producer=%s trace=%s",
		event.EventID, event.EventType, event.TenantID,
		event.SchemaVersion, event.Producer, event.TraceID,
	)

	if err := validation.ValidateEnvelope(event); err != nil {
		log.Printf("invalid envelope id=%s type=%s tenant=%s: %v",
			event.EventID, event.EventType, event.TenantID, err)
		return nil
	}

	if err := h.validator.ValidatePayload(event); err != nil {
		log.Printf("invalid payload id=%s type=%s tenant=%s: %v",
			event.EventID, event.EventType, event.TenantID, err)
		return nil
	}

	if err := h.repo.Save(ctx, event); err != nil {
		if errors.Is(err, domain.ErrDuplicateEvent) {
			log.Printf("duplicate event id=%s type=%s tenant=%s",
				event.EventID, event.EventType, event.TenantID)
			return nil
		}
		return fmt.Errorf("save event: %w", err)
	}

	log.Printf("event persisted id=%s type=%s tenant=%s", event.EventID, event.EventType, event.TenantID)
	return nil
}
