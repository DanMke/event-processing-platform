package processor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"github.com/DanMke/event-processing-platform/internal/domain"
	"github.com/DanMke/event-processing-platform/internal/retry"
	"github.com/DanMke/event-processing-platform/internal/validation"
)

type Repository interface {
	Save(ctx context.Context, event domain.Event) error
}

type Validator interface {
	ValidatePayload(event domain.Event) error
}

type DLQPublisher interface {
	Publish(ctx context.Context, original []byte, reason string) error
}

type Handler struct {
	repo      Repository
	validator Validator
	dlq       DLQPublisher
}

func NewHandler(repo Repository, validator Validator, dlq DLQPublisher) *Handler {
	return &Handler{repo: repo, validator: validator, dlq: dlq}
}

func (h *Handler) Handle(ctx context.Context, _ []byte, value []byte) error {
	var event domain.Event
	if err := json.Unmarshal(value, &event); err != nil {
		log.Printf("unmarshal error, sending to dlq: %v", err)
		h.sendToDLQ(ctx, value, fmt.Sprintf("unmarshal: %v", err))
		return nil
	}

	log.Printf("event received id=%s type=%s tenant=%s schema=%s producer=%s trace=%s",
		event.EventID, event.EventType, event.TenantID,
		event.SchemaVersion, event.Producer, event.TraceID,
	)

	if err := validation.ValidateEnvelope(event); err != nil {
		log.Printf("invalid envelope id=%s type=%s tenant=%s: %v",
			event.EventID, event.EventType, event.TenantID, err)
		h.sendToDLQ(ctx, value, err.Error())
		return nil
	}

	if err := h.validator.ValidatePayload(event); err != nil {
		log.Printf("invalid payload id=%s type=%s tenant=%s: %v",
			event.EventID, event.EventType, event.TenantID, err)
		h.sendToDLQ(ctx, value, err.Error())
		return nil
	}

	var isDuplicate bool
	saveErr := retry.Do(ctx, func() error {
		err := h.repo.Save(ctx, event)
		if errors.Is(err, domain.ErrDuplicateEvent) {
			isDuplicate = true
			return nil // stop retrying
		}
		return err
	})

	if isDuplicate {
		log.Printf("duplicate event id=%s type=%s tenant=%s",
			event.EventID, event.EventType, event.TenantID)
		return nil
	}

	if saveErr != nil {
		return fmt.Errorf("save event: %w", saveErr)
	}

	log.Printf("event persisted id=%s type=%s tenant=%s", event.EventID, event.EventType, event.TenantID)
	return nil
}

func (h *Handler) sendToDLQ(ctx context.Context, original []byte, reason string) {
	if err := h.dlq.Publish(ctx, original, reason); err != nil {
		log.Printf("dlq publish failed: %v", err)
	}
}
