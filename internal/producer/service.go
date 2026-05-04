package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/DanMke/event-processing-platform/internal/domain"
)

type Publisher interface {
	Publish(ctx context.Context, v any) error
}

type Service struct {
	publisher Publisher
}

func NewService(p Publisher) *Service {
	return &Service{publisher: p}
}

func (s *Service) PublishSampleEvents(ctx context.Context) error {
	events, err := buildSampleEvents()
	if err != nil {
		return fmt.Errorf("build events: %w", err)
	}

	for _, event := range events {
		if err := s.publisher.Publish(ctx, event); err != nil {
			return fmt.Errorf("publish %s: %w", event.EventType, err)
		}
		log.Printf("event published id=%s type=%s tenant=%s trace=%s",
			event.EventID, event.EventType, event.TenantID, event.TraceID)
	}
	return nil
}

func buildSampleEvents() ([]domain.Event, error) {
	createdPayload, err := json.Marshal(map[string]any{
		"contract_id": "contract-123",
		"amount":      1000,
		"currency":    "BRL",
	})
	if err != nil {
		return nil, err
	}

	cancelledPayload, err := json.Marshal(map[string]any{
		"contract_id": "contract-123",
		"reason":      "customer_request",
	})
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	return []domain.Event{
		{
			EventID:       "01HYZK8KJ3F9Z6K2X8YQ1W0ABC",
			TenantID:      "client-a",
			EventType:     "contract.created",
			SchemaVersion: "1.0",
			OccurredAt:    now,
			Producer:      "sample-producer",
			TraceID:       "trace-123",
			Payload:       json.RawMessage(createdPayload),
		},
		{
			EventID:       "01HYZK8KJ3F9Z6K2X8YQ1W0XYZ",
			TenantID:      "client-b",
			EventType:     "contract.cancelled",
			SchemaVersion: "1.0",
			OccurredAt:    now,
			Producer:      "sample-producer",
			TraceID:       "trace-456",
			Payload:       json.RawMessage(cancelledPayload),
		},
	}, nil
}
