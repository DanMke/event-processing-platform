package producer

import (
	"context"
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

func (s *Service) PublishSample(ctx context.Context) error {
	event := domain.Event{
		EventID:       "01HYZK8KJ3F9Z6K2X8YQ1W0ABC",
		TenantID:      "client-a",
		EventType:     "contract.created",
		SchemaVersion: "1.0",
		OccurredAt:    time.Now().UTC(),
		Producer:      "sample-producer",
		TraceID:       "trace-123",
		Payload: map[string]any{
			"contract_id": "contract-123",
			"amount":      1000,
			"currency":    "BRL",
		},
	}

	if err := s.publisher.Publish(ctx, event); err != nil {
		return fmt.Errorf("publish event: %w", err)
	}

	log.Printf("event published type=%s tenant=%s trace=%s",
		event.EventType, event.TenantID, event.TraceID)
	return nil
}
