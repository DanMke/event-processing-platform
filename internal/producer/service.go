package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/DanMke/event-processing-platform/internal/domain"
)

type Publisher interface {
	PublishKeyed(ctx context.Context, key []byte, v any) error
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
		if err := s.publisher.PublishKeyed(ctx, []byte(event.TenantID), event); err != nil {
			return fmt.Errorf("publish %s: %w", event.EventType, err)
		}
		slog.Info("event published",
			"event_id", event.EventID,
			"event_type", event.EventType,
			"tenant_id", event.TenantID,
			"trace_id", event.TraceID,
		)
	}
	return nil
}

func buildSampleEvents() ([]domain.Event, error) {
	now := time.Now().UTC()

	specs := []struct {
		id, tenant, eventType, traceID string
		payload                        map[string]any
	}{
		{
			id: "01HYZK8K-CONTRACT-CREATED-T01", tenant: "tenant-01",
			eventType: "contract.created", traceID: "trace-001",
			payload: map[string]any{"contract_id": "contract-001", "amount": 1500.00, "currency": "BRL"},
		},
		{
			id: "01HYZK8K-CONTRACT-CANCELLED-T02", tenant: "tenant-02",
			eventType: "contract.cancelled", traceID: "trace-002",
			payload: map[string]any{"contract_id": "contract-002", "reason": "customer_request"},
		},
		{
			id: "01HYZK8K-PAYMENT-PROCESSED-T01", tenant: "tenant-01",
			eventType: "payment.processed", traceID: "trace-003",
			payload: map[string]any{
				"transaction_id": "txn-001", "amount": 250.00,
				"currency": "BRL", "merchant_id": "merchant-abc", "card_last_four": "4242",
			},
		},
		{
			id: "01HYZK8K-PAYMENT-FAILED-T03", tenant: "tenant-03",
			eventType: "payment.failed", traceID: "trace-004",
			payload: map[string]any{
				"transaction_id": "txn-002", "amount": 800.00,
				"currency": "BRL", "merchant_id": "merchant-xyz", "failure_reason": "insufficient_funds",
			},
		},
		{
			id: "01HYZK8K-ACCOUNT-CREATED-T04", tenant: "tenant-04",
			eventType: "account.created", traceID: "trace-005",
			payload: map[string]any{
				"account_id": "acc-001", "account_type": "credit",
				"owner_id": "owner-123", "credit_limit": 5000.00,
			},
		},
	}

	events := make([]domain.Event, 0, len(specs))
	for _, s := range specs {
		p, err := json.Marshal(s.payload)
		if err != nil {
			return nil, fmt.Errorf("marshal payload for %s: %w", s.eventType, err)
		}
		events = append(events, domain.Event{
			EventID:       s.id,
			TenantID:      s.tenant,
			EventType:     s.eventType,
			SchemaVersion: "1.0",
			OccurredAt:    now,
			Producer:      "sample-producer",
			TraceID:       s.traceID,
			Payload:       json.RawMessage(p),
		})
	}
	return events, nil
}
