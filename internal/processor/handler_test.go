package processor_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/DanMke/event-processing-platform/internal/domain"
	"github.com/DanMke/event-processing-platform/internal/processor"
)

// --- mocks ---

type mockRepo struct {
	err   error
	saved []domain.Event
}

func (m *mockRepo) Save(_ context.Context, e domain.Event) error {
	if m.err != nil {
		return m.err
	}
	m.saved = append(m.saved, e)
	return nil
}

type mockValidator struct {
	err error
}

func (m *mockValidator) ValidatePayload(_ domain.Event) error {
	return m.err
}

type mockDLQ struct {
	calls []string
	err   error
}

func (m *mockDLQ) Publish(_ context.Context, _ []byte, reason string) error {
	if m.err != nil {
		return m.err
	}
	m.calls = append(m.calls, reason)
	return nil
}

// --- helpers ---

func validEventBytes(t *testing.T) []byte {
	t.Helper()
	payload, _ := json.Marshal(map[string]any{
		"contract_id": "c-123",
		"amount":      1000,
		"currency":    "BRL",
	})
	event := domain.Event{
		EventID:       "evt-001",
		TenantID:      "tenant-a",
		EventType:     "contract.created",
		SchemaVersion: "1.0",
		OccurredAt:    time.Now(),
		Producer:      "test-producer",
		TraceID:       "trace-001",
		Payload:       json.RawMessage(payload),
	}
	data, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// --- tests ---

func TestHandle_InvalidJSON(t *testing.T) {
	dlq := &mockDLQ{}
	h := processor.NewHandler(&mockRepo{}, &mockValidator{}, dlq)

	err := h.Handle(context.Background(), nil, []byte(`not-valid-json`))

	if err != nil {
		t.Errorf("expected nil (unmarshal error goes to dlq), got: %v", err)
	}
	if len(dlq.calls) != 1 {
		t.Errorf("expected 1 dlq call, got %d", len(dlq.calls))
	}
}

func TestHandle_InvalidEnvelope(t *testing.T) {
	repo := &mockRepo{}
	dlq := &mockDLQ{}
	h := processor.NewHandler(repo, &mockValidator{}, dlq)

	value := []byte(`{"event_id":"evt-001"}`)

	err := h.Handle(context.Background(), nil, value)

	if err != nil {
		t.Errorf("expected nil (envelope error goes to dlq), got: %v", err)
	}
	if len(repo.saved) != 0 {
		t.Errorf("expected no events saved, got %d", len(repo.saved))
	}
	if len(dlq.calls) != 1 {
		t.Errorf("expected 1 dlq call, got %d", len(dlq.calls))
	}
}

func TestHandle_InvalidPayload(t *testing.T) {
	repo := &mockRepo{}
	dlq := &mockDLQ{}
	validator := &mockValidator{err: errors.New("schema mismatch")}
	h := processor.NewHandler(repo, validator, dlq)

	err := h.Handle(context.Background(), nil, validEventBytes(t))

	if err != nil {
		t.Errorf("expected nil (payload error goes to dlq), got: %v", err)
	}
	if len(repo.saved) != 0 {
		t.Errorf("expected no events saved, got %d", len(repo.saved))
	}
	if len(dlq.calls) != 1 {
		t.Errorf("expected 1 dlq call, got %d", len(dlq.calls))
	}
}

func TestHandle_DuplicateEvent(t *testing.T) {
	dlq := &mockDLQ{}
	repo := &mockRepo{err: domain.ErrDuplicateEvent}
	h := processor.NewHandler(repo, &mockValidator{}, dlq)

	err := h.Handle(context.Background(), nil, validEventBytes(t))

	if err != nil {
		t.Errorf("expected nil for duplicate event, got: %v", err)
	}
	if len(dlq.calls) != 0 {
		t.Errorf("expected no dlq calls for duplicate, got %d", len(dlq.calls))
	}
}

func TestHandle_SaveError_ReturnsErrorWithoutDLQ(t *testing.T) {
	dlq := &mockDLQ{}
	repo := &mockRepo{err: errors.New("connection refused")}
	h := processor.NewHandler(repo, &mockValidator{}, dlq)

	// short timeout to cut through retry delays quickly
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := h.Handle(ctx, nil, validEventBytes(t))

	if err == nil {
		t.Error("expected error for transient save failure, got nil")
	}
	if len(dlq.calls) != 0 {
		t.Errorf("expected no dlq calls for transient error, got %d", len(dlq.calls))
	}
}

func TestHandle_Success(t *testing.T) {
	repo := &mockRepo{}
	dlq := &mockDLQ{}
	h := processor.NewHandler(repo, &mockValidator{}, dlq)

	err := h.Handle(context.Background(), nil, validEventBytes(t))

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(repo.saved) != 1 {
		t.Fatalf("expected 1 event saved, got %d", len(repo.saved))
	}
	if repo.saved[0].EventID != "evt-001" {
		t.Errorf("expected EventID evt-001, got %s", repo.saved[0].EventID)
	}
	if len(dlq.calls) != 0 {
		t.Errorf("expected no dlq calls for success, got %d", len(dlq.calls))
	}
}
