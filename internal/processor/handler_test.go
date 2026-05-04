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
	h := processor.NewHandler(&mockRepo{}, &mockValidator{})

	err := h.Handle(context.Background(), nil, []byte(`not-valid-json`))

	if err == nil {
		t.Error("expected error for malformed JSON, got nil")
	}
}

func TestHandle_InvalidEnvelope(t *testing.T) {
	repo := &mockRepo{}
	h := processor.NewHandler(repo, &mockValidator{})

	// valid JSON but missing required envelope fields
	value := []byte(`{"event_id":"evt-001"}`)

	err := h.Handle(context.Background(), nil, value)

	if err != nil {
		t.Errorf("expected nil (envelope error handled internally), got: %v", err)
	}
	if len(repo.saved) != 0 {
		t.Errorf("expected no events saved, got %d", len(repo.saved))
	}
}

func TestHandle_InvalidPayload(t *testing.T) {
	repo := &mockRepo{}
	validator := &mockValidator{err: errors.New("schema mismatch")}
	h := processor.NewHandler(repo, validator)

	err := h.Handle(context.Background(), nil, validEventBytes(t))

	if err != nil {
		t.Errorf("expected nil (payload error handled internally), got: %v", err)
	}
	if len(repo.saved) != 0 {
		t.Errorf("expected no events saved, got %d", len(repo.saved))
	}
}

func TestHandle_DuplicateEvent(t *testing.T) {
	repo := &mockRepo{err: domain.ErrDuplicateEvent}
	h := processor.NewHandler(repo, &mockValidator{})

	err := h.Handle(context.Background(), nil, validEventBytes(t))

	if err != nil {
		t.Errorf("expected nil for duplicate event, got: %v", err)
	}
}

func TestHandle_SaveError(t *testing.T) {
	repo := &mockRepo{err: errors.New("connection refused")}
	h := processor.NewHandler(repo, &mockValidator{})

	err := h.Handle(context.Background(), nil, validEventBytes(t))

	if err == nil {
		t.Error("expected error for save failure, got nil")
	}
}

func TestHandle_Success(t *testing.T) {
	repo := &mockRepo{}
	h := processor.NewHandler(repo, &mockValidator{})

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
}
