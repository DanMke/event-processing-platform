package producer_test

import (
	"context"
	"errors"
	"testing"

	"github.com/DanMke/event-processing-platform/internal/producer"
)

type mockPublisher struct {
	calls []any
	err   error
}

func (m *mockPublisher) PublishKeyed(_ context.Context, _ []byte, v any) error {
	if m.err != nil {
		return m.err
	}
	m.calls = append(m.calls, v)
	return nil
}

func TestPublishSampleEvents_Success(t *testing.T) {
	pub := &mockPublisher{}
	svc := producer.NewService(pub)

	if err := svc.PublishSampleEvents(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pub.calls) != 5 {
		t.Errorf("expected 5 events published, got %d", len(pub.calls))
	}
}

func TestPublishSampleEvents_PublisherError(t *testing.T) {
	pub := &mockPublisher{err: errors.New("broker unavailable")}
	svc := producer.NewService(pub)

	err := svc.PublishSampleEvents(context.Background())

	if err == nil {
		t.Error("expected error when publisher fails, got nil")
	}
}
