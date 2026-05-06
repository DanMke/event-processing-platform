package dlq

import (
	"context"
	"encoding/json"
	"testing"
)

type marshalSender struct {
	payloads [][]byte
}

func (s *marshalSender) Publish(_ context.Context, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	s.payloads = append(s.payloads, data)
	return nil
}

func TestPublisherPublish_MalformedJSONOriginalEvent(t *testing.T) {
	sender := &marshalSender{}
	publisher := NewPublisher(sender, "raw-events")

	err := publisher.Publish(context.Background(), []byte(`{not-valid-json}`), "unmarshal failed")
	if err != nil {
		t.Fatalf("expected nil, got: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(sender.payloads[0], &got); err != nil {
		t.Fatalf("unmarshal failed event: %v", err)
	}
	if got["original_event"] != "{not-valid-json}" {
		t.Fatalf("expected malformed event as string, got %#v", got["original_event"])
	}
}

func TestPublisherPublish_ValidJSONOriginalEvent(t *testing.T) {
	sender := &marshalSender{}
	publisher := NewPublisher(sender, "raw-events")

	err := publisher.Publish(context.Background(), []byte(`{"event_id":"evt-1"}`), "schema failed")
	if err != nil {
		t.Fatalf("expected nil, got: %v", err)
	}

	var got map[string]any
	if err := json.Unmarshal(sender.payloads[0], &got); err != nil {
		t.Fatalf("unmarshal failed event: %v", err)
	}
	original, ok := got["original_event"].(map[string]any)
	if !ok {
		t.Fatalf("expected valid event as object, got %#v", got["original_event"])
	}
	if original["event_id"] != "evt-1" {
		t.Fatalf("expected event_id evt-1, got %#v", original["event_id"])
	}
}
