package dlq

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"
)

type Sender interface {
	Publish(ctx context.Context, v any) error
}

type FailedEvent struct {
	OriginalEvent json.RawMessage `json:"original_event"`
	ErrorReason   string          `json:"error_reason"`
	FailedAt      time.Time       `json:"failed_at"`
	SourceTopic   string          `json:"source_topic"`
}

type Publisher struct {
	sender      Sender
	sourceTopic string
}

func NewPublisher(sender Sender, sourceTopic string) *Publisher {
	return &Publisher{sender: sender, sourceTopic: sourceTopic}
}

func (p *Publisher) Publish(ctx context.Context, original []byte, reason string) error {
	failed := FailedEvent{
		OriginalEvent: json.RawMessage(original),
		ErrorReason:   reason,
		FailedAt:      time.Now().UTC(),
		SourceTopic:   p.sourceTopic,
	}
	if err := p.sender.Publish(ctx, failed); err != nil {
		return fmt.Errorf("publish to dlq: %w", err)
	}
	slog.Info("event sent to DLQ",
		"source_topic", p.sourceTopic,
		"error_reason", reason,
	)
	return nil
}
