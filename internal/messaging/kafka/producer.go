package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  topic,
			Balancer:               &kafka.Hash{},
			WriteTimeout:           10 * time.Second,
			ReadTimeout:            10 * time.Second,
			AllowAutoTopicCreation: false,
		},
	}
}

// Publish marshals v to JSON and sends it without a partition key.
// Used by DLQ, which does not require tenant-based routing.
func (p *Producer) Publish(ctx context.Context, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	return p.publishRaw(ctx, nil, data)
}

// PublishKeyed marshals v to JSON and sends it with the given partition key.
// Used by the producer service to route events by tenant_id.
func (p *Producer) PublishKeyed(ctx context.Context, key []byte, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	return p.publishRaw(ctx, key, data)
}

// PublishRaw sends pre-built bytes with a partition key, skipping JSON marshalling.
// Used by the load generator to send arbitrary payloads including invalid ones.
func (p *Producer) PublishRaw(ctx context.Context, key, value []byte) error {
	return p.publishRaw(ctx, key, value)
}

func (p *Producer) publishRaw(ctx context.Context, key, value []byte) error {
	if err := p.writer.WriteMessages(ctx, kafka.Message{Key: key, Value: value}); err != nil {
		return fmt.Errorf("write message: %w", err)
	}
	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
