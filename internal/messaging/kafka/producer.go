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

// Publish sends v as JSON without a partition key.
func (p *Producer) Publish(ctx context.Context, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	return p.publishRaw(ctx, nil, data)
}

// PublishKeyed sends v as JSON using key for partitioning.
func (p *Producer) PublishKeyed(ctx context.Context, key []byte, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	return p.publishRaw(ctx, key, data)
}

// PublishRaw sends pre-encoded bytes with the provided key.
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
