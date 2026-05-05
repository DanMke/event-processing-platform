package kafka

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/segmentio/kafka-go"
)

type MessageHandler func(ctx context.Context, key, value []byte) error

type reader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Close() error
}

type Consumer struct {
	reader reader
	topic  string
	group  string
}

func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  brokers,
			Topic:    topic,
			GroupID:  groupID,
			MinBytes: 1,
			MaxBytes: 10e6,
		}),
		topic: topic,
		group: groupID,
	}
}

func (c *Consumer) Run(ctx context.Context, handler MessageHandler) error {
	slog.Info("consumer loop started", "topic", c.topic, "group", c.group)
	defer slog.Info("consumer loop stopped", "topic", c.topic, "group", c.group)

	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("fetch message: %w", err)
		}

		if err := handler(ctx, msg.Key, msg.Value); err != nil {
			slog.Warn("handler error — offset not committed, will reprocess",
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
				"error_reason", err.Error(),
			)
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			slog.Error("offset commit failed",
				"topic", msg.Topic,
				"partition", msg.Partition,
				"offset", msg.Offset,
				"error_reason", err.Error(),
			)
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
