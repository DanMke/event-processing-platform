package kafka

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type MessageHandler func(ctx context.Context, key, value []byte) error

type Consumer struct {
	reader *kafka.Reader
	topic  string
	group  string
}

func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        brokers,
			Topic:          topic,
			GroupID:        groupID,
			MinBytes:       1,
			MaxBytes:       10e6,
			CommitInterval: time.Second,
		}),
		topic: topic,
		group: groupID,
	}
}

func (c *Consumer) Run(ctx context.Context, handler MessageHandler) error {
	log.Printf("consumer loop started topic=%s group=%s", c.topic, c.group)
	defer log.Printf("consumer loop stopped topic=%s group=%s", c.topic, c.group)

	for {
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("read message: %w", err)
		}

		if err := handler(ctx, msg.Key, msg.Value); err != nil {
			log.Printf("handler error topic=%s partition=%d offset=%d: %v",
				msg.Topic, msg.Partition, msg.Offset, err)
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}
