package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/DanMke/event-processing-platform/internal/domain"
	"github.com/DanMke/event-processing-platform/internal/messaging/kafka"
)

func main() {
	brokers := []string{env("KAFKA_BROKERS", "localhost:9092")}
	topic := env("KAFKA_TOPIC", "raw-events")

	producer := kafka.NewProducer(brokers, topic)
	defer producer.Close()

	event := domain.Event{
		EventID:       "01HYZK8KJ3F9Z6K2X8YQ1W0ABC",
		TenantID:      "client-a",
		EventType:     "contract.created",
		SchemaVersion: "1.0",
		OccurredAt:    time.Now().UTC(),
		Producer:      "sample-producer",
		TraceID:       "trace-123",
		Payload: map[string]any{
			"contract_id": "contract-123",
			"amount":      1000,
			"currency":    "BRL",
		},
	}

	ctx := context.Background()
	if err := producer.Publish(ctx, event); err != nil {
		log.Fatalf("publish error: %v", err)
	}

	out, _ := json.MarshalIndent(event, "", "  ")
	fmt.Printf("event published to topic=%s:\n%s\n", topic, out)
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
