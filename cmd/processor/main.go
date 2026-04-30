package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/DanMke/event-processing-platform/internal/domain"
	"github.com/DanMke/event-processing-platform/internal/messaging/kafka"
)

func main() {
	brokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")
	topic := getEnv("KAFKA_TOPIC", "raw-events")
	groupID := getEnv("KAFKA_GROUP_ID", "event-processor")

	consumer := kafka.NewConsumer(brokers, topic, groupID)
	defer consumer.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	log.Printf("processor started — topic=%s group=%s", topic, groupID)

	if err := consumer.Run(ctx, handle); err != nil {
		log.Fatalf("consumer error: %v", err)
	}

	log.Println("processor stopped")
}

func handle(_ context.Context, key, value []byte) error {
	var event domain.Event
	if err := json.Unmarshal(value, &event); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	out, _ := json.MarshalIndent(event, "", "  ")
	log.Printf("received event [key=%s]:\n%s\n", key, out)
	return nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
