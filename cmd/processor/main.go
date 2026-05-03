package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/DanMke/event-processing-platform/internal/config"
	"github.com/DanMke/event-processing-platform/internal/messaging/kafka"
	"github.com/DanMke/event-processing-platform/internal/processor"
)

func main() {
	cfg := config.LoadProcessor()

	consumer := kafka.NewConsumer(cfg.Brokers, cfg.Topic, cfg.GroupID)
	defer consumer.Close()

	handler := processor.NewHandler()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	log.Printf("processor started brokers=%v topic=%s group=%s", cfg.Brokers, cfg.Topic, cfg.GroupID)

	if err := consumer.Run(ctx, handler.Handle); err != nil {
		log.Fatalf("consumer exited with error: %v", err)
	}

	log.Println("processor stopped gracefully")
}
