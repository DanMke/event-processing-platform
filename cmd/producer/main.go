package main

import (
	"context"
	"log"

	"github.com/DanMke/event-processing-platform/internal/config"
	"github.com/DanMke/event-processing-platform/internal/messaging/kafka"
	"github.com/DanMke/event-processing-platform/internal/producer"
)

func main() {
	cfg := config.LoadProducer()

	p := kafka.NewProducer(cfg.Brokers, cfg.Topic)
	defer p.Close()

	svc := producer.NewService(p)

	log.Printf("producer started brokers=%v topic=%s", cfg.Brokers, cfg.Topic)

	if err := svc.PublishSample(context.Background()); err != nil {
		log.Fatalf("publish failed: %v", err)
	}

	log.Println("producer finished")
}
