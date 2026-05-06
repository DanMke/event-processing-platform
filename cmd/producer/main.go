package main

import (
	"context"
	"log/slog"

	"github.com/DanMke/event-processing-platform/internal/config"
	"github.com/DanMke/event-processing-platform/internal/messaging/kafka"
	"github.com/DanMke/event-processing-platform/internal/observability"
	"github.com/DanMke/event-processing-platform/internal/producer"
)

func main() {
	observability.Init()

	cfg := config.LoadProducer()

	p := kafka.NewProducer(cfg.Brokers, cfg.Topic)
	defer p.Close()

	svc := producer.NewService(p)

	slog.Info("producer started", "brokers", cfg.Brokers, "topic", cfg.Topic)

	if err := svc.PublishSampleEvents(context.Background()); err != nil {
		slog.Error("publish failed", "error_reason", err.Error())
		panic(err)
	}

	slog.Info("producer finished")
}
