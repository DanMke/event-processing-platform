package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DanMke/event-processing-platform/internal/config"
	"github.com/DanMke/event-processing-platform/internal/dlq"
	"github.com/DanMke/event-processing-platform/internal/messaging/kafka"
	"github.com/DanMke/event-processing-platform/internal/processor"
	pg "github.com/DanMke/event-processing-platform/internal/repository/postgres"
	"github.com/DanMke/event-processing-platform/internal/validation"
)

func main() {
	kafkaCfg := config.LoadProcessor()
	pgCfg := config.LoadPostgres()
	dlqCfg := config.LoadDLQ()

	pool, err := pgxpool.New(context.Background(), pgCfg.DSN)
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}
	defer pool.Close()

	validator, err := validation.NewSchemaValidator()
	if err != nil {
		log.Fatalf("init schema validator: %v", err)
	}

	dlqProducer := kafka.NewProducer(dlqCfg.Brokers, dlqCfg.Topic)
	defer dlqProducer.Close()

	dlqPublisher := dlq.NewPublisher(dlqProducer, kafkaCfg.Topic)

	repo := pg.NewEventRepository(pool)
	handler := processor.NewHandler(repo, validator, dlqPublisher)

	consumer := kafka.NewConsumer(kafkaCfg.Brokers, kafkaCfg.Topic, kafkaCfg.GroupID)
	defer consumer.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	log.Printf("processor started brokers=%v topic=%s group=%s dlq_topic=%s",
		kafkaCfg.Brokers, kafkaCfg.Topic, kafkaCfg.GroupID, dlqCfg.Topic)

	if err := consumer.Run(ctx, handler.Handle); err != nil {
		log.Fatalf("consumer exited with error: %v", err)
	}

	log.Println("processor stopped gracefully")
}
