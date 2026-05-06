package main

import (
	"context"
	"log/slog"
	"net/http"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/DanMke/event-processing-platform/internal/config"
	"github.com/DanMke/event-processing-platform/internal/dlq"
	"github.com/DanMke/event-processing-platform/internal/messaging/kafka"
	"github.com/DanMke/event-processing-platform/internal/observability"
	obsmetrics "github.com/DanMke/event-processing-platform/internal/observability/metrics"
	"github.com/DanMke/event-processing-platform/internal/processor"
	pg "github.com/DanMke/event-processing-platform/internal/repository/postgres"
	"github.com/DanMke/event-processing-platform/internal/validation"
)

func main() {
	observability.Init()

	kafkaCfg := config.LoadProcessor()
	pgCfg := config.LoadPostgres()
	dlqCfg := config.LoadDLQ()
	obsCfg := config.LoadObservability()

	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	m := obsmetrics.New(reg)

	poolCfg, err := pgxpool.ParseConfig(pgCfg.DSN)
	if err != nil {
		slog.Error("parse postgres dsn failed", "error_reason", err.Error())
		panic(err)
	}
	poolCfg.MaxConns = pgCfg.MaxConns
	pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err != nil {
		slog.Error("connect postgres failed", "error_reason", err.Error())
		panic(err)
	}
	defer pool.Close()

	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			if err := pool.Ping(r.Context()); err != nil {
				http.Error(w, `{"status":"unhealthy"}`, http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"status":"ok"}`))
		})
		addr := ":" + obsCfg.MetricsPort
		slog.Info("metrics server listening", "addr", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			slog.Error("metrics server stopped", "error_reason", err.Error())
		}
	}()

	validator, err := validation.NewSchemaValidator()
	if err != nil {
		slog.Error("init schema validator failed", "error_reason", err.Error())
		panic(err)
	}

	dlqProducer := kafka.NewProducer(dlqCfg.Brokers, dlqCfg.Topic)
	defer dlqProducer.Close()
	dlqPublisher := dlq.NewPublisher(dlqProducer, kafkaCfg.Topic)

	repo := pg.NewEventRepository(pool)
	handler := processor.NewHandler(repo, validator, dlqPublisher, processor.WithMetrics(m))

	consumer := kafka.NewConsumer(kafkaCfg.Brokers, kafkaCfg.Topic, kafkaCfg.GroupID,
		kafka.WithWorkers(kafkaCfg.Workers))
	defer consumer.Close()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	slog.Info("processor started",
		"brokers", kafkaCfg.Brokers,
		"topic", kafkaCfg.Topic,
		"group", kafkaCfg.GroupID,
		"workers", kafkaCfg.Workers,
		"dlq_topic", dlqCfg.Topic,
		"metrics_port", obsCfg.MetricsPort,
		"pg_max_conns", pgCfg.MaxConns,
	)

	if err := consumer.Run(ctx, handler.Handle); err != nil {
		slog.Error("consumer exited with error", "error_reason", err.Error())
		panic(err)
	}

	slog.Info("processor stopped gracefully")
}
