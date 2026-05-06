package main

import (
	"context"
	"log/slog"
	"net/http"
	"os/signal"
	"syscall"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DanMke/event-processing-platform/internal/config"
	"github.com/DanMke/event-processing-platform/internal/observability"
	"github.com/DanMke/event-processing-platform/internal/sender"
)

func main() {
	observability.Init()

	pgCfg := config.LoadPostgres()
	senderCfg := config.LoadSender()

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
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			if err := pool.Ping(r.Context()); err != nil {
				http.Error(w, `{"status":"unhealthy"}`, http.StatusServiceUnavailable)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"status":"ok"}`))
		})
		addr := ":" + senderCfg.HealthPort
		slog.Info("sender health server listening", "addr", addr)
		if err := http.ListenAndServe(addr, mux); err != nil {
			slog.Error("health server stopped", "error_reason", err.Error())
		}
	}()

	poller := sender.NewPoller(pool, sender.LogDispatcher{}, senderCfg.PollInterval, senderCfg.BatchSize)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	slog.Info("sender started",
		"poll_interval_ms", senderCfg.PollInterval.Milliseconds(),
		"batch_size", senderCfg.BatchSize,
		"pg_max_conns", pgCfg.MaxConns,
		"health_port", senderCfg.HealthPort,
	)

	if err := poller.Run(ctx); err != nil && err != context.Canceled {
		slog.Error("sender exited with error", "error_reason", err.Error())
		panic(err)
	}

	slog.Info("sender stopped gracefully")
}
