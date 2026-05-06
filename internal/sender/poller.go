// Package sender claims pending outbox rows and hands them to a Dispatcher.
package sender

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Job struct {
	OutboxID int64
	EventID  string
	TenantID string
	TargetID string
	Kind     string
	Config   json.RawMessage
	Payload  json.RawMessage
	Attempts int
}

type Dispatcher interface {
	Dispatch(ctx context.Context, job Job) error
}

// LogDispatcher logs jobs without delivering. Replaced by per-kind adapters.
type LogDispatcher struct{}

func (LogDispatcher) Dispatch(_ context.Context, j Job) error {
	slog.Info("would deliver",
		"kind", j.Kind,
		"tenant_id", j.TenantID,
		"event_id", j.EventID,
		"target_id", j.TargetID,
		"outbox_id", j.OutboxID,
		"attempts", j.Attempts,
	)
	return nil
}

type Poller struct {
	pool       *pgxpool.Pool
	dispatcher Dispatcher
	interval   time.Duration
	batchSize  int
}

func NewPoller(pool *pgxpool.Pool, dispatcher Dispatcher, interval time.Duration, batchSize int) *Poller {
	return &Poller{
		pool:       pool,
		dispatcher: dispatcher,
		interval:   interval,
		batchSize:  batchSize,
	}
}

// Run loops until ctx is cancelled. Tick errors are logged, not fatal.
func (p *Poller) Run(ctx context.Context) error {
	slog.Info("sender loop started",
		"interval_ms", p.interval.Milliseconds(),
		"batch_size", p.batchSize,
	)
	defer slog.Info("sender loop stopped")

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			n, err := p.Tick(ctx)
			if err != nil && !errors.Is(err, context.Canceled) {
				slog.Error("poll tick failed", "error_reason", err.Error())
				continue
			}
			if n > 0 {
				slog.Info("batch processed", "count", n)
			}
		}
	}
}

// Tick claims and dispatches one batch in a single transaction.
func (p *Poller) Tick(ctx context.Context) (int, error) {
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	// SKIP LOCKED lets multiple sender replicas claim disjoint batches.
	rows, err := tx.Query(ctx, `
		SELECT o.id, o.event_id, o.tenant_id, o.target_id, o.payload, o.attempts,
		       t.kind, t.config
		FROM outbox o
		JOIN delivery_targets t ON t.id = o.target_id
		WHERE o.status = 'pending'
		  AND o.scheduled_at <= NOW()
		  AND t.active = true
		ORDER BY o.scheduled_at
		LIMIT $1
		FOR UPDATE OF o SKIP LOCKED
	`, p.batchSize)
	if err != nil {
		return 0, fmt.Errorf("claim batch: %w", err)
	}

	var jobs []Job
	for rows.Next() {
		var j Job
		if err := rows.Scan(
			&j.OutboxID, &j.EventID, &j.TenantID, &j.TargetID,
			&j.Payload, &j.Attempts, &j.Kind, &j.Config,
		); err != nil {
			rows.Close()
			return 0, fmt.Errorf("scan row: %w", err)
		}
		jobs = append(jobs, j)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("iterate rows: %w", err)
	}

	for _, j := range jobs {
		if err := p.dispatchAndUpdate(ctx, tx, j); err != nil {
			return 0, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit batch: %w", err)
	}
	return len(jobs), nil
}

func (p *Poller) dispatchAndUpdate(ctx context.Context, tx pgx.Tx, j Job) error {
	dispatchErr := p.dispatcher.Dispatch(ctx, j)
	if dispatchErr != nil {
		slog.Warn("dispatch failed",
			"outbox_id", j.OutboxID,
			"target_id", j.TargetID,
			"error_reason", dispatchErr.Error(),
		)
		_, err := tx.Exec(ctx, `
			UPDATE outbox
			SET status = 'failed', attempts = attempts + 1
			WHERE id = $1
		`, j.OutboxID)
		if err != nil {
			return fmt.Errorf("mark failed: %w", err)
		}
		return nil
	}

	_, err := tx.Exec(ctx, `
		UPDATE outbox
		SET status = 'sent', sent_at = NOW(), attempts = attempts + 1
		WHERE id = $1
	`, j.OutboxID)
	if err != nil {
		return fmt.Errorf("mark sent: %w", err)
	}
	return nil
}
