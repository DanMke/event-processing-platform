//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/DanMke/event-processing-platform/internal/domain"
	pg "github.com/DanMke/event-processing-platform/internal/repository/postgres"
)

func setupDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	ctr, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("events"),
		tcpostgres.WithUsername("events"),
		tcpostgres.WithPassword("events"),
		tcpostgres.WithInitScripts(
			"migrations/001_create_events_table.sql",
			"migrations/002_create_delivery_tables.sql",
		),
		// Postgres restarts after running init scripts; wait for the second
		// "ready" log to ensure the schema is applied before connecting.
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(ctx) })

	connStr, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	t.Cleanup(pool.Close)

	return pool
}

func newEvent(eventID, tenantID string) domain.Event {
	payload, _ := json.Marshal(map[string]any{
		"contract_id": "c-001",
		"amount":      500.0,
		"currency":    "BRL",
	})
	return domain.Event{
		EventID:       eventID,
		TenantID:      tenantID,
		EventType:     "contract.created",
		SchemaVersion: "1.0",
		OccurredAt:    time.Now().UTC(),
		Producer:      "integration-test",
		TraceID:       "trace-int-001",
		Payload:       json.RawMessage(payload),
	}
}

func countOutbox(t *testing.T, pool *pgxpool.Pool, eventID, tenantID string) int {
	t.Helper()
	var n int
	pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM outbox WHERE event_id = $1 AND tenant_id = $2`,
		eventID, tenantID,
	).Scan(&n)
	return n
}

func TestEventRepository_Save(t *testing.T) {
	pool := setupDB(t)
	repo := pg.NewEventRepository(pool)
	ctx := context.Background()

	t.Run("tenant with two targets enqueues two outbox rows", func(t *testing.T) {
		// tenant-01 has webhook + kafka targets in 002 seed (exercises 1:N outbox)
		evt := newEvent("evt-001", "tenant-01")
		if err := repo.Save(ctx, evt); err != nil {
			t.Fatalf("expected nil, got: %v", err)
		}
		if n := countOutbox(t, pool, "evt-001", "tenant-01"); n != 2 {
			t.Errorf("expected 2 outbox rows (webhook + kafka), got %d", n)
		}
	})

	t.Run("tenant with one target enqueues one outbox row", func(t *testing.T) {
		// tenant-03 has kafka target only
		evt := newEvent("evt-002", "tenant-03")
		if err := repo.Save(ctx, evt); err != nil {
			t.Fatalf("expected nil, got: %v", err)
		}
		if n := countOutbox(t, pool, "evt-002", "tenant-03"); n != 1 {
			t.Errorf("expected 1 outbox row (kafka), got %d", n)
		}
	})

	t.Run("duplicate returns ErrDuplicateEvent without re-enqueue", func(t *testing.T) {
		evt := newEvent("evt-dup", "tenant-01")
		if err := repo.Save(ctx, evt); err != nil {
			t.Fatalf("first save: %v", err)
		}
		if err := repo.Save(ctx, evt); !errors.Is(err, domain.ErrDuplicateEvent) {
			t.Fatalf("expected ErrDuplicateEvent, got: %v", err)
		}
		if n := countOutbox(t, pool, "evt-dup", "tenant-01"); n != 2 {
			t.Errorf("expected exactly 2 outbox rows after duplicate attempt, got %d", n)
		}
	})

	t.Run("tenant with no registered target saves without outbox entry", func(t *testing.T) {
		// tenant-99 has no delivery_target in the seed
		evt := newEvent("evt-no-target", "tenant-99")
		if err := repo.Save(ctx, evt); err != nil {
			t.Fatalf("expected nil, got: %v", err)
		}
		if n := countOutbox(t, pool, "evt-no-target", "tenant-99"); n != 0 {
			t.Errorf("expected 0 outbox rows for unregistered tenant, got %d", n)
		}
	})

	t.Run("same event_id different tenants are independent", func(t *testing.T) {
		// PK is (tenant_id, event_id)
		if err := repo.Save(ctx, newEvent("evt-shared", "tenant-01")); err != nil {
			t.Fatalf("tenant-01: %v", err)
		}
		if err := repo.Save(ctx, newEvent("evt-shared", "tenant-02")); err != nil {
			t.Fatalf("tenant-02: %v", err)
		}
	})
}
