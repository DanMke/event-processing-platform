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
		tcpostgres.WithInitScripts("migrations/001_create_events_table.sql"),
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
		"amount":      500,
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

func TestEventRepository_Save(t *testing.T) {
	repo := pg.NewEventRepository(setupDB(t))
	ctx := context.Background()

	t.Run("saves event successfully", func(t *testing.T) {
		if err := repo.Save(ctx, newEvent("evt-001", "tenant-a")); err != nil {
			t.Fatalf("expected nil, got: %v", err)
		}
	})

	t.Run("duplicate same tenant returns ErrDuplicateEvent", func(t *testing.T) {
		evt := newEvent("evt-dup", "tenant-a")
		if err := repo.Save(ctx, evt); err != nil {
			t.Fatalf("first save: %v", err)
		}
		if err := repo.Save(ctx, evt); !errors.Is(err, domain.ErrDuplicateEvent) {
			t.Fatalf("expected ErrDuplicateEvent, got: %v", err)
		}
	})

	t.Run("same event_id different tenant saves successfully", func(t *testing.T) {
		// PK is (tenant_id, event_id) — same event_id across tenants is allowed
		if err := repo.Save(ctx, newEvent("evt-shared", "tenant-a")); err != nil {
			t.Fatalf("tenant-a: %v", err)
		}
		if err := repo.Save(ctx, newEvent("evt-shared", "tenant-b")); err != nil {
			t.Fatalf("tenant-b: %v", err)
		}
	})
}
