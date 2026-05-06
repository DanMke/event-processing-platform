//go:build integration

package sender_test

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

	"github.com/DanMke/event-processing-platform/internal/sender"
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
			"../repository/postgres/migrations/001_create_events_table.sql",
			"../repository/postgres/migrations/002_create_delivery_tables.sql",
		),
		// Init scripts finish before the second "ready" log.
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

// seedOutbox inserts an event for tenant-01 and one outbox row per active target.
func seedOutbox(t *testing.T, pool *pgxpool.Pool, eventID string) {
	t.Helper()
	ctx := context.Background()

	payload := json.RawMessage(`{"contract_id":"c-1","amount":100,"currency":"BRL"}`)
	_, err := pool.Exec(ctx, `
		INSERT INTO events (event_id, tenant_id, event_type, schema_version, producer, trace_id, payload, occurred_at, processed_at)
		VALUES ($1, 'tenant-01', 'contract.created', '1.0', 'test', 't', $2::jsonb, NOW(), NOW())
	`, eventID, string(payload))
	if err != nil {
		t.Fatalf("seed event: %v", err)
	}

	_, err = pool.Exec(ctx, `
		INSERT INTO outbox (event_id, tenant_id, target_id, payload)
		SELECT $1, 'tenant-01', id, $2::jsonb
		FROM delivery_targets WHERE tenant_id = 'tenant-01' AND active = true
	`, eventID, string(payload))
	if err != nil {
		t.Fatalf("seed outbox: %v", err)
	}
}

func countByStatus(t *testing.T, pool *pgxpool.Pool, eventID, status string) int {
	t.Helper()
	var n int
	pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM outbox WHERE event_id = $1 AND status = $2`,
		eventID, status,
	).Scan(&n)
	return n
}

type stubDispatcher struct {
	calls   int
	failOn  string
	failErr error
}

func (s *stubDispatcher) Dispatch(_ context.Context, j sender.Job) error {
	s.calls++
	if s.failOn != "" && j.TargetID == s.failOn {
		return s.failErr
	}
	return nil
}

func TestPoller_Tick_ClaimsAndMarksSent(t *testing.T) {
	pool := setupDB(t)
	seedOutbox(t, pool, "evt-success")

	disp := &stubDispatcher{}
	p := sender.NewPoller(pool, disp, time.Second, 10)

	n, err := p.Tick(context.Background())
	if err != nil {
		t.Fatalf("tick: %v", err)
	}
	if n != 2 {
		t.Errorf("expected 2 jobs processed, got %d", n)
	}
	if disp.calls != 2 {
		t.Errorf("expected dispatcher called 2x, got %d", disp.calls)
	}
	if got := countByStatus(t, pool, "evt-success", "sent"); got != 2 {
		t.Errorf("expected 2 sent rows, got %d", got)
	}
	if got := countByStatus(t, pool, "evt-success", "pending"); got != 0 {
		t.Errorf("expected 0 pending rows, got %d", got)
	}
}

func TestPoller_Tick_DispatchFailureMarksFailed(t *testing.T) {
	pool := setupDB(t)
	seedOutbox(t, pool, "evt-fail")

	disp := &stubDispatcher{
		failOn:  "webhook-tenant-01",
		failErr: errors.New("simulated http 500"),
	}
	p := sender.NewPoller(pool, disp, time.Second, 10)

	n, err := p.Tick(context.Background())
	if err != nil {
		t.Fatalf("tick: %v", err)
	}
	if n != 2 {
		t.Errorf("expected both jobs claimed, got %d", n)
	}
	if got := countByStatus(t, pool, "evt-fail", "sent"); got != 1 {
		t.Errorf("expected 1 sent row, got %d", got)
	}
	if got := countByStatus(t, pool, "evt-fail", "failed"); got != 1 {
		t.Errorf("expected 1 failed row, got %d", got)
	}
}

func TestPoller_Tick_RespectsBatchSize(t *testing.T) {
	pool := setupDB(t)
	seedOutbox(t, pool, "evt-a")
	seedOutbox(t, pool, "evt-b")

	disp := &stubDispatcher{}
	p := sender.NewPoller(pool, disp, time.Second, 1)

	n, err := p.Tick(context.Background())
	if err != nil {
		t.Fatalf("tick: %v", err)
	}
	if n != 1 {
		t.Errorf("expected batch size 1, got %d", n)
	}

	var totalPending int
	pool.QueryRow(context.Background(),
		`SELECT COUNT(*) FROM outbox WHERE status = 'pending'`,
	).Scan(&totalPending)
	if totalPending != 3 {
		t.Errorf("expected 3 still pending, got %d", totalPending)
	}
}
