package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/DanMke/event-processing-platform/internal/domain"
)

type EventRepository struct {
	pool *pgxpool.Pool
}

func NewEventRepository(pool *pgxpool.Pool) *EventRepository {
	return &EventRepository{pool: pool}
}

// Save persists the event and enqueues outbox rows for active tenant targets.
// The event and outbox rows are written in one transaction.
// Tenants without targets only persist the event.
func (r *EventRepository) Save(ctx context.Context, event domain.Event) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	result, err := tx.Exec(ctx, `
		INSERT INTO events
			(event_id, tenant_id, event_type, schema_version, producer, trace_id, payload, occurred_at, processed_at)
		VALUES
			($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9)
		ON CONFLICT (tenant_id, event_id) DO NOTHING
	`,
		event.EventID,
		event.TenantID,
		event.EventType,
		event.SchemaVersion,
		event.Producer,
		event.TraceID,
		string(event.Payload),
		event.OccurredAt,
		time.Now().UTC(),
	)
	if err != nil {
		return fmt.Errorf("insert event: %w", err)
	}
	if result.RowsAffected() == 0 {
		return domain.ErrDuplicateEvent
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal outbox payload: %w", err)
	}

	// Close rows before inserting; pgx disallows another statement while rows is open.
	rows, err := tx.Query(ctx,
		`SELECT id FROM delivery_targets WHERE tenant_id = $1 AND active = true`,
		event.TenantID,
	)
	if err != nil {
		return fmt.Errorf("query delivery targets: %w", err)
	}
	var targetIDs []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return fmt.Errorf("scan target: %w", err)
		}
		targetIDs = append(targetIDs, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate targets: %w", err)
	}

	for _, targetID := range targetIDs {
		_, err = tx.Exec(ctx, `
			INSERT INTO outbox (event_id, tenant_id, target_id, payload)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (event_id, tenant_id, target_id) DO NOTHING
		`, event.EventID, event.TenantID, targetID, payload)
		if err != nil {
			return fmt.Errorf("insert outbox: %w", err)
		}
	}

	return tx.Commit(ctx)
}
