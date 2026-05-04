package postgres

import (
	"context"
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

func (r *EventRepository) Save(ctx context.Context, event domain.Event) error {
	result, err := r.pool.Exec(ctx, `
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
	return nil
}
