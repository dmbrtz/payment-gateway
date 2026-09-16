package outbox

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresOutboxRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresOutboxRepository(pool *pgxpool.Pool) *PostgresOutboxRepository {
	return &PostgresOutboxRepository{
		pool: pool,
	}
}

func (r *PostgresOutboxRepository) Save(ctx context.Context, event OutboxEvent) error {
	const query = `
INSERT INTO outbox_events (id, aggregate_id, event_type, payload, created_at)
VALUES ($1, $2, $3, $4, $5)
`
	_, err := r.pool.Exec(ctx, query, &event.ID, &event.AggregateID, &event.EventType, &event.Payload, &event.CreatedAt)

	if err != nil {
		return fmt.Errorf("save outbox event error %s: %w",
			event.ID,
			err,
		)
	}
	return nil
}

func (r *PostgresOutboxRepository) GetPending(ctx context.Context, limit int) ([]OutboxEvent, error) {
	const query = `
SELECT 
	id,
	aggregate_id,
	event_type,
	payload,
	created_at
FROM outbox_events
WHERE published_at IS NULL
ORDER BY created_at ASC
LIMIT $1
`
	rows, err := r.pool.Query(ctx, query, limit)

	if err != nil {
		return nil, fmt.Errorf("get outbox events error: %w", err)
	}

	defer rows.Close()
	var events []OutboxEvent

	for rows.Next() {
		var event OutboxEvent
		err = rows.Scan(&event.ID,
			&event.AggregateID,
			&event.EventType,
			&event.Payload,
			&event.CreatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("get outbox events error: %w", err)
		}
		events = append(events, event)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate outbox event errors: %w", err)
	}

	return events, nil
}

func (r *PostgresOutboxRepository) MarkPublished(ctx context.Context, eventID string) error {
	const query = `
UPDATE outbox_events
SET published_at = now()
WHERE id = $1
`
	_, err := r.pool.Exec(ctx, query, &eventID)

	if err != nil {
		return fmt.Errorf("mark outbox event error: %w", err)
	}

	return nil
}

func (r *PostgresOutboxRepository) SaveTx(
	ctx context.Context,
	tx pgx.Tx,
	event OutboxEvent,
) error {
	const query = `
INSERT INTO outbox_events (id, aggregate_id, event_type, payload, created_at)
VALUES ($1, $2, $3, $4, $5)
`
	_, err := tx.Exec(ctx, query, &event.ID, &event.AggregateID, &event.EventType, &event.Payload, &event.CreatedAt)

	if err != nil {
		return fmt.Errorf("save outbox event error %s: %w",
			event.ID,
			err,
		)
	}
	return nil
}
