package outbox

import (
	"context"

	"github.com/jackc/pgx/v5"
)

type OutboxRepository interface {
	Save(ctx context.Context, event OutboxEvent) error
	GetPending(ctx context.Context, limit int) ([]OutboxEvent, error)
	MarkPublished(ctx context.Context, eventID string) error
	SaveTx(ctx context.Context, tx pgx.Tx, event OutboxEvent) error
}
