package projection

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository interface {
	Save(ctx context.Context, view PaymentView) error
	Update(ctx context.Context, view PaymentView) error
}

type PostgresRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(pool *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{
		pool: pool,
	}
}

func (r *PostgresRepository) Save(ctx context.Context, view PaymentView) error {
	const query = `
INSERT INTO payment_views (
                           id,
                           client_id,
                           amount,
                           currency,
                           status,
                           provider,
                           created_at,
                           updated_at,
                           version
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
`
	_, err := r.pool.Exec(ctx,
		query,
		view.ID,
		view.ClientID,
		view.Amount,
		view.Currency,
		view.Status,
		view.Provider,
		view.CreatedAt,
		view.UpdatedAt,
		view.Version,
	)
	if err != nil {
		return fmt.Errorf("save payment view error: %w", err)
	}

	return nil
}

func (r *PostgresRepository) Update(ctx context.Context, view PaymentView) error {
	const query = `
UPDATE payment_views 
SET
    status = $1,
    updated_at = $2,
    version = $3
WHERE id = $4
`
	_, err := r.pool.Exec(ctx,
		query,
		view.Status,
		view.UpdatedAt,
		view.Version,
		view.ID,
	)
	if err != nil {
		return fmt.Errorf("update payment view error: %w", err)
	}

	return nil
}
