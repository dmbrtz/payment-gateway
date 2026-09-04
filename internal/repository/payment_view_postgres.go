package repository

import (
	"context"
	"errors"
	"fmt"
	"payment-gateway/internal/readmodel"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresPaymentViewRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresPaymentViewRepository(pool *pgxpool.Pool) *PostgresPaymentViewRepository {
	return &PostgresPaymentViewRepository{
		pool: pool,
	}
}

func (r *PostgresPaymentViewRepository) GetByID(
	ctx context.Context,
	id string,
) (*readmodel.PaymentView, error) {
	var view readmodel.PaymentView

	query := `
SELECT 
id,
client_id,
amount,
currency,
status,
provider,
created_at,
updated_at,
version
FROM payment_views
WHERE id = $1
`

	row := r.pool.QueryRow(
		ctx,
		query,
		id,
	)

	err := row.Scan(
		&view.ID,
		&view.ClientID,
		&view.Amount,
		&view.Currency,
		&view.Status,
		&view.Provider,
		&view.CreatedAt,
		&view.UpdatedAt,
		&view.Version,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrPaymentViewNotFound
		}
		return nil, fmt.Errorf("scan payment view error: %w", err)
	}

	return &view, nil
}
