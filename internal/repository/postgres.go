package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"payment-gateway/internal/payment"
)

type PaymentRepository interface {
	Save(ctx context.Context, p payment.Payment) error
	GetByID(ctx context.Context, paymentID string) (payment.Payment, error)
	Update(ctx context.Context, p payment.Payment) error
	GetByIdempotencyKey(ctx context.Context, idempotencyKey string) (payment.Payment, error)
	SaveProcessedProviderCallback(
		ctx context.Context,
		providerEventID string,
		paymentID string,
		receivedAt time.Time) error
	SaveTx(ctx context.Context, tx pgx.Tx, p payment.Payment) error
	UpdateTx(ctx context.Context, tx pgx.Tx, p payment.Payment) error
}

var ErrPaymentNotFound = errors.New("payment not found")
var ErrOptimisticLockingConflict = errors.New("optimistic locking conflict")
var ErrIdempotencyConflict = errors.New("idempotency key conflict")
var ErrProviderCallbackAlreadyProcessed = errors.New("provider callback already processed")

type PostgresPaymentRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresPaymentRepository(
	pool *pgxpool.Pool,
) *PostgresPaymentRepository {
	return &PostgresPaymentRepository{
		pool: pool,
	}
}

func (r *PostgresPaymentRepository) Save(
	ctx context.Context,
	p payment.Payment,
) error {
	const query = `
		INSERT INTO payments (
			id,
			client_id,
		    idempotency_key,
			amount,
			currency,
			status,
			provider,
			created_at,
			updated_at,
			version
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`

	_, err := r.pool.Exec(
		ctx,
		query,
		p.ID,
		p.ClientID,
		p.IdempotencyKey,
		p.Amount,
		p.Currency,
		p.Status,
		p.Provider,
		p.CreatedAt,
		p.UpdatedAt,
		p.Version,
	)

	var pgErr *pgconn.PgError

	if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "idx_payments_idempotency_key" {
		return ErrIdempotencyConflict
	}

	if err != nil {
		return fmt.Errorf("save payment: %w", err)
	}

	return nil
}

func (r *PostgresPaymentRepository) GetByID(
	ctx context.Context,
	paymentID string,
) (payment.Payment, error) {
	const query = `SELECT id, client_id, amount, currency, status, provider, created_at, updated_at, version FROM payments WHERE id = $1`
	var p payment.Payment
	err := r.pool.QueryRow(ctx, query, paymentID).Scan(&p.ID, &p.ClientID, &p.Amount, &p.Currency, &p.Status, &p.Provider, &p.CreatedAt, &p.UpdatedAt, &p.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return payment.Payment{}, ErrPaymentNotFound
	}
	if err != nil {
		return payment.Payment{}, fmt.Errorf("get payment by id %s: %w", paymentID, err)
	}
	return p, err
}

func (r *PostgresPaymentRepository) Update(ctx context.Context, p payment.Payment) error {
	const query = `
	UPDATE payments
	SET
	    client_id = $1,
	    amount = $2,
	    currency = $3,
	    status = $4,
	    provider = $5,
	    updated_at = $6,
	    version = $7
	WHERE id = $8
	AND version = $9
`
	result, err := r.pool.Exec(ctx, query, p.ClientID, p.Amount, p.Currency, p.Status, p.Provider, p.UpdatedAt, p.Version, p.ID, p.Version-1)

	if err != nil {
		return fmt.Errorf("update payment %s: %w", p.ID, err)
	}

	if result.RowsAffected() == 1 {
		return nil
	}
	if result.RowsAffected() == 0 {
		return ErrOptimisticLockingConflict
	}
	return nil
}

func (r *PostgresPaymentRepository) GetByIdempotencyKey(
	ctx context.Context,
	idempotencyKey string,
) (payment.Payment, error) {
	const query = `SELECT id, client_id, idempotency_key, amount, currency, status, provider, created_at, updated_at, version FROM payments WHERE idempotency_key = $1`
	var p payment.Payment
	err := r.pool.QueryRow(ctx, query, idempotencyKey).Scan(&p.ID, &p.ClientID, &p.IdempotencyKey, &p.Amount, &p.Currency, &p.Status, &p.Provider, &p.CreatedAt, &p.UpdatedAt, &p.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return payment.Payment{}, ErrPaymentNotFound
	}
	if err != nil {
		return payment.Payment{}, fmt.Errorf("get payment by idempotency key %s: %w", idempotencyKey, err)
	}
	return p, err
}

func (r *PostgresPaymentRepository) SaveProcessedProviderCallback(ctx context.Context, providerEventID string, paymentID string, receivedAt time.Time) error {
	const query = `
INSERT INTO processed_provider_callbacks(provider_event_id, payment_id, received_at)
VALUES ($1, $2, $3)`

	_, err := r.pool.Exec(ctx, query, providerEventID, paymentID, receivedAt)

	var pgErr *pgconn.PgError

	if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "processed_provider_callbacks_pkey" {
		return ErrProviderCallbackAlreadyProcessed
	}

	if err != nil {
		return fmt.Errorf("save payment: %w", err)
	}

	return nil

}

func (r *PostgresPaymentRepository) SaveTx(
	ctx context.Context,
	tx pgx.Tx,
	p payment.Payment,
) error {

	const query = `
		INSERT INTO payments (
			id,
			client_id,
		    idempotency_key,
			amount,
			currency,
			status,
			provider,
			created_at,
			updated_at,
			version
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`

	_, err := tx.Exec(
		ctx,
		query,
		p.ID,
		p.ClientID,
		p.IdempotencyKey,
		p.Amount,
		p.Currency,
		p.Status,
		p.Provider,
		p.CreatedAt,
		p.UpdatedAt,
		p.Version,
	)

	var pgErr *pgconn.PgError

	if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "idx_payments_idempotency_key" {
		return ErrIdempotencyConflict
	}

	if err != nil {
		return fmt.Errorf("save payment: %w", err)
	}

	return nil

}

func (r *PostgresPaymentRepository) UpdateTx(
	ctx context.Context,
	tx pgx.Tx,
	p payment.Payment,
) error {
	const query = `
	UPDATE payments
	SET
	    client_id = $1,
	    amount = $2,
	    currency = $3,
	    status = $4,
	    provider = $5,
	    updated_at = $6,
	    version = $7
	WHERE id = $8
	AND version = $9
`
	result, err := tx.Exec(ctx, query, p.ClientID, p.Amount, p.Currency, p.Status, p.Provider, p.UpdatedAt, p.Version, p.ID, p.Version-1)

	if err != nil {
		return fmt.Errorf("update payment %s: %w", p.ID, err)
	}

	if result.RowsAffected() == 1 {
		return nil
	}
	if result.RowsAffected() == 0 {
		return ErrOptimisticLockingConflict
	}
	return nil
}
