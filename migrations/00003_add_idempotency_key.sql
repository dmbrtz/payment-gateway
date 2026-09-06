-- +goose Up

ALTER TABLE payments
ADD COLUMN idempotency_key TEXT;

CREATE UNIQUE INDEX idx_payments_idempotency_key
ON payments (idempotency_key);

-- +goose Down

DROP INDEX IF EXISTS idx_payments_idempotency_key;

ALTER TABLE payments
DROP COLUMN idempotency_key;