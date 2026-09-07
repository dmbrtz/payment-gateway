-- +goose Up

CREATE TABLE IF NOT EXISTS processed_provider_callbacks (
    provider_event_id TEXT PRIMARY KEY,
    payment_id TEXT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_processed_provider_callbacks_payment_id
ON processed_provider_callbacks (payment_id);

-- +goose Down

DROP TABLE IF EXISTS processed_provider_callbacks;