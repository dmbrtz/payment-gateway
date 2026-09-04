-- +goose Up
CREATE TABLE IF NOT EXISTS payment_views (
    id  TEXT PRIMARY KEY,
    client_id   TEXT NOT NULL,
    amount BIGINT NOT NULL,
    currency TEXT NOT NULL,
    status TEXT NOT NULL,
    provider TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    version BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_payment_views_client_id
    ON payment_views (client_id);

-- +goose Down
DROP TABLE IF EXISTS payment_views;