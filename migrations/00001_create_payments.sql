-- +goose Up
CREATE TABLE IF NOT EXISTS payments (
                                        id          TEXT PRIMARY KEY,
                                        client_id   TEXT NOT NULL,
                                        amount      BIGINT NOT NULL CHECK (amount > 0),
    currency    TEXT NOT NULL,
    status      TEXT NOT NULL,
    provider    TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL,
    version     BIGINT NOT NULL CHECK (version > 0)
    );

CREATE INDEX IF NOT EXISTS idx_payments_client_id
    ON payments (client_id);

-- +goose Down
DROP TABLE IF EXISTS payments;