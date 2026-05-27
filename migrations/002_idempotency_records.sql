CREATE TABLE IF NOT EXISTS idempotency_records (
    handler_key TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    status TEXT NOT NULL,
    response_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (handler_key, idempotency_key)
);

CREATE INDEX IF NOT EXISTS idx_idempotency_records_status_updated
    ON idempotency_records (status, updated_at);
