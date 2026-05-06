CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS workflow_definitions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE,
    description TEXT NOT NULL DEFAULT '',
    version INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'active',
    definition_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS workflow_executions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_definition_id UUID NOT NULL REFERENCES workflow_definitions(id),
    status TEXT NOT NULL,
    input_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    output_json JSONB,
    error_text TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS task_instances (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workflow_execution_id UUID NOT NULL REFERENCES workflow_executions(id),
    task_name TEXT NOT NULL,
    handler_key TEXT NOT NULL,
    status TEXT NOT NULL,
    input_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    output_json JSONB,
    next_run_at TIMESTAMPTZ,
    last_error_text TEXT,
    attempts_total INTEGER NOT NULL DEFAULT 0,
    idempotency_key TEXT NOT NULL,
    dispatched_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS task_attempts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_instance_id UUID NOT NULL REFERENCES task_instances(id),
    attempt_number INTEGER NOT NULL,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    error_text TEXT,
    output_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type TEXT NOT NULL,
    aggregate_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    payload_json JSONB NOT NULL,
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    dispatched_at TIMESTAMPTZ,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error_text TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_workflow_executions_definition_status
    ON workflow_executions (workflow_definition_id, status);

CREATE INDEX IF NOT EXISTS idx_task_instances_execution_status
    ON task_instances (workflow_execution_id, status);

CREATE INDEX IF NOT EXISTS idx_task_attempts_task_attempt_number
    ON task_attempts (task_instance_id, attempt_number);

CREATE INDEX IF NOT EXISTS idx_outbox_events_ready
    ON outbox_events (dispatched_at, available_at, created_at);

