ALTER TABLE idempotency_records
    ADD COLUMN IF NOT EXISTS owner_task_instance_id UUID REFERENCES task_instances(id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_task_instances_idempotency_key_unique
    ON task_instances (idempotency_key);
