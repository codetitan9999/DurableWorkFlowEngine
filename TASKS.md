# DurableFlow Roadmap

This file is a short status and roadmap note.

## Current state

DurableFlow already supports:

- workflow definitions and executions in Postgres
- transactional outbox dispatch
- Redis Streams consumption
- persisted retries with `next_run_at`
- linear multi-step chaining
- dead-letter listing and replay
- stale pending-message reclaim
- handler-level idempotency
- a small dashboard for inspection and recovery

## Completed milestones

1. Definition-driven execution
2. Execution snapshots and attempt visibility
3. Persisted retry scheduling
4. Retry redispatch through the outbox
5. Dead-letter handling and replay
6. Crash recovery with `XAUTOCLAIM`
7. Handler-level idempotency with stored responses

## Next likely work

### 1. Workflow versioning

Why:

- bind each execution to an immutable definition version
- avoid in-place workflow mutation problems

### 2. Richer graph execution

Why:

- move beyond linear `next_task`
- support branching or parallel progression safely

### 3. Stronger operator tooling

Why:

- make retries, replay, and queue behavior easier to inspect
- improve dashboards, metrics, and audit visibility

## Recommended order

1. Workflow versioning
2. Richer graph execution
3. Stronger operator tooling

That order keeps the definition model stable before expanding workflow complexity.
