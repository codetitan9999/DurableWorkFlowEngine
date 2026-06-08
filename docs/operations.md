# DurableFlow Operations Notes

This document is intentionally short. The goal is to make the current system easier to operate when retries, dead-lettering, or worker interruptions happen.

## First checks

When something looks wrong, start here:

```bash
curl -fsS http://localhost:8080/healthz
curl -fsS http://localhost:8081/healthz
make metrics-rules
make metrics-api | rg "durableflow_(http_requests_total|retries_enqueued_total|task_replays_total)"
make metrics-worker | rg "durableflow_(tasks_processed_total|retries_scheduled_total|dead_lettered_tasks_total|reclaimed_messages_total)"
```

Also check:

- Grafana dashboard: [http://localhost:3000](http://localhost:3000)
- Prometheus alerts/rules UI: [http://localhost:9090/alerts](http://localhost:9090/alerts)
- Dead-letter API: `curl http://localhost:8080/api/dead-letter-tasks?limit=10`

## Current alert rules

The local Prometheus config now loads alert rules from [deployments/observability/alerts.yml](../deployments/observability/alerts.yml).

Current alerts:

- `DurableFlowApiDown`
- `DurableFlowWorkerDown`
- `DurableFlowApiLatencyP95High`
- `DurableFlowWorkerLatencyP95High`
- `DurableFlowDeadLetterActivity`
- `DurableFlowRetrySpike`
- `DurableFlowReclaimActivity`

## Runbook: dead-letter activity

Symptoms:

- `DurableFlowDeadLetterActivity` fires
- dead-letter panel in the dashboard starts filling up
- `durableflow_dead_lettered_tasks_total` increases

What to check:

- open the latest execution snapshot in the dashboard
- inspect `last_error_text` on the dead-lettered task
- compare `attempts_total` with the workflow definition's `max_attempts`
- verify whether the failure is a missing handler, invalid input, or a persistent downstream error

What it usually means:

- missing handler registration
- invalid input shape
- retry budget exhausted for a repeatable downstream failure

Safe operator action:

- fix the underlying issue first
- replay only the affected dead-lettered task through `POST /api/tasks/<task-id>/replay`

## Runbook: retry spike

Symptoms:

- `DurableFlowRetrySpike` fires
- `durableflow_retries_scheduled_total` rises faster than normal

What to check:

- worker p95 panel in Grafana
- task status mix in `durableflow_tasks_processed_total`
- recent deploy or config change that could have altered handler behavior

What it usually means:

- a handler is failing transiently
- outbox cadence is making backoff look longer than expected
- downstream dependencies are slower or flaky

## Runbook: reclaim activity

Symptoms:

- `DurableFlowReclaimActivity` fires
- `durableflow_reclaimed_messages_total` increases

What to check:

- whether a worker restarted recently
- whether the worker health endpoint is flapping
- whether task throughput or p95 latency changed at the same time

What it usually means:

- a consumer crashed after claiming Redis messages
- a consumer stalled long enough for Redis pending entries to be reclaimed

Why this is acceptable:

- reclaim is part of the intended at-least-once recovery path
- workers still consult Postgres before doing work, so reclaimed messages should not cause duplicate side effects by themselves

## Runbook: elevated API or worker latency

Symptoms:

- `DurableFlowApiLatencyP95High` or `DurableFlowWorkerLatencyP95High` fires

What to check:

- whether the default `OUTBOX_POLL_INTERVAL=2s` is still being used
- whether the workload is success-heavy, mixed, or failure-heavy
- whether dead-letter or retry counters are rising at the same time

How to interpret it:

- API p95 issues usually point to control-plane pressure or snapshot polling load
- worker p95 issues usually point to handler failures, retries, or reclaim behavior
- high latency with flat throughput often means queueing delay, not raw worker CPU saturation

## Startup config checks

API and worker startup now fail fast on malformed runtime settings instead of silently falling back.

Examples that now stop startup immediately:

- invalid `OUTBOX_POLL_INTERVAL`
- invalid `REDIS_RECLAIM_COUNT`
- zero or negative duration/integer settings
- missing required connection settings such as `DATABASE_URL` or `REDIS_ADDR`
