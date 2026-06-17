# DurableFlow Operations Notes

This is a short runbook for the local stack.

## First checks

```bash
curl -fsS http://localhost:8080/healthz
curl -fsS http://localhost:8081/healthz
make metrics-rules
make metrics-api | rg "durableflow_(http_requests_total|retries_enqueued_total|task_replays_total)"
make metrics-worker | rg "durableflow_(tasks_processed_total|retries_scheduled_total|dead_lettered_tasks_total|reclaimed_messages_total)"
```

Also useful:

- Grafana: [http://localhost:3000](http://localhost:3000)
- Prometheus alerts: [http://localhost:9090/alerts](http://localhost:9090/alerts)
- Dead-letter API: `curl http://localhost:8080/api/dead-letter-tasks?limit=10`

## Current alerts

- `DurableFlowApiDown`
- `DurableFlowWorkerDown`
- `DurableFlowApiLatencyP95High`
- `DurableFlowWorkerLatencyP95High`
- `DurableFlowDeadLetterActivity`
- `DurableFlowRetrySpike`
- `DurableFlowReclaimActivity`

Alert rules live in [deployments/observability/alerts.yml](../deployments/observability/alerts.yml).

## Dead-letter activity

What to check:

- latest execution snapshot
- `last_error_text`
- `attempts_total` vs `max_attempts`
- whether the failure is missing handler, invalid input, or repeatable downstream failure

Safe action:

- fix the root cause first
- replay only the affected task through `POST /api/tasks/<task-id>/replay`

## Retry spike

What to check:

- worker p95 in Grafana
- `durableflow_tasks_processed_total`
- recent deploy or config change

Likely causes:

- transient handler failures
- slower downstream dependency
- outbox cadence making retries look longer than expected

## Reclaim activity

What to check:

- recent worker restart
- worker health flapping
- throughput or p95 shift during the same window

Meaning:

- a consumer crashed or stalled after claiming messages
- Redis reclaimed pending messages

This is expected behavior under at-least-once recovery.

## High latency

What to check:

- current `OUTBOX_POLL_INTERVAL`
- whether the workload is happy-path or failure-heavy
- whether dead-letter or retry counters are rising

Rule of thumb:

- API p95 issues usually mean control-plane pressure or snapshot polling load
- worker p95 issues usually mean retries, handler failures, or reclaim behavior

## Startup config checks

API and worker fail fast on malformed runtime settings.

Examples:

- invalid `OUTBOX_POLL_INTERVAL`
- invalid `REDIS_RECLAIM_COUNT`
- zero or negative numeric settings
- missing required connection settings such as `DATABASE_URL` or `REDIS_ADDR`
