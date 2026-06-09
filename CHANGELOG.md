# Changelog

## v0.1.0 - 2026-06-09

Initial public release of DurableFlow as a correctness-focused workflow orchestration project.

Highlights:

- workflow definitions, execution creation, and execution snapshots backed by Postgres
- transactional outbox for durable dispatch intent
- Redis Streams delivery with consumer-group reclaim for abandoned messages
- retries with persisted `next_run_at`, dead-letter handling, and replay through the normal dispatch path
- handler-level idempotency with stored responses and reservation ownership
- React/TypeScript dashboard for workflow creation, inspection, dead-letter visibility, and replay
- Prometheus, Grafana, and OpenTelemetry support in the local stack
- benchmark artifacts, charts, and operational runbooks for local verification
