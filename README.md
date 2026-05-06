# DurableFlow

DurableFlow is a learning-first starter for a durable workflow engine built around a simple rule:

- Postgres is the source of truth.
- Redis Streams is only the dispatch mechanism.
- Workers must tolerate duplicate delivery.

This repository is intentionally scaffolded, not finished. It includes one thin happy-path vertical slice so you can validate the architecture locally, then implement the important workflow-engine features yourself.

## What is included now

- Monorepo-style layout for API, worker, and dashboard
- Docker-based local stack for Postgres, Redis, API, worker, Prometheus, Grafana, and an OpenTelemetry collector
- Go API skeleton with health and minimal workflow/execution endpoints
- Go worker skeleton with Redis Streams consumption and a sample handler
- SQL migrations for:
  - `workflow_definitions`
  - `workflow_executions`
  - `task_instances`
  - `task_attempts`
  - `outbox_events`
- Minimal outbox publisher so task dispatch is anchored in Postgres
- Basic tracing and Prometheus metrics bootstrap
- React + TypeScript + Vite dashboard shell
- Starter architecture and implementation roadmap docs

## What is intentionally not implemented yet

- Retry engine with backoff
- Delayed task scheduler
- Dead-letter queue routing
- Cancellation and timeouts
- Workflow versioning
- Recovery of stuck/pending Redis consumer-group messages
- Advanced dashboard features
- AI workflow generation and failure summarization

Those are left as explicit extension points and learning tasks in [TASKS.md](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/TASKS.md).

## Repository layout

```text
apps/
  api/       HTTP API service
  worker/    Redis Streams worker service
  web/       React dashboard shell
internal/
  config/       environment loading
  db/           Postgres access and migrations
  domain/       shared domain models
  handlers/     task handler registry and sample handler
  httpapi/      HTTP routing and health endpoints
  orchestrator/ workflow service and worker orchestration
  outbox/       Postgres outbox publisher
  queue/        Redis Streams adapter
  telemetry/    metrics and tracing bootstrap
migrations/     SQL schema files
deployments/    local observability config
docs/           supplementary notes
```

## Thin happy path

The current vertical slice does exactly this:

1. Create a workflow definition with `POST /api/workflows`
2. Trigger an execution with `POST /api/executions`
3. Persist a workflow execution row
4. Persist one sample task instance
5. Persist an outbox event in Postgres
6. Publish that event to Redis Streams
7. Consume it in the worker
8. Run a mock handler
9. Persist task success and execution success back into Postgres

The current implementation deliberately expands every execution into one hardcoded sample task. That seam is where you will later implement definition-driven graph expansion.

## Prerequisites

- Docker
- Docker Compose v2 available as `docker compose`
- Optional for native backend work outside Docker: Go 1.23+
- Optional for native frontend work outside Docker: Node 22+ and npm

If `docker compose` is missing on your machine, install or enable the Docker Compose v2 plugin first.

## Local setup

1. Copy the environment template:

```bash
cp .env.example .env
```

2. Start the local stack:

```bash
docker compose up --build
```

3. Open the local services:

- Dashboard: [http://localhost:5173](http://localhost:5173)
- API health: [http://localhost:8080/healthz](http://localhost:8080/healthz)
- Worker health: [http://localhost:8081/healthz](http://localhost:8081/healthz)
- API metrics: [http://localhost:8080/metrics](http://localhost:8080/metrics)
- Worker metrics: [http://localhost:8081/metrics](http://localhost:8081/metrics)
- Prometheus: [http://localhost:9090](http://localhost:9090)
- Grafana: [http://localhost:3000](http://localhost:3000) with `admin` / `admin`

## Minimal API usage

Create a workflow:

```bash
curl -X POST http://localhost:8080/api/workflows \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "demo-order-approval",
    "description": "Starter workflow",
    "definition": {
      "entry_task": "sample-task",
      "tasks": [
        {
          "name": "sample-task",
          "handler_key": "sample.echo"
        }
      ]
    }
  }'
```

Trigger an execution:

```bash
curl -X POST http://localhost:8080/api/executions \
  -H 'Content-Type: application/json' \
  -d '{
    "workflow_definition_id": "<workflow-definition-id>",
    "input": {
      "customer_id": "demo-customer-123",
      "order_id": "demo-order-456"
    }
  }'
```

Fetch execution state:

```bash
curl http://localhost:8080/api/executions/<execution-id>
```

## Key design decisions in this starter

- Postgres owns workflow state, task state, attempts, and dispatch intent.
- Redis Streams is not authoritative; it only carries dispatch messages.
- The outbox table is already present so you can grow toward stronger durability without reworking the entire write path later.
- At-least-once delivery is embraced, so handlers must behave idempotently.
- The code stays readable on purpose. It favors obvious seams over early abstraction.

## Next docs

- System responsibilities and data flow: [ARCHITECTURE.md](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/ARCHITECTURE.md)
- Learning roadmap and backlog: [TASKS.md](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/TASKS.md)

