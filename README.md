# DurableFlow

DurableFlow is a workflow-engine project I am building to learn distributed systems by actually building one.

I wanted a project that would force me to think about the parts that are usually hand-waved away in smaller backend apps: durable state, retries, delayed work, duplicate delivery, crash recovery, and observability. I am building those pieces in layers so each step is testable and easier to reason about.

The core rule behind the design is simple:

- Postgres is the source of truth.
- Redis Streams is only the dispatch mechanism.
- Workers must tolerate duplicate delivery.

This repository is intentionally incomplete. Right now it has a runnable foundation plus one thin happy-path vertical slice so I can test the architecture end to end while building the harder parts incrementally.

## Project status

What works today:

- Working local stack with API, worker, dashboard, Postgres, Redis, and observability services
- One verified end-to-end happy path from workflow creation to task completion
- Basic schema, orchestration, outbox dispatch, and worker processing in place
- Many production-shaping features still intentionally pending

I want the repo to reflect the current state of the build clearly, with the foundation in place and the next phases mapped out.

## Why this exists

A lot of business workflows are long-running, asynchronous, and failure-prone:

- payment and refund flows
- order approval pipelines
- document processing
- KYC / verification workflows
- notification and follow-up jobs

These systems often start as scattered background jobs, cron tasks, or tightly coupled API logic. That usually creates the same problems:

- no durable workflow state
- poor visibility into what failed
- unsafe retries
- duplicate side effects
- hard crash recovery

This project is my way of learning how to design a system that handles those problems with explicit persistence, asynchronous dispatch, retry-ready state modeling, and operational visibility.

## What is included now

- Monorepo-style layout for API, worker, and dashboard
- Docker-based local stack for Postgres, Redis, API, worker, Prometheus, Grafana, and an OpenTelemetry collector
- Go API service with health and minimal workflow/execution endpoints
- Go worker service with Redis Streams consumption and a sample handler
- SQL migrations for:
  - `workflow_definitions`
  - `workflow_executions`
  - `task_instances`
  - `task_attempts`
  - `outbox_events`
- Outbox publisher so task dispatch is anchored in Postgres
- Basic tracing and Prometheus metrics bootstrap
- React + TypeScript + Vite dashboard for manual local validation
- Architecture notes and implementation roadmap docs

## Current scope in plain English

Today, the repo proves that this flow works:

1. Create a workflow definition
2. Trigger an execution
3. Persist execution state in Postgres
4. Persist a task and an outbox event in Postgres
5. Dispatch the task through Redis Streams
6. Let a worker process it
7. Persist the final result back to Postgres

That is enough to validate the shape of the system.

What it does not prove yet is the full workflow-engine problem space: retries, delayed tasks, recovery, DLQ handling, richer graph execution, and stronger idempotency boundaries.

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

## Why I think this is a useful foundation

Even though the feature set is still small, the current version already exercises a few important durability concepts:

- workflow and task state is persisted before async dispatch is relied on
- dispatch intent is stored in an outbox table instead of being fire-and-forget
- worker execution records attempts durably
- Redis is treated as transport rather than the system of record
- the codebase is structured so retries, scheduling, and recovery can be added without rewriting the foundation

That is the main reason the repo looks the way it does right now. I am trying to get the boundaries right early instead of adding features first and cleaning up later.

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

## How to read this repository

When I come back to the codebase, this is the reading order I use:

1. Read this file for project intent and current scope
2. Read [ARCHITECTURE.md](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/ARCHITECTURE.md) for system responsibilities and data flow
3. Read [TASKS.md](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/TASKS.md) to see the planned implementation path
4. Inspect the migration in [migrations/001_init.sql](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/migrations/001_init.sql)
5. Follow the happy path through:
   - [apps/api/main.go](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/apps/api/main.go)
   - [internal/orchestrator/service.go](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/internal/orchestrator/service.go)
   - [internal/outbox/publisher.go](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/internal/outbox/publisher.go)
   - [apps/worker/main.go](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/apps/worker/main.go)
   - [internal/orchestrator/worker.go](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/internal/orchestrator/worker.go)

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

Right now, each execution creates one hardcoded sample task. It keeps the first pass simple and makes the execution flow easy to inspect before I add definition-driven task creation.

## Current snapshot

As of the current version:

- `3` application services: `api`, `worker`, `web`
- `8` total Docker Compose services in the local stack
- `5` core workflow tables
- `7` backend HTTP endpoints across API and worker
- `1` fully verified end-to-end happy path
- `~0.9s` observed local trigger-to-completion latency in one verified run

I keep these numbers here mainly to make the current scope explicit.

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

## Current design choices

- Postgres owns workflow state, task state, attempts, and dispatch intent.
- Redis Streams is not authoritative; it only carries dispatch messages.
- The outbox table is already present so you can grow toward stronger durability without reworking the entire write path later.
- At-least-once delivery is embraced, so handlers must behave idempotently.
- The code stays readable on purpose. It favors obvious seams over early abstraction.

## What I plan to build next

The next major milestones are:

- definition-driven task creation
- richer execution inspection APIs
- retry policy with backoff
- delayed task scheduling
- DLQ behavior
- worker crash recovery
- stronger idempotency guarantees

Those are tracked in more detail in [TASKS.md](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/TASKS.md).

I am treating DurableFlow as a long-term build, with the early work focused on getting the core execution model and system boundaries right.

## Next docs

- System responsibilities and data flow: [ARCHITECTURE.md](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/ARCHITECTURE.md)
- Learning roadmap and backlog: [TASKS.md](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/TASKS.md)
