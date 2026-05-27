# DurableFlow Architecture

This document describes the current system shape, why the components are split the way they are, and which parts are intentionally still unfinished.

Because this is a personal learning project, I optimized the architecture for:

- durability-first thinking
- readability
- explicit extension points
- gradual feature growth

It is intentionally not over-abstracted.

## Goal

Build a durable workflow engine where workflow state survives crashes, dispatch can be retried safely, and operational behavior is inspectable.

This first pass is intentionally small, but the structure is chosen so the later features can fit naturally.

## Current maturity

What exists today:

- a runnable API service
- a runnable worker service
- a minimal dashboard
- a Postgres-backed workflow state model
- Redis Streams dispatch
- an outbox-based dispatch path for both first-run tasks and retries
- retry scheduling backed by `next_run_at`
- linear task chaining through `next_task`
- a verified multi-step sample execution flow

What does not exist yet:

- DLQ routing
- strong crash recovery
- workflow versioning
- branching or parallel workflow execution

That distinction matters because the project is being built in stages, and the architecture is designed to support that growth cleanly.

## Core principles

- Postgres is the source of truth for workflow definitions, executions, task instances, attempts, and dispatch intent.
- Redis Streams is only a transport for task delivery.
- Delivery semantics are at-least-once.
- Task handlers must be idempotent.
- State transitions should be persisted before relying on in-memory assumptions.

## Components

### `apps/api`

Responsibilities:

- Expose HTTP endpoints for workflow definition creation and execution triggering
- Persist workflow state into Postgres
- Write outbox events as part of the same transaction as task creation
- Run the outbox publisher loop
- Expose `/healthz` and `/metrics`

### `apps/worker`

Responsibilities:

- Consume dispatched tasks from Redis Streams consumer groups
- Load authoritative task state from Postgres before executing work
- Create a task attempt record
- Execute a handler
- Persist success or failure back to Postgres
- Expose `/healthz` and `/metrics`

### `apps/web`

Responsibilities:

- Provide a minimal manual validation shell
- Create a workflow definition
- Trigger an execution
- Poll the current execution snapshot

The web app is still just a thin validation UI, not a real dashboard yet.

### `internal/db`

Responsibilities:

- Open the Postgres pool
- Apply SQL migrations
- Encapsulate data access for definitions, executions, tasks, attempts, and outbox events

### `internal/orchestrator`

Responsibilities:

- Keep workflow-related application logic out of transport and persistence glue
- Host the execution-start logic
- Host the worker-side processing flow

### `internal/outbox`

Responsibilities:

- Poll undispatched outbox rows
- Publish task dispatch messages to Redis Streams
- Mark dispatch completion in Postgres

### `internal/queue`

Responsibilities:

- Hide Redis Streams-specific concerns behind a small adapter
- Create the consumer group if needed
- Publish and consume task messages

### `internal/handlers`

Responsibilities:

- Register handler implementations by key
- Provide the sample task handler used by the initial happy path

### `internal/telemetry`

Responsibilities:

- Bootstrap OpenTelemetry traces
- Expose Prometheus metrics
- Add lightweight HTTP instrumentation

## Current data model

### `workflow_definitions`

Represents the stored definition of a workflow.

Why it exists:

- Lets executions point back to a stable definition
- Gives you a home for future versioning and validation logic

### `workflow_executions`

Represents one run of a workflow.

Why it exists:

- Holds lifecycle state
- Stores execution input and eventual output/error
- Becomes the parent of all task instances
- Stays `running` while a workflow advances from one task to the next

### `task_instances`

Represents the concrete task units created for a workflow execution.

Why it exists:

- Tracks delivery and completion state separately from the workflow execution
- Holds handler routing metadata
- Gives you a place for scheduling fields like `next_run_at`
- Carries an `idempotency_key` for future hardening
- Represents both retry wait state and linear workflow progression

### `task_attempts`

Represents each processing attempt for a task instance.

Why it exists:

- Preserves execution history
- Makes retries observable and auditable
- Provides the retry and progression history that feeds the execution snapshot

### `outbox_events`

Represents dispatch intent stored in Postgres before sending to Redis.

Why it exists:

- Keeps Postgres authoritative
- Decouples durable state changes from transient dispatch
- Provides the base for more reliable re-dispatch and recovery later

## Current request and processing flow

```mermaid
sequenceDiagram
    participant Web
    participant API
    participant Postgres
    participant Outbox
    participant Redis
    participant Worker

    Web->>API: POST /api/workflows
    API->>Postgres: insert workflow_definitions

    Web->>API: POST /api/executions
    API->>Postgres: insert workflow_executions
    API->>Postgres: insert task_instances
    API->>Postgres: insert outbox_events

    Outbox->>Postgres: poll undispatched outbox rows
    Outbox->>Redis: publish task message
    Outbox->>Postgres: mark outbox row dispatched

    Worker->>Redis: XREADGROUP
    Worker->>Postgres: load task + insert task_attempts
    Worker->>Worker: run handler
    Worker->>Postgres: mark task and execution succeeded
    Worker->>Redis: XACK
```

## Current implementation boundaries

The current vertical slice is still intentionally smaller than a full graph engine:

- one workflow execution starts from one entry task
- one successful task can enqueue one `next_task`
- one retryable failure can schedule later redispatch through the outbox
- one outbox event becomes one Redis dispatch
- the sample handler is used to validate the durability path, not business complexity

For now, this is enough to prove the main architecture seams without trying to build every workflow-engine feature at once.

## Why the outbox matters here

Without an outbox, the API could write task state to Postgres and then crash before publishing to Redis, leaving work stranded. With an outbox row, the dispatch intent is durable even if the publish step happens later.

The outbox processor is not hardened yet, but the persistence seam is already there.

## Why at-least-once changes the design

Redis Streams consumer groups can redeliver work, and a publisher can also re-publish if it crashes between external publish and durable acknowledgement.

That means:

- Duplicate task messages are normal, not exceptional
- Worker logic must read task state from Postgres before doing work
- Handlers should be safe to run more than once

The sample handler is intentionally simple, but the system shape assumes idempotency from the start.

## Why I structured it this way

Each major concern already has a clear home:

- HTTP and request decoding live in `internal/httpapi`
- orchestration logic lives in `internal/orchestrator`
- database writes and reads live in `internal/db`
- async transport details live in `internal/queue`
- durable dispatch bridging lives in `internal/outbox`
- task business logic lives in `internal/handlers`

That separation keeps the code approachable while still resembling a real service architecture. It also lets me extend one subsystem at a time without turning every new feature into a full-codebase refactor.

## Known intentional gaps

These are not missing by accident. They are the next learning steps.

- Workflow definitions are not yet expanded into real task graphs
- Retry scheduling is not implemented
- Delayed tasks are not dispatched from `next_run_at`
- Pending consumer-group messages are not reclaimed after worker crashes
- DLQ behavior is not implemented
- Workflow versioning is not implemented
- Cancellation is not implemented
- Handler-level idempotency is only lightly modeled today

## Extension points

### Definition-driven graph expansion

Primary seam:

- [internal/orchestrator/service.go](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/internal/orchestrator/service.go)
- [internal/db/store.go](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/internal/db/store.go)

Current behavior:

- One hardcoded sample task is created per execution

Future direction:

- Parse the stored workflow definition
- Generate task instances from the graph
- Track dependencies and ready-to-run tasks

### Retries and backoff

Primary seam:

- [internal/db/store.go](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/internal/db/store.go:279)
- [internal/orchestrator/worker.go](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/internal/orchestrator/worker.go)

Future direction:

- Fail attempts without always failing the whole workflow immediately
- Compute next retry time
- Requeue only when retry policy allows it

### Delayed execution and scheduler

Primary seam:

- `task_instances.next_run_at`
- `outbox_events.available_at`

Future direction:

- Scheduler loop scans ready tasks in Postgres
- Scheduler writes outbox events when a delayed task becomes runnable

### Crash recovery

Primary seam:

- `task_attempts`
- Redis consumer-group pending entries

Future direction:

- inspect Redis pending entries
- reclaim abandoned deliveries carefully
- reconcile Redis delivery state against authoritative Postgres task state

## Project evolution strategy

The intended build order is:

1. prove a minimal durable happy path
2. replace hardcoded task creation with definition-driven behavior
3. make reads and debugging better
4. add retries and delayed scheduling
5. harden failure handling and recovery
6. only then add more advanced product features

That ordering is intentional. In workflow systems, I think getting the state model right early matters more than adding lots of features quickly.
- `task_instances.status`

Future direction:

- Reconcile in-flight attempts after crashes
- Reclaim pending stream messages
- Decide whether the source of truth says the task should resume, retry, or stop

### Idempotency hardening

Primary seam:

- `task_instances.idempotency_key`
- handler boundary in [internal/handlers](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/internal/handlers)

Future direction:

- Persist deduplication markers per side effect
- Make external integrations safe under duplicate delivery
