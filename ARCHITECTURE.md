# DurableFlow Architecture

DurableFlow is a small workflow engine built around one idea: keep execution truth in Postgres, use Redis Streams only for delivery, and assume duplicate delivery can happen.

## Diagram index

- [System view](#system-view): services and infrastructure
- [Class diagram](#class-diagram): Go structs, interfaces, and collaborators
- [Entity relationship view](#entity-relationship-view): tables, keys, and relationships
- [Main flow](#main-flow) and [task lifecycle](#task-lifecycle)
- [Low-level design](docs/low-level-design.md): domain and handler class diagrams, construction, and sequences for dispatch, chaining, retries, replay, idempotency, recovery, and snapshot reads

## Goal

The system is meant to handle the failure cases that usually make background work messy:

- state is written but work is not published
- a task is delivered more than once
- retries need to survive restarts
- a task fails permanently and needs replay
- a worker crashes after claiming a message

## Core invariants

### 1. Postgres is authoritative

Workflow definitions, executions, tasks, attempts, retries, dead-letter state, outbox rows, and idempotency records all live in Postgres.

If Redis and Postgres disagree, Postgres wins.

### 2. Redis Streams is transport

Redis carries delivery opportunities. It does not define workflow truth.

Workers always check Postgres before running task logic.

### 3. Delivery is at-least-once

Duplicate delivery is expected because:

- publish can succeed before durable acknowledgment
- a worker can crash before acking
- stale pending messages can be reclaimed later

### 4. Idempotency is explicit

Handlers that cross side-effect boundaries use `idempotency_records` so duplicate-safe behavior is visible in durable state.

## System view

```mermaid
flowchart LR
    User["User / Operator"] --> Web["Web Dashboard"]
    Web --> API["API Service"]
    API --> PG[("Postgres")]
    API --> Outbox["Outbox Publisher"]
    Outbox --> Redis[("Redis Streams")]
    Redis --> Worker["Worker Service"]
    Worker --> PG
    Worker --> Handlers["Handlers"]
    API --> OTEL["OpenTelemetry Collector"]
    Worker --> OTEL
    OTEL --> Prom["Prometheus"]
    Prom --> Graf["Grafana"]
```

## Main components

### API

`apps/api`

Responsible for:

- storing workflow definitions
- creating executions
- creating the first task
- writing outbox intent in the same transaction
- exposing execution, dead-letter, and replay APIs
- running the outbox publisher loop

### Worker

`apps/worker`

Responsible for:

- consuming Redis Streams messages
- reclaiming stale pending messages
- loading authoritative task state
- starting attempts
- running handlers
- deciding success, retry, dead-letter, or next-task progression

### Dashboard

`apps/web`

Keeps the system easy to inspect:

- create definitions
- trigger executions
- view execution snapshots
- inspect attempts and retry state
- list dead-lettered tasks
- replay dead-lettered tasks

Reference screenshots:

- [Overview](docs/screenshots/01-overview.png)
- [Successful execution](docs/screenshots/02-successful-execution.jpeg)
- [Dead-letter handling](docs/screenshots/03-dead-letter-panel.jpeg)
- [Replay flow](docs/screenshots/04-replay-response.jpeg)

### Outbox

`internal/outbox`

Bridges Postgres state and Redis publish.

The same outbox path is used for:

- first-run dispatch
- retry redispatch
- replay
- next-task progression

### Queue adapter

`internal/queue`

Wraps Redis Streams details:

- publish
- consumer-group setup
- read and decode
- stale-message reclaim with `XAUTOCLAIM`

### Orchestrator

`internal/orchestrator`

Owns workflow semantics:

- definition validation
- execution creation
- retry behavior
- dead-letter decisions
- next-task chaining

## Class diagram

These boxes represent Go structs and interfaces. Method lists show the main responsibilities; interface satisfaction is implicit in Go. `Store` owns SQL and transactions, while `Worker` chooses the execution outcome.

```mermaid
classDiagram
    direction TB
    class Router {
        -handleExecutions()
        -handleExecutionSnapshot()
        -handleTaskActions()
    }
    class Service {
        +CreateWorkflowDefinition()
        +TriggerExecution()
        +GetExecutionSnapshot()
        +GetDeadLetteredTasks()
        +ReplayDeadLetteredTask()
    }
    class Worker {
        +GetWorkflowSpecAndTaskSpecByTaskID()
        +HandleDispatchedTask()
    }
    class workerStore {
        <<interface>>
        +GetTaskInstance()
        +StartTaskAttempt()
        +ScheduleTaskRetry()
        +FailTaskAttempt()
        +CompleteTaskAttempt()
        +CompleteTaskAttemptAndEnqueueNextTask()
    }
    class Store {
        +CreateExecutionAndTask()
        +GetExecutionSnapshot()
        +ReplayDeadLetteredTask()
        +EnqueueDueTaskRetries()
        +ListPendingOutbox()
        +MarkOutboxDispatched()
        +RecordOutboxFailure()
    }
    class Publisher {
        +Run()
        -publishOnce()
    }
    class RedisStreams {
        +DispatchTask()
        +Consume()
        +EnsureGroup()
        -claimPending()
        -processMessages()
    }
    class Registry {
        +Get(key)
    }
    class Handler {
        <<interface>>
        +Key() string
        +Handle(ctx, task)
    }
    Router --> Service : service
    Service --> Store : store
    Worker --> workerStore : store
    Store ..|> workerStore : satisfies
    Worker --> Registry : registry
    Registry o--> Handler : indexed by key
    Worker ..> Handler : calls Handle
    Publisher --> Store : store
    Publisher --> RedisStreams : streams
    RedisStreams ..> Worker : invokes injected callback
```

`Service` and `Publisher` take a concrete `*db.Store`; `Worker` takes the smaller `workerStore` interface. `RedisStreams` receives a function callback, not a worker object. The [handler class diagram](docs/low-level-design.md#handler-strategy-and-persistence-contract) and [construction diagram](docs/low-level-design.md#dependency-injection) show the remaining wiring.

## Data model

### Core tables

- `workflow_definitions`
- `workflow_executions`
- `task_instances`
- `task_attempts`
- `outbox_events`
- `idempotency_records`

### Why they exist

- `workflow_definitions`: stores durable workflow specs
- `workflow_executions`: one row per workflow run
- `task_instances`: one row per concrete task in an execution
- `task_attempts`: preserves retry history
- `outbox_events`: stores dispatch intent before Redis publish
- `idempotency_records`: protects side effects under duplicate delivery

## Entity relationship view

```mermaid
erDiagram
    WORKFLOW_DEFINITIONS ||--o{ WORKFLOW_EXECUTIONS : "used by"
    WORKFLOW_EXECUTIONS ||--o{ TASK_INSTANCES : "contains"
    TASK_INSTANCES ||--o{ TASK_ATTEMPTS : "has"
    TASK_INSTANCES ||..o{ OUTBOX_EVENTS : "logical dispatch reference"
    TASK_INSTANCES |o--o{ IDEMPOTENCY_RECORDS : "owns"

    WORKFLOW_DEFINITIONS {
        uuid id PK
        text name UK
        int version
        text status
        jsonb definition_json
    }
    WORKFLOW_EXECUTIONS {
        uuid id PK
        uuid workflow_definition_id FK
        text status
        jsonb input_json
        jsonb output_json
        text error_text
        timestamptz started_at
        timestamptz completed_at
    }
    TASK_INSTANCES {
        uuid id PK
        uuid workflow_execution_id FK
        text task_name
        text handler_key
        text status
        text idempotency_key UK
        int attempts_total
        jsonb input_json
        jsonb output_json
        timestamptz next_run_at
        timestamptz dispatched_at
        timestamptz completed_at
        text last_error_text
    }
    TASK_ATTEMPTS {
        uuid id PK
        uuid task_instance_id FK
        int attempt_number
        text status
        jsonb output_json
        text error_text
        timestamptz started_at
        timestamptz finished_at
    }
    OUTBOX_EVENTS {
        uuid id PK
        text aggregate_type
        uuid aggregate_id
        text event_type
        jsonb payload_json
        timestamptz available_at
        timestamptz dispatched_at
        int attempt_count
        text last_error_text
    }
    IDEMPOTENCY_RECORDS {
        text handler_key PK
        text idempotency_key PK
        uuid owner_task_instance_id FK
        text status
        jsonb response_json
    }
```

Routine creation/update timestamps are omitted. The outbox's `aggregate_id` is a logical task reference with no foreign-key constraint. An idempotency record has a composite primary key `(handler_key, idempotency_key)` and a nullable owner FK; the schema allows multiple records per task. The built-in handlers normally use one record per task. A definition has a `version` column, but no version-management API.

Source: [schema](migrations/001_init.sql), [idempotency key](migrations/002_idempotency_records.sql), and [ownership migration](migrations/003_idempotency_record_ownership.sql).

## Code map

- [internal/orchestrator/service.go](internal/orchestrator/service.go): execution creation
- [internal/orchestrator/worker.go](internal/orchestrator/worker.go): runtime task handling
- [internal/outbox/publisher.go](internal/outbox/publisher.go): outbox polling and publish
- [internal/queue/redis_streams.go](internal/queue/redis_streams.go): Redis Streams delivery and reclaim
- [internal/db/store.go](internal/db/store.go): workflow and task persistence
- [internal/db/idempotency.go](internal/db/idempotency.go): idempotency reservations and stored responses

## Main flow

```mermaid
flowchart TD
    A["POST /api/executions"] --> B["Create execution, task, and outbox row in one transaction"]
    B --> C["Outbox publisher polls pending rows"]
    C --> D["Publish task message to Redis Streams"]
    D --> E["Worker reads or reclaims message"]
    E --> F["Load task state from Postgres"]
    F --> G["Start task attempt"]
    G --> H["Run handler"]

    H -->|success| I["Complete task"]
    I --> J{"next_task exists?"}
    J -->|yes| K["Create next task and new outbox row"]
    K --> C
    J -->|no| L["Mark execution succeeded"]

    H -->|retryable failure| M["Mark attempt failed and persist next_run_at"]
    M --> N["Scheduler creates retry outbox row when due"]
    N --> C

    H -->|terminal failure| O["Mark task dead_lettered and execution failed"]
    O --> P["Expose through API and dashboard"]
    P --> Q["Replay resets task and creates outbox row"]
    Q --> C
```

## Important paths

### Execution start

When an execution is triggered, the API writes:

- one `workflow_executions` row
- one entry `task_instances` row
- one `outbox_events` row

All three happen in one transaction.

### Success path

On success, the worker:

- completes the attempt
- completes the task
- either creates the next task and outbox row
- or marks the workflow execution complete

### Retry path

Retries are persisted, not slept in memory.

The worker:

- marks the attempt failed
- moves the task back to `pending`
- writes `next_run_at`

Later, a scheduler turns due retries into new outbox rows.

### Dead-letter and replay

When retries are exhausted:

- the task becomes `dead_lettered`
- the execution becomes `failed`

Replay does not bypass the engine. It resets state in Postgres and re-enters through the normal outbox path.

### Crash recovery

If a worker dies after claiming a Redis message, the message may stay pending in the consumer group. DurableFlow reclaims stale messages with `XAUTOCLAIM`.

Reclaimed messages still go through the normal worker path and still consult Postgres first.

### Idempotency

Task state alone is not enough to protect side effects.

`idempotency_records` allows a handler to:

- reserve a durable idempotency key
- store a successful response
- let the same task instance continue an unfinished reservation
- reject a different task instance while a reservation is in progress

The built-in handlers generate JSON responses; they do not call an external notification provider. Same-task ownership is not an execution lease. The [idempotency sequence](docs/low-level-design.md#idempotency) shows the cached-response and conflict paths, including these boundaries.

## Task lifecycle

```mermaid
stateDiagram-v2
    [*] --> pending
    pending --> running: worker starts attempt
    running --> running: redelivery starts another attempt
    running --> succeeded: handler succeeds
    running --> pending: retry scheduled
    running --> dead_lettered: missing handler or attempts exhausted
    dead_lettered --> pending: manual replay
    succeeded --> [*]
```

Retry waiting is `pending` with `next_run_at` set. `TaskStatusFailed` is declared in Go but is not written by the current worker paths. Attempt rows use `running`, `succeeded`, and `failed`; an abandoned attempt is not automatically closed when a new one starts.

## Current scope

What the system supports today:

- definition-driven execution
- linear `next_task` chaining
- durable retries
- dead-letter listing and replay
- worker reclaim for stale pending messages
- handler-level idempotency

What it does not support yet:

- workflow versioning
- branching or parallel graphs
- cancellation and timeouts
- richer replay audit tooling

## Short mental model

Postgres decides what should happen, Redis delivers chances to do that work, and idempotent handlers make duplicate chances safe.
