# DurableFlow Learning Roadmap

This file is the implementation backlog for the project.

I keep it for two reasons:

- to keep the project honest about what is still unfinished
- to make the roadmap visible instead of leaving the next steps vague

The phases below are ordered from easiest to hardest. Each one builds on the current codebase and pushes the project a little closer to a real workflow engine.

## Status summary

Completed foundation:

- local multi-service development stack
- schema and migrations
- API and worker skeletons
- outbox-based happy path
- sample handler processing
- dashboard-based manual validation
- starter metrics and tracing bootstrap
- definition-driven entry task creation
- execution snapshots with task attempts
- retry policy, backoff scheduling, and outbox-based retry redispatch
- linear task chaining through `next_task`
- Postgres-backed dead-lettered task listing and replay
- Redis consumer-group reclaim for stale pending deliveries

Still to build:

- richer graph execution beyond linear `next_task`
- workflow versioning

The phases below describe that remaining work in the order I plan to tackle it.

## Phase 1: Replace hardcoded task creation with definition parsing

Status: completed

### Build

Read the stored workflow definition and create task instances from it instead of always creating one hardcoded sample task.

### Why it matters

This is the first real step from “demo pipeline” to “workflow engine.” It teaches you how orchestration logic should depend on durable definitions rather than ad hoc code paths.

### Learn first

- JSON schema design
- Graph modeling basics
- Validation of user-defined configs
- Transaction boundaries in orchestrators

### Files to touch

- [internal/orchestrator/service.go](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/internal/orchestrator/service.go)
- [internal/db/store.go](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/internal/db/store.go)
- [internal/domain/models.go](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/internal/domain/models.go)

### Acceptance criteria

- A workflow definition can describe at least multiple tasks
- Triggering an execution creates task rows based on the definition
- The sample workflow still works through the new definition path
- Invalid definitions are rejected with a clear API error

### Optional hints

- Start with a very small definition format: list of tasks plus one entry task
- Do not implement branching yet unless you want the stretch challenge

## Phase 2: Add execution-read APIs that expose attempts and timelines

Status: completed

### Build

Expand the read side so the API can return task attempts, timestamps, and failure details for an execution.

### Why it matters

Good engines are debuggable. This phase is where observability starts to show up in the data model and read APIs, not just in metrics dashboards.

### Learn first

- Read models
- API response shaping
- Query performance and indexing

### Files to touch

- [internal/db/store.go](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/internal/db/store.go)
- [internal/httpapi/router.go](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/internal/httpapi/router.go)
- [apps/web/src/App.tsx](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/apps/web/src/App.tsx)

### Acceptance criteria

- Execution responses include attempts and failure context
- The dashboard shell can show attempt history
- Query paths stay simple and understandable

### Optional hints

- Resist premature CQRS complexity
- A richer execution snapshot endpoint is enough for now

## Phase 3: Implement retry policy with backoff

Status: completed

### Build

Add retry-aware failure handling so task failures create another attempt later instead of always failing terminally.

### Why it matters

Retries are where workflow engines become operationally useful, and where state-modeling mistakes become visible quickly.

### Learn first

- Exponential backoff
- Retry budgets and max attempts
- Failure classification
- Idempotency requirements under retries

### Files to touch

- [internal/orchestrator/worker.go](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/internal/orchestrator/worker.go)
- [internal/db/store.go](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/internal/db/store.go)
- [internal/domain/models.go](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/internal/domain/models.go)
- [migrations/001_init.sql](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/migrations/001_init.sql) or a new migration

### Acceptance criteria

- A failed task can retry up to a configured limit
- Retry timing is persisted in Postgres
- Each attempt is recorded in `task_attempts`
- Final terminal failure still persists clearly

### Optional hints

- Add a retry policy to the workflow definition or task model
- Persist the next eligible run time rather than sleeping in the worker

## Phase 4: Build a delayed-task scheduler

Status: completed

### Build

Create a scheduler loop that scans Postgres for tasks whose `next_run_at` has arrived and writes dispatch events into the outbox.

### Why it matters

This phase forces you to think like a durable system: time-based state changes should be data-driven and restart-safe.

### Learn first

- Polling schedulers
- Leases and concurrency control
- “Ready queue” patterns
- Time-based orchestration

### Files to touch

- [internal/outbox](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/internal/outbox)
- [internal/db/store.go](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/internal/db/store.go)
- [apps/api/main.go](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/apps/api/main.go) or a dedicated scheduler service

### Acceptance criteria

- Retriable tasks are redispatched when their scheduled time arrives
- Restarting the scheduler does not lose delayed work
- Dispatch intent still flows through Postgres first

### Optional hints

- Keep the scheduler small at first
- Use row-level ownership or careful updates to avoid duplicate scheduling

## Phase 5: Add dead-letter queue behavior

Status: completed

### Build

Introduce terminal routing for tasks that exceed retry policy or fail with non-retriable errors.

### Why it matters

DLQs are part of making failure modes explicit and operable instead of invisible.

### Learn first

- Terminal vs transient failures
- Failure triage workflows
- Operational replay patterns

### Files to touch

- [internal/db/store.go](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/internal/db/store.go)
- [internal/orchestrator/worker.go](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/internal/orchestrator/worker.go)
- [ARCHITECTURE.md](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/ARCHITECTURE.md)

### Acceptance criteria

- Terminally failed tasks are clearly marked
- DLQ state is queryable from Postgres
- You can distinguish retry exhaustion from hard validation failures
- A dead-lettered task can be replayed manually through the same outbox path

### Optional hints

- You do not need a second Redis stream yet
- A Postgres-backed DLQ view or table is a clean first version

## Phase 6: Harden consumer crash recovery

Status: completed

### Build

Handle worker crashes and pending Redis consumer-group entries by reconciling Redis delivery state with Postgres task state.

### Why it matters

This is where durable systems become real. It teaches the difference between “queued,” “in progress,” and “durably recoverable.”

### Learn first

- Redis Streams pending entries
- Consumer groups and message claiming
- Heartbeats and leases
- Recovery reconciliation

### Files to touch

- [internal/queue/redis_streams.go](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/internal/queue/redis_streams.go)
- [internal/orchestrator/worker.go](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/internal/orchestrator/worker.go)
- [internal/db/store.go](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/internal/db/store.go)

### Acceptance criteria

- Stuck pending messages can be reclaimed
- Duplicate reprocessing does not corrupt durable state
- Crash recovery behavior is documented clearly

### Optional hints

- Start by inspecting the Redis pending-entry list
- Postgres should decide whether reclaimed work is still valid to run

## Phase 7: Strengthen idempotency guarantees

Status: completed

### Build

Move from “best effort task-level idempotency” to explicit side-effect idempotency boundaries.

### Why it matters

At-least-once delivery only works in practice when side effects are safe under duplication.

### Learn first

- Idempotency keys
- Deduplication stores
- External API write semantics
- Exactly-once myths

### Files to touch

- [internal/handlers](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/internal/handlers)
- [internal/domain/models.go](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/internal/domain/models.go)
- [migrations](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/migrations)

### Acceptance criteria

- A handler can safely resume or repeat without duplicating its side effect
- Idempotency strategy is explicit in code and docs
- Failure and retry paths preserve the same idempotency contract
- A dedicated idempotency table persists in-progress reservations and completed responses
- The same task instance can safely resume an in-progress reservation after retries or crash recovery

### Optional hints

- Think in terms of “what external thing could be done twice?”
- A dedicated idempotency table is often clearer than implicit logic

## Phase 8: Add workflow versioning

### Build

Allow multiple versions of a workflow definition and ensure executions bind to a specific version immutably.

### Why it matters

Workflow systems need stable historical behavior. Versioning teaches immutability, migration, and compatibility tradeoffs.

### Learn first

- Immutable definitions
- Compatibility and rollout strategies
- Metadata vs behavior versioning

### Files to touch

- [internal/db/store.go](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/internal/db/store.go)
- [internal/orchestrator/service.go](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/internal/orchestrator/service.go)
- [migrations](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/migrations)

### Acceptance criteria

- New workflow versions can be created without mutating old ones
- Executions always reference the intended version
- Reads make workflow version visible

### Optional hints

- Decide whether `name` stays unique or `name + version` becomes the unique key

## Phase 9: Improve observability for operations and debugging

### Build

Expand metrics, traces, and dashboard panels to show queue lag, attempt counts, retry outcomes, and failure rates.

### Why it matters

You cannot operate workflow infrastructure blindly. This phase helps you connect internal state transitions to operational signals.

### Learn first

- RED metrics
- Queue lag and throughput
- Trace spans across async boundaries
- Useful dashboards vs noisy dashboards

### Files to touch

- [internal/telemetry/telemetry.go](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/internal/telemetry/telemetry.go)
- [deployments/observability](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/deployments/observability)
- [apps/web](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/apps/web)

### Acceptance criteria

- You can answer “what is failing, how often, and where?”
- Async spans are traceable across API, outbox, and worker stages
- Grafana shows at least one useful queue/attempt dashboard

### Optional hints

- Start with counters and timestamps you already persist
- Derive queue lag from durable timestamps before chasing perfect metrics

## Phase 10: Add an AI assist layer last

### Build

After the engine is operationally trustworthy, add AI helpers for workflow generation and failure summarization.

### Why it matters

AI can accelerate authoring and debugging, but only after the core system is deterministic and observable.

### Learn first

- Structured generation
- Guardrails for user-defined automation
- Failure summarization from traces and DB state

### Files to touch

- [apps/api](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/apps/api)
- [apps/web](/Users/sumanth/Desktop/CodexApps/DurableWorkFlow/apps/web)
- New AI-specific packages once the engine is stable

### Acceptance criteria

- AI suggestions never become the source of truth
- The system can explain failures from durable state, not from guesses
- AI features are optional layers on top of a reliable engine

### Optional hints

- Start with read-only assistance before generation that mutates definitions
