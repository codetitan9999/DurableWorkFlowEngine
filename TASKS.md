# DurableFlow Roadmap

This file is the project roadmap, but it is also a record of how the system was built.

I do not want this repository to look like a giant “future ideas” list with no proof behind it. The sections below separate what is already real from what is still intentionally unfinished.

## Current state

DurableFlow is already a serious distributed-systems project, not just a scaffold.

The current build includes:

- durable workflow definitions and executions in Postgres
- transactional outbox-based dispatch
- Redis Streams worker consumption
- retry scheduling with persisted `next_run_at`
- linear multi-step workflow chaining
- dead-lettered task listing and replay
- stale pending-message reclaim for crashed workers
- handler-level idempotency with durable reservations and stored responses
- a dashboard for execution inspection and dead-letter recovery

The main things still missing are:

- workflow versioning
- richer graph execution beyond linear `next_task`
- deeper observability and operator tooling

## Completed milestones

### Phase 1: definition-driven execution

The project started by moving from hardcoded task creation to stored workflow definitions with validation.

Why it mattered:

- it turned the project into a workflow engine instead of a fixed async demo
- it created the seam for later versioning and graph expansion

### Phase 2: execution snapshots and attempt visibility

Execution reads were expanded to include task attempts, timestamps, and failure details.

Why it mattered:

- it made retries and failures explainable
- it forced the data model to support observability instead of treating it as an afterthought

### Phase 3: retries with persisted scheduling

Task failures stopped being immediately terminal. Retry policy and backoff were added, with future execution stored in `task_instances.next_run_at`.

Why it mattered:

- retry intent survives restarts
- the system became operationally useful, not just functionally correct on the happy path

### Phase 4: delayed redispatch through the outbox

A scheduler loop was added to materialize retryable tasks back into `outbox_events` when their retry time arrives.

Why it mattered:

- initial execution and delayed retries now share the same durable dispatch path
- retry timing is driven by persisted state instead of in-memory sleeps

### Phase 5: dead-letter handling and replay

Tasks that exhaust retries are now marked `dead_lettered`, exposed through the API and dashboard, and replayable manually.

Why it mattered:

- failure is no longer just recorded; it is operable
- the system has a clear answer for “what needs attention?”

### Phase 6: crash recovery for stale pending deliveries

Redis Streams consumer-group recovery was added with `XAUTOCLAIM` so stale pending messages can be reclaimed safely after worker crashes.

Why it mattered:

- abandoned queue deliveries no longer stay stuck forever
- the project now models one of the most important real-world queue recovery problems

### Phase 7: explicit idempotency guarantees

Handler-level idempotency was strengthened with `idempotency_records`, task-owned reservations, stored successful responses, and safe resume behavior for the same task instance.

Why it mattered:

- duplicate delivery is now treated as a first-class correctness problem
- crash recovery and retries can coexist with side-effect safety

## Remaining roadmap

The remaining work is no longer “make it distributed.” That part is already real. The remaining work is about making the engine broader, easier to evolve, and easier to operate.

### Phase 8: workflow versioning

Goal:

- allow multiple immutable versions of the same workflow definition
- bind each execution to the exact version it started with

Why it matters:

- workflow engines need historical stability
- changing a definition in place is dangerous once real executions exist

What this phase would likely change:

- versioned workflow-definition storage
- execution reads that surface version metadata
- stricter validation and creation rules around definition updates

### Phase 9: richer graph execution

Goal:

- move beyond a linear `next_task` chain
- support branching or parallel progression safely

Why it matters:

- linear workflows prove the state machine shape, but they do not cover the full orchestration problem
- this is the phase where DurableFlow would start resembling a more general workflow engine

What this phase would likely change:

- task dependency modeling
- readiness checks for runnable tasks
- multiple task creation paths after one task completes

### Phase 10: stronger observability and operator tooling

Goal:

- make it easier to operate the system under load or failure

Why it matters:

- the project already persists rich state, but better dashboards, metrics, and audit flows would make that state more actionable

What this phase would likely change:

- richer Prometheus metrics
- more useful Grafana dashboards
- better replay/audit history in the UI
- clearer queue lag and retry trend visibility

## Recommended next order

Recommended sequence:

1. workflow versioning
2. richer graph execution
3. deeper observability

That order is intentional.

Versioning should come before more expressive workflow graphs because graph complexity is harder to manage if the definition model itself is still mutable in place.

## Roadmap summary

The roadmap is short now because most of the hard foundational work is already done.

What remains is not “add retries” or “add dead letters.” Those pieces already exist.

What remains is the next tier of workflow-engine concerns:

- immutable definition evolution
- broader orchestration semantics
- stronger operational tooling

That is exactly where I want the project to be at this stage.
