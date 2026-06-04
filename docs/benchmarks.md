# DurableFlow Benchmark Plan

This document exists to turn DurableFlow from "uses distributed-systems patterns" into "implements and measures distributed-systems behavior."

The goal is not to manufacture giant scale numbers. The goal is to produce honest, repeatable evidence for:

- end-to-end latency
- steady-state throughput
- concurrency behavior
- retry timing
- dead-letter timing
- crash-recovery semantics

For a personal workflow engine, those measurements are more credible than inflated TPS claims.

## What is already true without benchmarking

These repo facts are already safe to use:

- `8` services in the local stack: `api`, `worker`, `web`, `postgres`, `redis`, `otel-collector`, `prometheus`, `grafana`
- `6` core workflow/state tables: `workflow_definitions`, `workflow_executions`, `task_instances`, `task_attempts`, `outbox_events`, `idempotency_records`
- `24` focused test cases across orchestration, handlers, queue decoding/recovery helpers, and API-path validation
- a workflow engine with explicit at-least-once delivery, durable retries, dead-letter replay, stale-message reclaim, and handler-level idempotency

These are good architecture and correctness signals, but they are not enough to claim performance or scale.

## Benchmark harness

The repo includes a small benchmark runner at [cmd/bench/main.go](../cmd/bench/main.go).

It does three things:

1. creates a benchmark workflow definition through the real API
2. triggers many executions concurrently
3. polls execution snapshots until terminal state, then computes summary statistics

It reports two latency views:

- observed latency: request trigger until the harness sees a terminal snapshot
- reported engine latency: `started_at` to `completed_at` from the execution snapshot

The reported engine latency is the number to prefer in resume bullets because it removes most poll-interval noise.

## Supported scenarios

### 1. `success-linear`

Creates a two-step workflow:

- `validate-order` -> `sample.echo`
- `send-confirmation` -> `notifications.send`

Use this scenario to measure:

- happy-path latency
- steady-state throughput
- behavior under concurrent successful executions

### 2. `retry-invalid-input`

Creates a one-step workflow using `sample.echo` and intentionally passes invalid JSON shape (`123`) so the handler fails, schedules persisted retries, and eventually dead-letters after exhausting `max_attempts`.

Use this scenario to measure:

- retry timing
- attempts-to-terminal-failure behavior
- dead-letter latency under persisted backoff

### 3. `dead-letter-missing-handler`

Creates a one-step workflow with a missing handler and configurable retry settings.

Use this scenario to measure:

- immediate terminal-failure latency
- dead-letter visibility without retry overhead

### 4. `replay-missing-handler`

Creates a missing-handler workflow, waits for the dead-letter transition, triggers replay through the API, then measures the end-to-end time until the task reaches terminal failure again.

Use this scenario to measure:

- replay-path overhead
- whether replay truly re-enters the normal dispatch path

### 5. `success-deep-chain`

Creates a longer linear workflow chain with configurable `chain-length`.

Use this scenario to measure:

- how latency scales with workflow depth
- whether task-attempt counts stay aligned with chain length

## Recommended measurement runs

### A. Baseline happy path

Run:

```bash
go run ./cmd/bench \
  -scenario success-linear \
  -executions 25 \
  -concurrency 5
```

Capture:

- throughput in executions/sec
- reported latency `avg`, `p50`, `p95`
- attempts per execution

This gives the first honest performance line for the project.

### B. Higher concurrency happy path

Run:

```bash
go run ./cmd/bench \
  -scenario success-linear \
  -executions 100 \
  -concurrency 20
```

Capture:

- throughput
- reported latency `p95`
- whether attempts stay at `2` per execution for the two-step workflow

This tells you whether the system still behaves cleanly when many executions are active at once.

### C. Retry and dead-letter timing

Run:

```bash
go run ./cmd/bench \
  -scenario retry-invalid-input \
  -executions 20 \
  -concurrency 5 \
  -max-attempts 3 \
  -backoff-seconds 1
```

Capture:

- attempts per execution
- reported latency to terminal failure
- whether terminal state is consistently `failed`

This gives you evidence that retries are persisted and dead-lettering is deterministic.

### D. Immediate dead-letter timing

Run:

```bash
go run ./cmd/bench \
  -scenario dead-letter-missing-handler \
  -executions 10 \
  -concurrency 3
```

Capture:

- terminal-failure latency
- attempts per execution

This isolates dead-letter overhead from retry scheduling.

### E. Replay timing

Run:

```bash
go run ./cmd/bench \
  -scenario replay-missing-handler \
  -executions 5 \
  -concurrency 2
```

Capture:

- latency from first run through replay and second terminal failure
- attempts per execution after replay

This gives you a measurable replay-path result instead of only a functional demo.

### F. Worker-scaling comparison

Start one run with the default single worker, then scale workers and repeat:

```bash
docker compose up -d
docker compose up -d --scale worker=3
```

Then rerun scenario B and compare:

- throughput change
- `p95` latency change

This gives you an honest statement about how the current design responds to additional workers, even in a local environment.

### G. Crash-recovery validation

This is the most important non-throughput check.

Suggested flow:

1. start a higher-concurrency `success-linear` run
2. kill the worker container while messages are in flight
3. restart or scale the worker back up
4. confirm reclaimed work completes without duplicate side effects
5. inspect:
   - execution snapshots
   - dead-letter list
   - Prometheus task metrics

This scenario does not need a giant number attached to it. It gives you a stronger claim:

- work was recovered after consumer failure
- reclaimed messages still respected task-state checks and idempotency boundaries

## How to read the output

Example:

```text
Scenario: success-linear
Executions: 100
Concurrency: 20
Throughput: 18.42 executions/sec

Reported engine latency (started_at -> completed_at)
  avg:   742ms
  p50:   701ms
  p95:   1.12s
```

Interpretation:

- throughput tells you how many whole workflow executions completed per second
- reported latency tells you how long the workflow engine itself took, excluding most snapshot-poll delay
- attempts per execution tells you whether retries or duplicate processing happened

For `success-linear`, attempts per execution should usually stay near `2` because there are two tasks and one successful attempt per task.

For `retry-dead-letter`, attempts per execution should track the configured retry count.

## Metrics to inspect alongside benchmark runs

Prometheus and Grafana should be used together with the benchmark harness.

Useful Prometheus queries:

```promql
sum(rate(durableflow_workflow_executions_created_total[5m])) by (service)
```

```promql
sum(rate(durableflow_dispatch_events_total[5m])) by (service, status)
```

```promql
sum(rate(durableflow_tasks_processed_total[5m])) by (service, handler, status)
```

These help validate:

- execution creation rate
- outbox publish behavior
- worker success/retry/failure mix

## Resume-safe phrasing after results exist

Once you have real measurements, prefer wording like this:

- Built a fault-tolerant workflow engine in Go and measured `X exec/s` at `Y` concurrent executions with `p95` completion latency of `Z` in a local `8`-service stack.
- Validated persisted retry and dead-letter semantics across `N` benchmarked executions, with terminal failures reaching a stable dead-letter state after `A` attempts and `B` seconds of configured backoff.
- Verified crash-safe recovery under at-least-once delivery by reclaiming stale pending messages and preserving duplicate-safe side effects through task-owned idempotency records.

Avoid wording like:

- processed millions of workflows
- production-grade scale
- exactly-once delivery

Those claims are not what this repo currently proves.

## Next observability upgrades

The current benchmark harness is enough to get honest first numbers. The next instrumentation upgrades that would make the project stronger are:

- HTTP duration histogram
- task processing duration histogram
- retry scheduled counter
- dead-letter counter
- replay counter
- reclaimed-message counter

Those would let future benchmark runs produce even stronger evidence with less manual interpretation.
