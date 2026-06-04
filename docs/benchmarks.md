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

## Measured local results

The following results were captured on `2026-06-05` against the local Docker-based stack. These are local validation numbers, not production-scale claims.

### Single-worker baseline

| Scenario | Workload | Throughput | Reported latency | Attempts/execution |
| --- | --- | ---: | --- | ---: |
| `success-linear` | `20` exec, concurrency `5` | `1.30 exec/s` | avg `3.72s`, p95 `3.88s` | `2` |
| `success-linear` | `60` exec, concurrency `15` | `3.89 exec/s` | avg `3.73s`, p95 `3.89s` | `2` |
| `success-deep-chain` (`5` steps) | `20` exec, concurrency `5` | `0.51 exec/s` | avg `9.53s`, p95 `9.84s` | `5` |
| `success-deep-chain` (`10` steps) | `10` exec, concurrency `3` | `0.13 exec/s` | avg `19.73s`, p95 `19.99s` | `10` |
| `retry-invalid-input` | `6` exec, concurrency `3`, `3` attempts, `1s` backoff | `0.53 exec/s` | avg `5.53s`, p95 `5.93s` | `3` |
| `dead-letter-missing-handler` | `10` exec, concurrency `3` | `1.38 exec/s` | avg `1.58s`, p95 `1.83s` | `1` |
| `replay-missing-handler` | `5` exec, concurrency `2` | `0.43 exec/s` | avg `3.77s`, p95 `3.93s` | `2` |

### Default-shape saturation results

The most useful default-path result came from pushing the same `2-step` happy-path workflow harder:

| Scenario | Workload | Throughput | Reported latency |
| --- | --- | ---: | --- |
| `success-linear` | `120` exec, concurrency `30` | `4.99 exec/s` | avg `5.79s`, p95 `5.98s` |
| `success-linear` | `240` exec, concurrency `60` | `4.99 exec/s` | avg `11.47s`, p95 `11.99s` |

Interpretation:

- with the default `OUTBOX_POLL_INTERVAL=2s`, the system plateaued almost exactly at `~5 exec/s`
- increasing workload beyond that point did not increase throughput further
- it did increase queueing latency substantially

This matches the architecture:

- the outbox publisher drains up to `20` pending rows per poll
- the default poll interval is `2s`
- a `2-step` workflow needs two dispatches per execution

That implies a theoretical ceiling near:

- `20 outbox rows / 2s = 10 dispatches/s`
- `10 dispatches/s / 2 tasks per execution = ~5 exec/s`

The measured plateau matched that model closely, which makes this one of the strongest "measured system understanding" results in the project.

### Crash-recovery validation

One benchmark run intentionally stopped the only worker mid-flight, waited long enough for Redis reclaim to matter, then restarted the worker.

Result:

- scenario: `success-linear`
- workload: `20` executions, concurrency `5`
- final outcome: all `20` executions still succeeded
- throughput dropped to `0.29 exec/s`
- reported latency avg rose to `16.77s`
- reported latency p95 rose to `55.82s`
- attempts per execution remained `2`

What this shows:

- work was delayed significantly by consumer failure and reclaim timing
- work was not lost
- execution semantics stayed correct after recovery

### Multi-worker comparison

Two extra worker processes were started against the same Redis consumer group and the `success-linear` benchmark was rerun.

Observed result:

- `success-linear`, `60` executions, concurrency `15`
- throughput stayed effectively flat at `3.88 exec/s`
- latency stayed effectively flat as well

Interpretation:

- the current bottleneck is likely upstream of worker parallelism
- the main limiting factor in this local setup appears closer to outbox publish cadence and end-to-end orchestration timing than raw worker count

### Tuned publisher comparison

To confirm whether the default ceiling was architectural or just configuration-driven, the API was rerun once with:

- `OUTBOX_POLL_INTERVAL=100ms`

The repo code did not change for this comparison. Only the runtime poll interval changed.

| Scenario | Workload | Throughput | Reported latency |
| --- | --- | ---: | --- |
| `success-linear` tuned | `240` exec, concurrency `60` | `97.30 exec/s` | avg `462ms`, p95 `524ms` |
| `success-linear` tuned | `1000` exec, concurrency `200` | `98.92 exec/s` | avg `1.80s`, p95 `1.97s` |
| `success-linear` tuned | `2000` exec, concurrency `400` | `99.06 exec/s` | avg `3.70s`, p95 `3.96s` |
| `retry-invalid-input` tuned | `6` exec, concurrency `3`, `3` attempts, `1s` backoff | `1.25 exec/s` | avg `2.27s`, p95 `2.28s` |

Interpretation:

- the `~5 exec/s` default plateau was mostly publisher-cadence bound
- reducing the outbox poll interval by `20x` increased measured happy-path throughput by roughly `20x`
- the next observed ceiling under local load was roughly `~99 exec/s` for the `2-step` workflow
- persisted retries also became much closer to the configured backoff floor once outbox cadence was no longer the dominant delay

This is a strong result because it shows:

- the system was benchmarked enough to expose its first bottleneck
- the bottleneck hypothesis was tested with a controlled configuration change
- the resulting throughput shift matched the architectural expectation

### Important benchmarking note

Benchmarking exposed a real bug in the retry scheduler path: due retries could become stuck because `EnqueueDueTaskRetries` kept a query cursor open and then attempted additional `Exec` calls inside the same transaction, causing repeated `conn busy` failures in the outbox loop.

That issue was fixed before the persisted-retry benchmark above was recorded. This is one of the strongest kinds of benchmark evidence: the benchmark suite did not just produce numbers, it surfaced and helped validate a real correctness bug in the retry path.
