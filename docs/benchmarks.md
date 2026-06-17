# DurableFlow Benchmarks

This document keeps the benchmark story short: what was measured, what the current numbers mean, and how to rerun the suite.

## What was measured

The benchmark harness measures:

- end-to-end workflow throughput
- reported engine latency from execution `started_at` to `completed_at`
- retry and dead-letter behavior
- worker interruption and reclaim behavior

It uses the real API and execution flow, not a synthetic in-process path.

## Harness

Main runner:

- [cmd/bench/main.go](../cmd/bench/main.go)

Helper scripts:

- [scripts/run_bench_suite.sh](../scripts/run_bench_suite.sh)
- [scripts/generate_benchmark_charts.sh](../scripts/generate_benchmark_charts.sh)

The harness:

1. creates a workflow through the API
2. triggers many executions concurrently
3. polls execution snapshots until terminal state
4. reports throughput and latency summaries

## Scenarios

- `success-linear`: 2-step happy path
- `success-deep-chain`: longer linear workflow
- `retry-invalid-input`: persisted retries followed by dead-letter
- `dead-letter-missing-handler`: immediate terminal failure
- `replay-missing-handler`: dead-letter followed by replay and second terminal failure

## Quick results

### Main headline

- default `OUTBOX_POLL_INTERVAL=2s`: about `~5 exec/s`
- tuned `OUTBOX_POLL_INTERVAL=100ms`: about `~99 exec/s` at `200` concurrent executions

That tells us the first default bottleneck was publisher cadence, not worker count.

### Recovery headline

- losing the only worker caused a large latency spike, but work still recovered
- interrupting `1` of `3` workers had little effect on tuned throughput

### Stability headline

- the default happy path stayed stable over repeated soak runs with little drift

## Current boundary summary

| Question | Current answer |
| --- | --- |
| First default bottleneck? | Outbox publish cadence. |
| Biggest tuning win? | Reducing `OUTBOX_POLL_INTERVAL` from `2s` to `100ms`. |
| Happy-path tuned throughput? | About `~99 exec/s` for the current 2-step workflow in the local stack. |
| Does adding more workers help immediately? | Not much for the built-in handlers at the tested tuned workload. |
| What happens if the only worker dies? | Work recovers, but latency rises sharply. |
| What happens if one worker in a larger group dies? | Remaining consumers absorb most of the load. |

## Measured local results

These numbers were captured against the local Docker-based stack and should be read that way.

### Baseline and tuned results

| Scenario | Workload | Throughput | Reported latency | Notes |
| --- | --- | ---: | --- | --- |
| `success-linear` | `120` exec, concurrency `30` | `4.99 exec/s` | avg `5.79s`, p95 `5.98s` | default poll interval |
| `success-linear` | `240` exec, concurrency `60` | `4.99 exec/s` | avg `11.47s`, p95 `11.99s` | default poll interval |
| `success-linear` tuned | `240` exec, concurrency `60` | `97.30 exec/s` | avg `462ms`, p95 `524ms` | `OUTBOX_POLL_INTERVAL=100ms` |
| `success-linear` tuned | `1000` exec, concurrency `200` | `98.92 exec/s` | avg `1.80s`, p95 `1.97s` | `OUTBOX_POLL_INTERVAL=100ms` |

### Failure-path results

| Scenario | Workload | Throughput | Reported latency | Attempts/execution |
| --- | --- | ---: | --- | ---: |
| `retry-invalid-input` | `6` exec, concurrency `3`, `3` attempts, `1s` backoff | `0.53 exec/s` | avg `5.53s`, p95 `5.93s` | `3` |
| `retry-invalid-input` tuned | `6` exec, concurrency `3`, `3` attempts, `1s` backoff | `1.25 exec/s` | avg `2.27s`, p95 `2.28s` | `3` |
| `dead-letter-missing-handler` | `10` exec, concurrency `3` | `1.38 exec/s` | avg `1.58s`, p95 `1.83s` | `1` |
| `replay-missing-handler` | `5` exec, concurrency `2` | `0.43 exec/s` | avg `3.77s`, p95 `3.93s` | `2` |

### Recovery results

| Scenario | Workload | Throughput | Reported latency | Notes |
| --- | --- | ---: | --- | --- |
| only worker interrupted | `20` exec, concurrency `5` | `0.29 exec/s` | avg `16.77s`, p95 `55.82s` | all executions still succeeded |
| `3` workers, `1` interrupted | `500` exec, concurrency `100` | `98.14 exec/s` | p95 `942ms` | near-normal throughput |

### Soak result

| Scenario | Workload | Repeat | Throughput | Reported latency |
| --- | --- | ---: | ---: | --- |
| `success-linear` | `100` exec, concurrency `20` | `30` | `5.01 exec/s` | avg p95 `3.95s` |

## How to rerun

### Single run

```bash
go run ./cmd/bench \
  -scenario success-linear \
  -executions 100 \
  -concurrency 20
```

### Repeated run with saved output

```bash
go run ./cmd/bench \
  -scenario success-linear \
  -executions 100 \
  -concurrency 20 \
  -repeat 5 \
  -output-file benchmarks/results/$(date +%F)/baseline-success-linear.json
```

### Full suite

```bash
./scripts/run_bench_suite.sh
```

### Chart generation

```bash
./scripts/generate_benchmark_charts.sh benchmarks/results/2026-06-05
```

### Multi-worker pass

```bash
docker compose --profile benchmark up -d --scale worker-bench=2 worker-bench
```

## What to inspect during runs

Useful Prometheus queries:

```promql
sum(rate(durableflow_dispatch_events_total[5m])) by (service, status)
```

```promql
sum(rate(durableflow_tasks_processed_total[5m])) by (service, handler, status)
```

```promql
histogram_quantile(0.95, sum(rate(durableflow_http_request_duration_seconds_bucket[5m])) by (le, service))
```

```promql
histogram_quantile(0.95, sum(rate(durableflow_task_processing_duration_seconds_bucket[5m])) by (le, handler, status))
```

```promql
sum(rate(durableflow_reclaimed_messages_total[5m])) by (stream, group)
```

## Caveats

- These are local Docker-based numbers, not production claims.
- The benchmark harness also loads the execution snapshot read path, not just the worker path.
- Very aggressive snapshot polling can become its own bottleneck at high load.

## Useful takeaway

The most important benchmark conclusion is simple:

DurableFlow’s first default bottleneck was outbox cadence. Once that was reduced, the local 2-step happy path sustained about `~99 exec/s`, and partial worker loss remained easy for the consumer group to absorb.
