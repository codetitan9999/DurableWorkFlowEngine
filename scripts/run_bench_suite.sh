#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

DATE_DIR="${RESULT_DATE:-$(date +%F)}"
RESULT_DIR="${RESULT_DIR:-benchmarks/results/$DATE_DIR}"
BENCH="${BENCH_CMD:-go run ./cmd/bench}"

mkdir -p "$RESULT_DIR"

run_bench() {
  local label="$1"
  shift
  echo "== $label =="
  eval "$BENCH $*"
  echo
}

check_health() {
  curl -fsS http://localhost:8080/healthz >/dev/null
  curl -fsS http://localhost:8081/healthz >/dev/null
}

restore_default_api() {
  docker compose up -d --force-recreate --no-deps api >/dev/null
  sleep 5
  curl -fsS http://localhost:8080/healthz >/dev/null
}

enable_tuned_api() {
  OUTBOX_POLL_INTERVAL=100ms docker compose up -d --force-recreate --no-deps api >/dev/null
  sleep 5
  curl -fsS http://localhost:8080/healthz >/dev/null
}

start_benchmark_workers() {
  docker compose --profile benchmark up -d --scale worker-bench=2 worker-bench >/dev/null
}

stop_benchmark_workers() {
  docker compose stop worker-bench >/dev/null 2>&1 || true
}

check_health

run_bench \
  "default baseline" \
  "-scenario success-linear -executions 100 -concurrency 20 -repeat 5 -timeout 60s -label default-baseline -output-file $RESULT_DIR/default-baseline-success-linear.json"

run_bench \
  "default load 120/30" \
  "-scenario success-linear -executions 120 -concurrency 30 -repeat 3 -timeout 60s -label default-load-c30-e120 -output-file $RESULT_DIR/default-load-c30-e120.json"

run_bench \
  "default load 240/60" \
  "-scenario success-linear -executions 240 -concurrency 60 -repeat 3 -timeout 90s -label default-load-c60-e240 -output-file $RESULT_DIR/default-load-c60-e240.json"

run_bench \
  "default load 500/100" \
  "-scenario success-linear -executions 500 -concurrency 100 -repeat 2 -timeout 120s -label default-load-c100-e500 -output-file $RESULT_DIR/default-load-c100-e500.json"

run_bench \
  "default soak 100/20 repeat 30" \
  "-scenario success-linear -executions 100 -concurrency 20 -repeat 30 -timeout 60s -label soak-default-10min -output-file $RESULT_DIR/soak-default-10min.json"

enable_tuned_api

run_bench \
  "tuned load 240/60" \
  "-scenario success-linear -executions 240 -concurrency 60 -repeat 3 -timeout 60s -label tuned-outbox-100ms-c60-e240 -output-file $RESULT_DIR/tuned-outbox-100ms-c60-e240.json"

run_bench \
  "tuned load 1000/200" \
  "-scenario success-linear -executions 1000 -concurrency 200 -repeat 3 -timeout 90s -label tuned-outbox-100ms-c200-e1000 -output-file $RESULT_DIR/tuned-outbox-100ms-c200-e1000.json"

run_bench \
  "tuned load 5000/500 with 1s polling" \
  "-scenario success-linear -executions 5000 -concurrency 500 -repeat 1 -poll-interval 1s -timeout 120s -label tuned-outbox-100ms-c500-e5000-poll1s -output-file $RESULT_DIR/tuned-outbox-100ms-c500-e5000-poll1s.json"

start_benchmark_workers

run_bench \
  "tuned multi-worker load 1000/200" \
  "-scenario success-linear -executions 1000 -concurrency 200 -repeat 3 -timeout 90s -label tuned-outbox-100ms-worker3-c200-e1000 -output-file $RESULT_DIR/tuned-outbox-100ms-worker3-c200-e1000.json"

stop_benchmark_workers
restore_default_api

run_bench \
  "retry invalid input" \
  "-scenario retry-invalid-input -executions 15 -concurrency 3 -repeat 1 -timeout 120s -label mixed-retry-invalid-input -output-file $RESULT_DIR/mixed-retry-invalid-input.json"

run_bench \
  "dead-letter missing handler" \
  "-scenario dead-letter-missing-handler -executions 10 -concurrency 2 -repeat 1 -timeout 120s -label mixed-dead-letter-missing-handler -output-file $RESULT_DIR/mixed-dead-letter-missing-handler.json"

run_bench \
  "replay missing handler" \
  "-scenario replay-missing-handler -executions 5 -concurrency 1 -repeat 1 -timeout 120s -label mixed-replay-missing-handler -output-file $RESULT_DIR/mixed-replay-missing-handler.json"

run_bench \
  "mixed happy-path reference" \
  "-scenario success-linear -executions 70 -concurrency 14 -repeat 1 -timeout 120s -label mixed-success-linear -output-file $RESULT_DIR/mixed-success-linear.json"

echo "Benchmark suite complete."
echo "Results written to $RESULT_DIR"
