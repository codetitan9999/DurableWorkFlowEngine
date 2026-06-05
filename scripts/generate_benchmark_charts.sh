#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

RESULT_DIR="${1:-benchmarks/results/$(date +%F)}"
OUTPUT_FILE="${2:-$RESULT_DIR/charts.md}"

require_file() {
  local file="$1"
  if [[ ! -f "$file" ]]; then
    echo "missing required artifact: $file" >&2
    exit 1
  fi
}

summary_numbers() {
  local file="$1"
  jq -r '[.aggregate.throughput_per_second.avg, (.aggregate.reported_latency_p95.avg / 1000000000)] | @tsv' "$file"
}

bucket_series() {
  local file="$1"
  jq -r '
    .results
    | [.[0:5], .[5:10], .[10:15], .[15:20], .[20:25], .[25:30]]
    | map((map(.throughput_per_second) | add / length * 100 | round / 100))
    | join(", ")
  ' "$file"
}

bucket_p95_series() {
  local file="$1"
  jq -r '
    .results
    | [.[0:5], .[5:10], .[10:15], .[15:20], .[20:25], .[25:30]]
    | map((map(.reported_latency.p95) | add / length / 1000000000 * 100 | round / 100))
    | join(", ")
  ' "$file"
}

require_file "$RESULT_DIR/default-baseline-success-linear.json"
require_file "$RESULT_DIR/default-load-c30-e120.json"
require_file "$RESULT_DIR/default-load-c60-e240.json"
require_file "$RESULT_DIR/default-load-c100-e500.json"
require_file "$RESULT_DIR/tuned-outbox-100ms-c200-e1000.json"
require_file "$RESULT_DIR/tuned-outbox-100ms-worker3-c200-e1000.json"
require_file "$RESULT_DIR/soak-default-10min.json"
require_file "$RESULT_DIR/failure-single-worker-restart-forced.json"
require_file "$RESULT_DIR/tuned-partial-worker-loss.json"
require_file "$RESULT_DIR/mixed-success-linear.json"

read -r base_tput base_p95 <<<"$(summary_numbers "$RESULT_DIR/default-baseline-success-linear.json")"
read -r load30_tput load30_p95 <<<"$(summary_numbers "$RESULT_DIR/default-load-c30-e120.json")"
read -r load60_tput load60_p95 <<<"$(summary_numbers "$RESULT_DIR/default-load-c60-e240.json")"
read -r load100_tput load100_p95 <<<"$(summary_numbers "$RESULT_DIR/default-load-c100-e500.json")"
read -r tuned_tput tuned_p95 <<<"$(summary_numbers "$RESULT_DIR/tuned-outbox-100ms-c200-e1000.json")"
read -r tuned3_tput tuned3_p95 <<<"$(summary_numbers "$RESULT_DIR/tuned-outbox-100ms-worker3-c200-e1000.json")"
read -r fail_tput fail_p95 <<<"$(summary_numbers "$RESULT_DIR/failure-single-worker-restart-forced.json")"
read -r partial_tput partial_p95 <<<"$(summary_numbers "$RESULT_DIR/tuned-partial-worker-loss.json")"
read -r mixed_tput mixed_p95 <<<"$(summary_numbers "$RESULT_DIR/mixed-success-linear.json")"

soak_tput_series="$(bucket_series "$RESULT_DIR/soak-default-10min.json")"
soak_p95_series="$(bucket_p95_series "$RESULT_DIR/soak-default-10min.json")"

cat > "$OUTPUT_FILE" <<EOF
# Benchmark Charts

Source directory: \`$RESULT_DIR\`

## Throughput comparison

\`\`\`mermaid
xychart-beta
    title "Throughput by benchmark shape"
    x-axis ["D 100/20", "D 120/30", "D 240/60", "D 500/100", "T 1000/200", "T 3W 1000/200", "Mixed success"]
    y-axis "exec/s" 0 --> 110
    bar [$base_tput, $load30_tput, $load60_tput, $load100_tput, $tuned_tput, $tuned3_tput, $mixed_tput]
\`\`\`

## Tail latency comparison

\`\`\`mermaid
xychart-beta
    title "Reported p95 latency by benchmark shape"
    x-axis ["D 100/20", "D 120/30", "D 240/60", "D 500/100", "T 1000/200", "T 3W 1000/200", "Mixed success"]
    y-axis "seconds" 0 --> 25
    bar [$base_p95, $load30_p95, $load60_p95, $load100_p95, $tuned_p95, $tuned3_p95, $mixed_p95]
\`\`\`

## Soak drift

\`\`\`mermaid
xychart-beta
    title "Soak throughput by 5-run bucket"
    x-axis ["1-5", "6-10", "11-15", "16-20", "21-25", "26-30"]
    y-axis "exec/s" 0 --> 6
    line [$soak_tput_series]
\`\`\`

\`\`\`mermaid
xychart-beta
    title "Soak reported p95 by 5-run bucket"
    x-axis ["1-5", "6-10", "11-15", "16-20", "21-25", "26-30"]
    y-axis "seconds" 0 --> 5
    line [$soak_p95_series]
\`\`\`

## Failure and recovery comparison

\`\`\`mermaid
xychart-beta
    title "Recovery behavior under worker loss"
    x-axis ["Single worker outage", "Partial loss with 3 workers"]
    y-axis "seconds" 0 --> 40
    bar [$fail_p95, $partial_p95]
\`\`\`

\`\`\`mermaid
xychart-beta
    title "Throughput under worker loss"
    x-axis ["Single worker outage", "Partial loss with 3 workers"]
    y-axis "exec/s" 0 --> 110
    bar [$fail_tput, $partial_tput]
\`\`\`
EOF

echo "Wrote $OUTPUT_FILE"
