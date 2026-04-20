#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
BUILD_DIR="${1:-${PROJECT_DIR}/build}"
BENCH_DIR="${BUILD_DIR}/benchmark"
RUST_DIR="${PROJECT_DIR}/benchmark/compare/rust"
RUST_TARGET_DIR="${RUST_DIR}/target"
CARGO_BIN="${CARGO_BIN:-/Users/gongzhijie/.rustup/toolchains/stable-aarch64-apple-darwin/bin/cargo}"
RUSTC_BIN="${RUSTC_BIN:-/Users/gongzhijie/.rustup/toolchains/stable-aarch64-apple-darwin/bin/rustc}"
CARGO_EXTRA_ARGS="${CARGO_EXTRA_ARGS:-}"

CPP_B1="${BENCH_DIR}/B1-SyncPressure"
CPP_B2="${BENCH_DIR}/B2-AsyncPressure"
RUST_B1="${RUST_TARGET_DIR}/release/rust_sync_pressure"
RUST_B2="${RUST_TARGET_DIR}/release/rust_async_pressure"

MYSQL_HOST="${GALAY_MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${GALAY_MYSQL_PORT:-3306}"
MYSQL_USER="${GALAY_MYSQL_USER:-root}"
if [ "${GALAY_MYSQL_PASSWORD+x}" = "x" ]; then
    MYSQL_PASSWORD="${GALAY_MYSQL_PASSWORD}"
else
    MYSQL_PASSWORD="password"
fi
MYSQL_DB="${GALAY_MYSQL_DB:-test}"

extract_metric() {
    local key="$1"
    awk -F': ' -v k="$key" '$1==k{print $2; exit}'
}

require_binary() {
    local path="$1"
    if [ ! -x "$path" ]; then
        echo "missing executable: $path" >&2
        exit 1
    fi
}

run_with_env() {
    GALAY_MYSQL_HOST="$MYSQL_HOST" \
    GALAY_MYSQL_PORT="$MYSQL_PORT" \
    GALAY_MYSQL_USER="$MYSQL_USER" \
    GALAY_MYSQL_PASSWORD="$MYSQL_PASSWORD" \
    GALAY_MYSQL_DB="$MYSQL_DB" \
    "$@"
}

warmup_benchmark() {
    run_with_env "$@" >/dev/null
}

timestamp() {
    date -u +"%Y-%m-%dT%H:%M:%SZ UTC"
}

print_language_metrics() {
    local label="$1"
    local qps="$2"
    local p50="$3"
    local p95="$4"
    local p99="$5"
    printf "  %-6s qps=%s  p50=%s  p95=%s  p99=%s\n" "$label" "$qps" "$p50" "$p95" "$p99"
}

print_scenario_summary() {
    local scenario_label="$1"
    local start_time="$2"
    local end_time="$3"
    local cpp_qps="$4"
    local cpp_p50="$5"
    local cpp_p95="$6"
    local cpp_p99="$7"
    local rust_qps="$8"
    local rust_p50="${9}"
    local rust_p95="${10}"
    local rust_p99="${11}"

    echo
    echo "== ${scenario_label} (start_time: ${start_time}, end_time: ${end_time}) =="
    print_language_metrics "C++" "$cpp_qps" "$cpp_p50" "$cpp_p95" "$cpp_p99"
    print_language_metrics "Rust" "$rust_qps" "$rust_p50" "$rust_p95" "$rust_p99"
    echo "resource metrics: not automated; collect manually if needed"
}

echo "== Build Rust benchmark counterparts =="
RUSTC="${RUSTC_BIN}" CARGO_TARGET_DIR="${RUST_TARGET_DIR}" \
    "${CARGO_BIN}" build --manifest-path "${RUST_DIR}/Cargo.toml" --release ${CARGO_EXTRA_ARGS}

require_binary "$CPP_B1"
require_binary "$CPP_B2"
require_binary "$RUST_B1"
require_binary "$RUST_B2"

echo
echo "== Scenario A: Sync pressure (C++ B1 vs Rust sync) =="
SCENARIO_A_START="$(timestamp)"
warmup_benchmark "$CPP_B1" \
    --clients 4 \
    --queries 200 \
    --warmup 20 \
    --timeout-sec 60 \
    --sql "SELECT 1"
CPP_B1_OUT="$(run_with_env "$CPP_B1" \
    --clients 4 \
    --queries 200 \
    --warmup 20 \
    --timeout-sec 60 \
    --sql "SELECT 1")"
printf '%s\n' "$CPP_B1_OUT"
CPP_B1_QPS="$(printf '%s\n' "$CPP_B1_OUT" | extract_metric "qps")"
CPP_B1_P50="$(printf '%s\n' "$CPP_B1_OUT" | extract_metric "p50_latency_ms")"
CPP_B1_P95="$(printf '%s\n' "$CPP_B1_OUT" | extract_metric "p95_latency_ms")"
CPP_B1_P99="$(printf '%s\n' "$CPP_B1_OUT" | extract_metric "p99_latency_ms")"

warmup_benchmark "$RUST_B1" \
    --clients 4 \
    --queries 200 \
    --warmup 20 \
    --timeout-sec 60 \
    --sql "SELECT 1"
RUST_B1_OUT="$(run_with_env "$RUST_B1" \
    --clients 4 \
    --queries 200 \
    --warmup 20 \
    --timeout-sec 60 \
    --sql "SELECT 1")"
printf '%s\n' "$RUST_B1_OUT"
RUST_B1_QPS="$(printf '%s\n' "$RUST_B1_OUT" | extract_metric "qps")"
RUST_B1_P50="$(printf '%s\n' "$RUST_B1_OUT" | extract_metric "p50_latency_ms")"
RUST_B1_P95="$(printf '%s\n' "$RUST_B1_OUT" | extract_metric "p95_latency_ms")"
RUST_B1_P99="$(printf '%s\n' "$RUST_B1_OUT" | extract_metric "p99_latency_ms")"
SCENARIO_A_END="$(timestamp)"

CPP_B1_P50="${CPP_B1_P50:-n/a}"
CPP_B1_P95="${CPP_B1_P95:-n/a}"
CPP_B1_P99="${CPP_B1_P99:-n/a}"
RUST_B1_P50="${RUST_B1_P50:-n/a}"
RUST_B1_P95="${RUST_B1_P95:-n/a}"
RUST_B1_P99="${RUST_B1_P99:-n/a}"

print_scenario_summary "Scenario A: Sync pressure (C++ B1 vs Rust sync)" "$SCENARIO_A_START" "$SCENARIO_A_END" \
    "$CPP_B1_QPS" "$CPP_B1_P50" "$CPP_B1_P95" "$CPP_B1_P99" \
    "$RUST_B1_QPS" "$RUST_B1_P50" "$RUST_B1_P95" "$RUST_B1_P99"

echo
echo "== Scenario B: Async pressure (C++ B2 vs Rust async) =="
SCENARIO_B_START="$(timestamp)"
warmup_benchmark "$CPP_B2" \
    --clients 32 \
    --queries 120 \
    --warmup 20 \
    --timeout-sec 60 \
    --mode pipeline \
    --batch-size 16 \
    --buffer-size 16384 \
    --sql "SELECT 1"
CPP_B2_OUT="$(run_with_env "$CPP_B2" \
    --clients 32 \
    --queries 120 \
    --warmup 20 \
    --timeout-sec 60 \
    --mode pipeline \
    --batch-size 16 \
    --buffer-size 16384 \
    --sql "SELECT 1")"
printf '%s\n' "$CPP_B2_OUT"
CPP_B2_QPS="$(printf '%s\n' "$CPP_B2_OUT" | extract_metric "qps")"
CPP_B2_P50="$(printf '%s\n' "$CPP_B2_OUT" | extract_metric "p50_latency_ms")"
CPP_B2_P95="$(printf '%s\n' "$CPP_B2_OUT" | extract_metric "p95_latency_ms")"
CPP_B2_P99="$(printf '%s\n' "$CPP_B2_OUT" | extract_metric "p99_latency_ms")"

warmup_benchmark "$RUST_B2" \
    --clients 32 \
    --queries 120 \
    --warmup 20 \
    --timeout-sec 60 \
    --mode pipeline \
    --batch-size 16 \
    --buffer-size 16384 \
    --sql "SELECT 1"
RUST_B2_OUT="$(run_with_env "$RUST_B2" \
    --clients 32 \
    --queries 120 \
    --warmup 20 \
    --timeout-sec 60 \
    --mode pipeline \
    --batch-size 16 \
    --buffer-size 16384 \
    --sql "SELECT 1")"
printf '%s\n' "$RUST_B2_OUT"
RUST_B2_QPS="$(printf '%s\n' "$RUST_B2_OUT" | extract_metric "qps")"
RUST_B2_P50="$(printf '%s\n' "$RUST_B2_OUT" | extract_metric "p50_latency_ms")"
RUST_B2_P95="$(printf '%s\n' "$RUST_B2_OUT" | extract_metric "p95_latency_ms")"
RUST_B2_P99="$(printf '%s\n' "$RUST_B2_OUT" | extract_metric "p99_latency_ms")"
SCENARIO_B_END="$(timestamp)"

CPP_B2_P50="${CPP_B2_P50:-n/a}"
CPP_B2_P95="${CPP_B2_P95:-n/a}"
CPP_B2_P99="${CPP_B2_P99:-n/a}"
RUST_B2_P50="${RUST_B2_P50:-n/a}"
RUST_B2_P95="${RUST_B2_P95:-n/a}"
RUST_B2_P99="${RUST_B2_P99:-n/a}"

print_scenario_summary "Scenario B: Async pressure (C++ B2 vs Rust async)" "$SCENARIO_B_START" "$SCENARIO_B_END" \
    "$CPP_B2_QPS" "$CPP_B2_P50" "$CPP_B2_P95" "$CPP_B2_P99" \
    "$RUST_B2_QPS" "$RUST_B2_P50" "$RUST_B2_P95" "$RUST_B2_P99"

if [ -z "$CPP_B1_QPS" ] || [ -z "$RUST_B1_QPS" ] || [ -z "$CPP_B2_QPS" ] || [ -z "$RUST_B2_QPS" ]; then
    echo "failed to parse qps from one or more benchmark outputs" >&2
    exit 1
fi

echo
echo "== Summary (throughput + latency) =="
printf "sync:  C++ qps=%s  p50=%s  p95=%s  p99=%s | Rust qps=%s  p50=%s  p95=%s  p99=%s\n" \
    "$CPP_B1_QPS" "$CPP_B1_P50" "$CPP_B1_P95" "$CPP_B1_P99" \
    "$RUST_B1_QPS" "$RUST_B1_P50" "$RUST_B1_P95" "$RUST_B1_P99"
printf "async: C++ qps=%s  p50=%s  p95=%s  p99=%s | Rust qps=%s  p50=%s  p95=%s  p99=%s\n" \
    "$CPP_B2_QPS" "$CPP_B2_P50" "$CPP_B2_P95" "$CPP_B2_P99" \
    "$RUST_B2_QPS" "$RUST_B2_P50" "$RUST_B2_P95" "$RUST_B2_P99"
