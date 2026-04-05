#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
BUILD_DIR="${1:-${PROJECT_DIR}/build}"
BENCH_DIR="${BUILD_DIR}/benchmark"
RUST_DIR="${PROJECT_DIR}/benchmark/compare/rust"
RUST_TARGET_DIR="${RUST_DIR}/target"

CPP_B1="${BENCH_DIR}/B1-SyncPressure"
CPP_B2="${BENCH_DIR}/B2-AsyncPressure"
RUST_B1="${RUST_TARGET_DIR}/release/rust_sync_pressure"
RUST_B2="${RUST_TARGET_DIR}/release/rust_async_pressure"

MYSQL_HOST="${GALAY_MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${GALAY_MYSQL_PORT:-3306}"
MYSQL_USER="${GALAY_MYSQL_USER:-root}"
MYSQL_PASSWORD="${GALAY_MYSQL_PASSWORD:-password}"
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

echo "== Build Rust benchmark counterparts =="
cargo build --manifest-path "${RUST_DIR}/Cargo.toml" --release --target-dir "${RUST_TARGET_DIR}"

require_binary "$CPP_B1"
require_binary "$CPP_B2"
require_binary "$RUST_B1"
require_binary "$RUST_B2"

echo
echo "== Scenario A: Sync pressure (C++ B1 vs Rust sync) =="
CPP_B1_OUT="$(run_with_env "$CPP_B1" \
    --clients 4 \
    --queries 200 \
    --warmup 20 \
    --timeout-sec 60 \
    --sql "SELECT 1")"
printf '%s\n' "$CPP_B1_OUT"
CPP_B1_QPS="$(printf '%s\n' "$CPP_B1_OUT" | extract_metric "qps")"

RUST_B1_OUT="$(run_with_env "$RUST_B1" \
    --clients 4 \
    --queries 200 \
    --warmup 20 \
    --timeout-sec 60 \
    --sql "SELECT 1")"
printf '%s\n' "$RUST_B1_OUT"
RUST_B1_QPS="$(printf '%s\n' "$RUST_B1_OUT" | extract_metric "qps")"

echo
echo "== Scenario B: Async pressure (C++ B2 vs Rust async) =="
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

if [ -z "$CPP_B1_QPS" ] || [ -z "$RUST_B1_QPS" ] || [ -z "$CPP_B2_QPS" ] || [ -z "$RUST_B2_QPS" ]; then
    echo "failed to parse qps from one or more benchmark outputs" >&2
    exit 1
fi

echo
echo "== Summary (QPS) =="
echo "sync:  C++=${CPP_B1_QPS}  Rust=${RUST_B1_QPS}"
echo "async: C++=${CPP_B2_QPS}  Rust=${RUST_B2_QPS}"
