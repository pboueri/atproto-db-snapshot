#!/usr/bin/env bash
#
# start.sh — bring up the full at-snapshotter pipeline locally.
#
# Long-running services started here:
#   1. run    — Jetstream consumer (writes events → ./data/staging.db, seals → ./data/daily/)
#   2. labels — Bluesky labeler subscriber (writes labels → ./data/labels.db)
#   3. serve  — read-only HTTP dashboard at $LISTEN
#
# `build` is a nightly one-shot (cron it; see "After first run" below).
#
# Usage:
#   ./start.sh                  start the three services (assumes graph already seeded)
#   ./start.sh --seed N         CAR-crawl N DIDs first to seed current_graph.duckdb
#   ./start.sh --listen ADDR    bind serve to ADDR (default 127.0.0.1:8080)
#   ./start.sh --store DIR      use DIR as the file-backed object store (enables uploads)
#   ./start.sh --no-labels      skip the labels subscriber
#
# Stop with Ctrl+C — the trap propagates SIGTERM to all children and waits.

set -euo pipefail

cd "$(dirname "$0")"

# ----- defaults -----
DATA_DIR="./data"
STORE_DIR=""
LISTEN="127.0.0.1:8080"
SEED_N=""
WITH_LABELS=1

# ----- arg parse -----
while [[ $# -gt 0 ]]; do
    case "$1" in
        --seed)       SEED_N="$2"; shift 2 ;;
        --store)      STORE_DIR="$2"; shift 2 ;;
        --listen)     LISTEN="$2"; shift 2 ;;
        --data-dir)   DATA_DIR="$2"; shift 2 ;;
        --no-labels)  WITH_LABELS=0; shift ;;
        -h|--help)
            sed -n '2,/^set -euo/p' "$0" | sed -n 's|^# \{0,1\}||p'
            exit 0
            ;;
        *) echo "unknown flag: $1" >&2; exit 2 ;;
    esac
done

# ----- build binary (cheap; Go caches) -----
echo "==> building at-snapshotter"
go build -o ./at-snapshotter ./cmd/at-snapshotter

mkdir -p "$DATA_DIR/logs"

# ----- optional first-time graph backfill -----
if [[ -n "$SEED_N" ]]; then
    if [[ -f "$DATA_DIR/current_graph.duckdb" ]]; then
        echo "==> current_graph.duckdb already exists; skipping --seed $SEED_N"
    else
        echo "==> seeding graph: CAR-crawling $SEED_N DIDs from bsky.network"
        store_args=()
        if [[ -n "$STORE_DIR" ]]; then store_args=(-file-store "$STORE_DIR"); fi
        ./at-snapshotter build \
            -mode graph-backfill \
            -limit "$SEED_N" \
            -data-dir "$DATA_DIR" \
            "${store_args[@]}" 2>&1 | tee "$DATA_DIR/logs/seed.log"
    fi
fi

# ----- launch services -----
declare -a PIDS=()
declare -a NAMES=()

start_bg() {
    local name="$1"; shift
    local logfile="$DATA_DIR/logs/${name}.log"
    echo "==> starting $name (log: $logfile)"
    "$@" >>"$logfile" 2>&1 &
    PIDS+=($!)
    NAMES+=("$name")
}

# run — Jetstream consumer
run_args=(-data-dir "$DATA_DIR")
if [[ -n "$STORE_DIR" ]]; then
    run_args+=(-file-store "$STORE_DIR")
else
    run_args+=(-no-upload)
fi
start_bg "run" ./at-snapshotter run "${run_args[@]}"

# labels — Bluesky moderation labeler subscriber
if [[ "$WITH_LABELS" == "1" ]]; then
    start_bg "labels" ./at-snapshotter labels -data-dir "$DATA_DIR"
fi

# serve — read-only health dashboard
serve_args=(-data-dir "$DATA_DIR" -listen "$LISTEN")
if [[ -n "$STORE_DIR" ]]; then serve_args+=(-file-store "$STORE_DIR"); fi
start_bg "serve" ./at-snapshotter serve "${serve_args[@]}"

# ----- shutdown handling -----
shutdown() {
    echo
    echo "==> shutting down (SIGTERM to ${PIDS[*]})"
    for pid in "${PIDS[@]}"; do
        kill -TERM "$pid" 2>/dev/null || true
    done
    for pid in "${PIDS[@]}"; do
        wait "$pid" 2>/dev/null || true
    done
    echo "==> stopped"
    exit 0
}
trap shutdown INT TERM

# ----- status -----
echo
echo "==================================================="
echo " at-snapshotter is running"
echo "---------------------------------------------------"
for i in "${!PIDS[@]}"; do
    printf " %-7s pid=%s log=%s/logs/%s.log\n" \
        "${NAMES[$i]}" "${PIDS[$i]}" "$DATA_DIR" "${NAMES[$i]}"
done
echo "---------------------------------------------------"
echo " dashboard: http://${LISTEN}/"
echo " stop:      Ctrl+C  (or: kill ${PIDS[*]})"
echo "==================================================="
echo
echo "After first run, schedule the nightly build via cron:"
storearg=""
if [[ -n "$STORE_DIR" ]]; then storearg=" -file-store $STORE_DIR"; fi
echo "  0 1 * * *  cd $(pwd) && ./at-snapshotter build -mode incremental -data-dir $DATA_DIR$storearg"
echo

# Block until any child exits OR a signal is received.
# `wait -n` returns when the first one exits; we then propagate.
if wait -n 2>/dev/null; then
    echo "==> a service exited cleanly; tearing down the rest"
else
    echo "==> a service exited with non-zero status; tearing down"
fi
shutdown
