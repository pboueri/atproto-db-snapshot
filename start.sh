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
#   ./start.sh --config PATH    load object_store from PATH (e.g. config.yaml for R2)
#   ./start.sh --no-labels      skip the labels subscriber
#
# --store and --config are mutually exclusive: pick local-disk uploads OR
# the YAML-configured backend (R2). With neither, `run` falls back to
# -no-upload and seal artifacts stay in ./data/daily/.
#
# Stop with Ctrl+C — the trap propagates SIGTERM to all children and waits.

set -euo pipefail

cd "$(dirname "$0")"

# ----- defaults -----
DATA_DIR="./data"
STORE_DIR=""
CONFIG_PATH=""
LISTEN="127.0.0.1:8080"
SEED_N=""
WITH_LABELS=1

# ----- arg parse -----
while [[ $# -gt 0 ]]; do
    case "$1" in
        --seed)       SEED_N="$2"; shift 2 ;;
        --store)      STORE_DIR="$2"; shift 2 ;;
        --config)     CONFIG_PATH="$2"; shift 2 ;;
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

if [[ -n "$STORE_DIR" && -n "$CONFIG_PATH" ]]; then
    echo "error: --store and --config are mutually exclusive" >&2
    exit 2
fi
if [[ -n "$CONFIG_PATH" && ! -f "$CONFIG_PATH" ]]; then
    echo "error: --config $CONFIG_PATH not found" >&2
    exit 2
fi

# Args shared by run/build/serve to select the object store backend.
store_args=()
if [[ -n "$STORE_DIR" ]]; then
    store_args+=(-file-store "$STORE_DIR")
elif [[ -n "$CONFIG_PATH" ]]; then
    store_args+=(-config "$CONFIG_PATH")
fi

# ----- build binary (cheap; Go caches) -----
# On macOS, force Apple's clang for the cgo build. Homebrew's LLVM links
# against its own libc++, which doesn't match the libc++ the duckdb static
# library was built against — link errors like missing
# std::__1::basic_stringbuf and _SecTrustCopyCertificateChain.
if [[ "$(uname -s)" == "Darwin" ]]; then
    if APPLE_CC=$(xcrun --find clang 2>/dev/null) && APPLE_CXX=$(xcrun --find clang++ 2>/dev/null); then
        export CC="$APPLE_CC"
        export CXX="$APPLE_CXX"
    fi
    # Some CLT installs don't auto-resolve the sysroot; clang then fails to
    # find <stdlib.h>/<errno.h>. Setting SDKROOT explicitly is the fix.
    if [[ -z "${SDKROOT:-}" ]]; then
        if SDK_PATH=$(xcrun --show-sdk-path 2>/dev/null); then
            export SDKROOT="$SDK_PATH"
        fi
    fi
fi

echo "==> building at-snapshotter (CC=${CC:-default})"
go build -o ./at-snapshotter ./cmd/at-snapshotter

mkdir -p "$DATA_DIR/logs"

# ----- optional graph backfill (resumable; safe to re-run) -----
# graph-backfill is idempotent: the binary checks actors_registry +
# actors.repo_processed and skips DIDs already crawled in a prior run.
# Running again with the same --seed N is a near-no-op when complete;
# running after a crash resumes from the last ~30s checkpoint.
if [[ -n "$SEED_N" ]]; then
    echo "==> seeding graph: CAR-crawling up to $SEED_N DIDs from bsky.network (resumable)"
    ./at-snapshotter build \
        -mode graph-backfill \
        -limit "$SEED_N" \
        -data-dir "$DATA_DIR" \
        "${store_args[@]}" 2>&1 | tee "$DATA_DIR/logs/seed.log"
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
if [[ ${#store_args[@]} -gt 0 ]]; then
    run_args+=("${store_args[@]}")
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
if [[ ${#store_args[@]} -gt 0 ]]; then serve_args+=("${store_args[@]}"); fi
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
if [[ -n "$STORE_DIR" ]]; then
    storearg=" -file-store $STORE_DIR"
elif [[ -n "$CONFIG_PATH" ]]; then
    storearg=" -config $CONFIG_PATH"
fi
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
