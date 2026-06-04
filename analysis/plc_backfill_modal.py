"""Backfill account creation/deactivation dates from the PLC directory export.

Streams the full PLC operation log (https://plc.directory/export, JSONL, ordered
by createdAt ascending) and captures two op kinds per DID:

  - genesis      (operation.prev is null)            -> kind='create'
  - tombstone    (operation.type == 'plc_tombstone') -> kind='tombstone'

and writes them as sharded parquet to the shared output volume at
`/vol-out/var/plc/part-NNNNN.parquet` with schema (did: str, kind: str,
ts: timestamp[ms]). The Rust hydrate stage (`07_enrich_actors_created.sql`)
reads these shards to add `created_at` / `tombstoned_at` to `actors`.

Resumable: a `/vol-out/var/plc/.cursor.json` records the last `after` cursor,
shard count, and row totals. Re-running resumes from the last flushed shard
(at most one shard's worth of pages is re-fetched). Idempotent at the SQL layer
(`GROUP BY did`) absorbs rare millisecond-tie boundary dupes.

Launch (detached, runs server-side for hours):
    .venv/bin/modal run --detach analysis/plc_backfill_modal.py

Check progress:
    .venv/bin/modal volume get at-snapshot-output var/plc/.cursor.json -
    .venv/bin/modal app logs at-plc-backfill
"""

from __future__ import annotations

import json
import os
import time

import modal

volume_out = modal.Volume.from_name("at-snapshot-output", create_if_missing=False)

image = modal.Image.debian_slim(python_version="3.12").pip_install(
    "pyarrow==16.1.0", "requests==2.32.3"
)

app = modal.App("at-plc-backfill")

PLC_DIR = "/vol-out/var/plc"
EXPORT_URL = "https://plc.directory/export"


@app.function(
    image=image,
    volumes={"/vol-out": volume_out},
    timeout=24 * 60 * 60,
    cpu=2.0,
    memory=8 * 1024,
)
def backfill(page_size: int = 1000, flush_every: int = 1_000_000) -> dict:
    import datetime as dt

    import pyarrow as pa
    import pyarrow.parquet as pq
    import requests

    os.makedirs(PLC_DIR, exist_ok=True)
    cursor_path = f"{PLC_DIR}/.cursor.json"

    cur: dict = {}
    if os.path.exists(cursor_path):
        with open(cursor_path) as f:
            cur = json.load(f)
    if cur.get("completed"):
        print(f"=== PLC backfill already completed: {cur} ===", flush=True)
        return cur

    after = cur.get("after")               # raw ISO string passed back to the API
    shards = int(cur.get("shards", 0))
    rows = int(cur.get("rows", 0))
    pages = int(cur.get("pages", 0))
    print(f"=== PLC backfill start (resume after={after!r} shards={shards} "
          f"rows={rows:,} pages={pages:,}) ===", flush=True)

    schema = pa.schema([
        ("did", pa.string()),
        ("kind", pa.string()),
        ("ts", pa.timestamp("ms")),
    ])
    buf_did: list[str] = []
    buf_kind: list[str] = []
    buf_ts: list = []

    def parse_ts(s: str):
        # PLC createdAt is ISO-8601 UTC ("...Z", variable fractional digits).
        # Store as naive-UTC ms to match Rust Timestamp(Millisecond, None).
        d = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
        return d.astimezone(dt.timezone.utc).replace(tzinfo=None)

    def flush():
        nonlocal shards, buf_did, buf_kind, buf_ts
        if not buf_did:
            return
        path = f"{PLC_DIR}/part-{shards:05d}.parquet"
        tmp = path + ".tmp"
        tbl = pa.table(
            {
                "did": pa.array(buf_did, pa.string()),
                "kind": pa.array(buf_kind, pa.string()),
                "ts": pa.array(buf_ts, pa.timestamp("ms")),
            },
            schema=schema,
        )
        pq.write_table(tbl, tmp, compression="zstd")
        os.replace(tmp, path)
        shards += 1
        buf_did, buf_kind, buf_ts = [], [], []

    def save_cursor(completed: bool = False):
        payload = {
            "after": after,
            "shards": shards,
            "rows": rows,
            "pages": pages,
            "completed": completed,
            "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        }
        tmp = cursor_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(payload, f)
        os.replace(tmp, cursor_path)
        volume_out.commit()

    sess = requests.Session()
    sess.headers["user-agent"] = "atproto-db-snapshot/plc-backfill"
    rows_since_flush = 0
    t0 = time.time()
    backoff = 1.0

    while True:
        params = {"count": page_size}
        if after:
            params["after"] = after
        try:
            resp = sess.get(EXPORT_URL, params=params, timeout=90)
        except requests.RequestException as e:
            print(f"  request error: {e}; sleeping {backoff:.0f}s", flush=True)
            time.sleep(backoff)
            backoff = min(60.0, backoff * 2)
            continue
        if resp.status_code == 429 or resp.status_code >= 500:
            print(f"  http {resp.status_code}; sleeping {backoff:.0f}s", flush=True)
            time.sleep(backoff)
            backoff = min(60.0, backoff * 2)
            continue
        if resp.status_code != 200:
            raise RuntimeError(f"unexpected http {resp.status_code}: {resp.text[:200]}")
        backoff = 1.0

        lines = [ln for ln in resp.text.split("\n") if ln.strip()]
        if not lines:
            flush()
            save_cursor(completed=True)
            break

        for ln in lines:
            op = json.loads(ln)
            inner = op.get("operation", {})
            if inner.get("prev") is None:
                kind = "create"
            elif inner.get("type") == "plc_tombstone":
                kind = "tombstone"
            else:
                continue
            buf_did.append(op["did"])
            buf_kind.append(kind)
            buf_ts.append(parse_ts(op["createdAt"]))
            rows += 1
            rows_since_flush += 1

        after = json.loads(lines[-1])["createdAt"]
        pages += 1

        if len(lines) < page_size:
            # Reached the tail of the export.
            flush()
            save_cursor(completed=True)
            break

        if rows_since_flush >= flush_every:
            flush()
            save_cursor(completed=False)
            rows_since_flush = 0
            rate = pages / max(1e-6, time.time() - t0)
            print(f"  pages={pages:,} rows={rows:,} shards={shards} "
                  f"after={after} ({rate:.1f} pages/s)", flush=True)

    elapsed = time.time() - t0
    print(f"=== PLC backfill DONE: pages={pages:,} rows={rows:,} shards={shards} "
          f"in {elapsed/3600:.2f}h ===", flush=True)
    return {"pages": pages, "rows": rows, "shards": shards, "completed": True}


@app.local_entrypoint()
def main(page_size: int = 1000, flush_every: int = 1_000_000) -> None:
    call = backfill.spawn(page_size=page_size, flush_every=flush_every)
    print(f"[spawn] FunctionCall {call.object_id} — PLC backfill running server-side.")
    print("[progress] modal volume get at-snapshot-output var/plc/.cursor.json -")
    print("[logs]     modal app logs at-plc-backfill")
