# account viewer

A local tool for eyeballing Bluesky accounts to figure out *why* so many of
them join and then do nothing (≈40% of all accounts have zero posts/likes and
<10 follows). Browse the snapshot's per-actor counts on the left, hydrate one
account live from the bsky public API on the right, and tag accounts with a
reason as you flip through them.

## What's where

- `build_accounts_db.py` — builds the local `accounts.duckdb` (one row per
  actor: DID + all `actor_aggs` counts + a derived activity `category`). It
  range-reads the published 57 GB snapshot over HTTPS and copies only the two
  tables it needs — **no full download**.
- `server.py` — zero-dependency stdlib server (DuckDB for the list, bsky public
  AppView for the detail pane, sqlite for API cache + your labels).
- `static/index.html` — the two-pane UI.
- `accounts.duckdb` — generated; the data the viewer queries.
- `viewer.sqlite` — generated; API cache + your labels.

## Run

```bash
# 1. build the local db (once per snapshot; a few minutes over the network)
.venv/bin/python analysis/account_viewer/build_accounts_db.py

# 2. start the viewer
.venv/bin/python analysis/account_viewer/server.py
# open http://127.0.0.1:8765
```

## Using it

- **Category chips** (top) filter by activity bucket. `ghost_lt10` is the
  cohort under investigation (joined, <10 follows, no posts/likes).
- **WHERE box**: type a SQL predicate against the `accounts` columns, e.g.
  `followers > 1000 AND content = 0`. Tick **raw SQL** to paste a full SELECT.
- **r** — random sample (re-roll). **Run / ⏎** — apply the filter.
- **↑ / ↓** (or j/k) — flip through accounts; the detail pane hydrates live.
- **1–9** — tag the current account with a reason. **/** focuses the query box.
  **o** opens the account on bsky.app.
- Labels + notes are saved to `viewer.sqlite` (table `labels`) — that's the
  seed for the mass-labeling pass.

## What the detail pane shows

Live from bsky: avatar, display name, handle, bio, **join date**, current
follow/follower/post counts (next to the snapshot counts), recent posts +
a small activity sparkline, and **who they follow**. If the profile call fails
(deactivated / deleted / takedown) that status is shown — itself a diagnosis.

Note: bsky has no structured location field; any location lives in the bio text.

## Categories (in `build_accounts_db.py`, tunable)

| category | meaning |
|---|---|
| `inert` | joined, did nothing — not even a follow |
| `ghost_lt10` | no posts/likes, 1–9 follows ← the 40% |
| `ghost_followed` | no posts/likes, ≥10 follows |
| `lurker` | likes things but never authored anything |
| `dabbler` | 1–4 authored items |
| `poster` | 5–99 authored items |
| `active` | ≥100 authored items |
