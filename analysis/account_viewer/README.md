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
- `add_created_est.py` — *optional* enrichment: adds an estimated signup-date
  column (`created_est`) so you can sort by creation date. See below.
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

### Optional: sort by estimated creation date

Account creation time is **not** in the snapshot. `add_created_est.py`
approximates it as each actor's earliest *plausible* follow (the `follows`
table is full-history, and onboarding follows land near signup), adding a
`created_est` column that becomes a sort facet. It does a heavy one-time scan
of the ~1.33B-row `follows` table over the network (**multiple hours**) into a
side file, then merges in a few seconds — the viewer stays usable throughout
(stop the server briefly only for the final merge). Restart the server after
it finishes to pick up the new facet.

```bash
.venv/bin/python analysis/account_viewer/add_created_est.py    # ~hours, resumable
```

`created_est` is an estimate; ~16% of accounts (fully-inert + like-only
lurkers, which have no follow edge) are null and sort last. The TID
plausibility filter (`2022-11-01`..snapshot) keeps bogus 1970/future
timestamps from poisoning the per-actor minimum.

## Using it

- **Category chips** (top) filter by activity bucket. `ghost_lt10` is the
  cohort under investigation (joined, <10 follows, no posts/likes).
- **WHERE box**: type a SQL predicate against the `accounts` columns, e.g.
  `followers > 1000 AND content = 0`. Tick **raw SQL** to paste a full SELECT.
- **sort** — order the list by any facet (followers, follows, posts, likes,
  reposts, replies, quotes, blocks in/out, content, …); the **↓ desc / ↑ asc**
  button flips direction. The sorted value is shown in each row.
- **sort → "created date (live)"** — sorts the *currently loaded* rows by each
  account's **real** signup date, fetched live (`getProfiles`, batched 25/call,
  cached). No precompute needed; accurate, but scoped to the loaded sample
  (re-sample / "More" to extend). Account creation isn't in the snapshot and
  can't be derived from the DID (it's a hash) — this reads the true date the
  API already returns. For a *global* estimated date column instead, run
  `add_created_est.py` (above).
- **prefetch** — how many accounts *ahead* of the cursor to hydrate in the
  background (default 3) so arrowing down is instant. Set to `off` to disable.
- **r** — random sample (re-roll). **Run / ⏎** — apply the filter.
- **↑ / ↓** (or j/k) — flip through accounts; the detail pane hydrates live
  (instant for already-prefetched rows).
- **1–9** — tag the current account with a reason. **/** focuses the query box.
  **o** opens the account on bsky.app.
- Labels + notes are saved to `viewer.sqlite` (table `labels`) — that's the
  seed for the mass-labeling pass.

## What the detail pane shows

Live from bsky: profile **banner** + avatar, display name, handle, bio,
**join date**, current follow/follower/post counts (next to the snapshot
counts), recent posts **with their media** (images, video thumbnails, link
cards, quoted posts) + a small activity sparkline, and **who they follow**.
If the profile call fails (deactivated / deleted / takedown) that status is
shown — itself a diagnosis.

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
