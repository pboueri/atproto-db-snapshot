# ATProto Analytic Snapshot

## Goal

Produce a public DuckDB snapshot of the Bluesky social graph and post-relationship
graph, derived end-to-end from the **microcosm.blue constellation** RocksDB
backlinks index. Cheap, no-ceremony analytics on commodity hardware.

This spec covers a **full, from-scratch build**. Out of scope (deferred):

- Streaming / incremental updates (jetstream).
- Post text, profile bios, display names, alt text.
- Media blob downloads / CDN mirroring.
- Language / labeler filters (return when text is back in scope).

## What constellation actually stores

Constellation is a **backlinks index**, not a record store. Per
`constellation/src/storage/rocks_store.rs` it has four column families:

| CF | Key | Value | What it is |
|---|---|---|---|
| `did_ids` | bincode(`Did`) | bincode(`DidIdValue(DidId, active: bool)`) | DID → numeric ID + active flag. Also stores reverse: 8-byte big-endian `DidId` → bincode(`Did`). |
| `target_ids` | bincode(`TargetKey(Target, Collection, RPath)`) | bincode(`TargetId`) | A URI/DID/blob *as referenced from a specific (collection, json-path)* → numeric ID. Also reverse 8-byte → `TargetKey`. |
| `target_links` | bincode(`TargetId`) | bincode(`TargetLinkers(Vec<(DidId, RKey)>)`) | For a target, who linked to it (reverse index). Tombstones use `DidId(0)`. |
| `link_targets` | bincode(`RecordLinkKey(DidId, Collection, RKey)`) | bincode(`RecordLinkTargets(Vec<(RPath, TargetId)>)`) | For a record, the targets it links to + the json-path of each link (forward index). |

Implications for us:

- We only see records that **contain at least one link**. A text-only post with
  no embed / reply / quote and no engagement won't have a `link_targets` entry.
  It might still appear as the *target* of someone else's like / reply / quote,
  in which case the post URI shows up in `target_ids`.
- AT-URIs encode author DID and rkey; rkeys for like / repost / follow / block
  / post are TIDs that decode to a microsecond timestamp. So even when we only
  know about a post via `target_ids`, we can still derive `(uri, author_did,
  created_at)` without the forward record.
- `app.bsky.actor.profile` rkeys are `self`, not TIDs — `created_at` is NULL.
- Encoding is **bincode big-endian**; reverse-mapping entries inside `did_ids`
  and `target_ids` are 8-byte keys (the IDs themselves) and must be skipped
  when iterating those CFs (constellation's repair loop does exactly this with
  `if raw_key.len() == 8`).
- `target_links` uses a custom merge operator on writes. We open **read-only**,
  so we don't need to register it.

## Mental model

```
constellation rocksdb  ──(eat-rocks)──▶  local rocks mirror
local rocks mirror     ──(stage)──────▶  staging parquet (per entity)
staging parquet        ──(hydrate)────▶  snapshot.duckdb (bounded RAM)
snapshot.duckdb + parquet  ──(publish)─▶  object store (R2 default)
```

Each stage is a single-responsibility module behind a `Stage` trait. The
binary exposes one command today; the trait split makes per-stage subcommands
mechanical to add later.

## Final output

```
data/
  raw/YYYY-MM-DD/
    actors.parquet
    follows.parquet
    blocks.parquet
    posts_from_records.parquet     # rows derived from link_targets scan
    posts_from_targets.parquet     # rows derived from target_ids scan (deduped in hydrate)
    post_media.parquet
    likes.parquet
    reposts.parquet
  snapshot/YYYY-MM-DD/
    snapshot.duckdb
    snapshot_metadata.json
```

`snapshot.duckdb` is the single analytic artifact. `snapshot_metadata.json`
captures constellation source revision, eat-rocks revision, schema version,
host stats, and per-table row counts.

## Technologies

- **Rust** stable.
- **eat-rocks** (https://tangled.org/microcosm.blue/eat-rocks) as a Cargo
  dependency. eat-rocks owns the *mirror* (clone the upstream rocksdb to a
  local directory). Our staging stage then opens that directory **read-only**
  with the `rocksdb` crate directly using the same CF descriptors as
  `rocks_store.rs` (no merge operator needed for read-only access).
- `bincode` (default options + big-endian) to decode keys/values, matching
  `_bincode_opts()` in rocks_store.rs.
- DuckDB (`duckdb` crate), Parquet/Arrow (`parquet` + `arrow` crates).
- `object_store` crate; **R2 default**, local FS for tests.

## CLI

A single command today; modular internals.

```
at-snapshot build [flags]
```

Flags:

- `--config <path>`: TOML config (preferred). Flags below also via env / CLI.
- `--work-dir <path>`: local working dir (default `./var`).
- `--snapshot-date <YYYY-MM-DD>`: namespace for outputs (default today UTC).
- `--memory-limit <size>`: DuckDB memory cap (default `4GiB`).
- `--batch-size <n>`: parquet writer batch size (default `100_000`).
- `--object-store <url>`: e.g. `s3://bucket/prefix` or `file:///tmp/out`.
- `--skip-mirror | --skip-stage | --skip-hydrate | --skip-publish`: per-stage
  opt-out for resumes and tests.
- `--monitor-addr <host:port>` (optional): JSON progress endpoint.
  Structured INFO logs are emitted regardless.

Each stage is idempotent and resumable.

## Stages

### 1. Mirror — eat-rocks

- Configured with the constellation source URL.
- Calls eat-rocks's library API to clone / refresh `./var/rocks/`.
- Source revision / cursor checkpointed in `./var/rocks/.cursor`.
- Re-running with a matching cursor is a no-op; otherwise eat-rocks resumes.

### 2. Stage — rocks → parquet

Open `./var/rocks/` read-only with the four CF descriptors above. Run three
streaming passes; each writes via temp file + atomic rename, batched at
`--batch-size` rows. The rocks adapter (`src/rocks/schema.rs`) is the only
module that knows about bincode layouts.

**Pass A — scan `did_ids`** (skip 8-byte reverse-mapping keys):
- Decode `Did` (key) and `DidIdValue(DidId, active)` (value).
- Emit `actors.parquet` with `(did_id, did, active)`.

**Pass B — scan `link_targets`** (no reverse-mapping rows in this CF):
- Decode `RecordLinkKey(did_id, collection, rkey)` and
  `RecordLinkTargets(Vec<(rpath, target_id)>)`.
- Compute `created_at` = TID-decode(rkey) for like / repost / follow / block /
  post collections; NULL otherwise.
- Resolve each `target_id` to `TargetKey(target, collection, rpath)` via point
  lookup `db.get_cf(target_ids, id.to_be_bytes())`.
- Multiplex into output files by collection:
  - `app.bsky.feed.post` → `posts_from_records.parquet` row, plus 0..n
    `post_media.parquet` rows for forward links at media RPaths
    (`.embed.images[N].image`, `.embed.video.video`, `.embed.external.uri`,
    `.embed.external.thumb`, `.embed.record.uri`,
    `.embed.recordWithMedia.media.images[N].image`, etc.).
  - `app.bsky.feed.like` → `likes.parquet`, subject from `.subject.uri`.
  - `app.bsky.feed.repost` → `reposts.parquet`, subject from `.subject.uri`.
  - `app.bsky.graph.follow` → `follows.parquet`, subject from `.subject`.
  - `app.bsky.graph.block` → `blocks.parquet`, subject from `.subject`.
  - `app.bsky.actor.profile` → no row here (we already have the actor from
    Pass A); future text-content spec will populate fields from this record.
  - Other collections → ignored (out of scope for this snapshot).

**Pass C — scan `target_ids`** (skip 8-byte reverse-mapping keys):
- For `TargetKey` rows whose `Target` is an AT-URI of `app.bsky.feed.post`,
  emit a `posts_from_targets.parquet` row with `(uri, author_did, rkey,
  created_at)` parsed from the URI / TID. This catches posts that exist in
  the graph only as targets of likes / quotes / replies.

Memory is bounded by `--batch-size`; each pass streams. Point lookups for ID
resolution rely on RocksDB's block cache.

### 3. Hydrate — parquet → duckdb

Open `./var/snapshot.duckdb` with `SET memory_limit=...; SET temp_directory=...;`.
Build tables and aggregates entirely in SQL:

- `actors` ← `read_parquet('actors.parquet')`.
- `follows`, `blocks`, `likes`, `reposts`, `post_media` ← straightforward
  loads.
- `posts` ← `posts_from_records UNION BY NAME posts_from_targets`, deduped on
  `uri`, preferring rows that came from records (they have richer fields like
  reply / quote refs).
- `actor_aggs`, `post_aggs` ← `GROUP BY` queries over the base tables.

Validation queries run after hydrate; failures abort publish.

### 4. Publish

- Stream `raw/.../*.parquet`, `snapshot/.../snapshot.duckdb`, and
  `snapshot_metadata.json` to the configured object store via multipart
  upload.

## Data model

Join keys are constellation's native numeric IDs (`DidId`, `TargetId`). They
are reproducible within a snapshot but not portable across snapshots. Source
strings are preserved alongside in `actors` and `posts` so consumers can join
by DID / URI directly if needed.

| Table | Columns |
|---|---|
| `actors` | `did_id PK`, `did STRING`, `active BOOL` |
| `actor_aggs` | `did_id PK`, `follows`, `followers`, `blocks_out`, `blocks_in`, `posts`, `likes_out`, `likes_in`, `reposts_out`, `reposts_in`, `replies_out`, `quotes_out`, `quoted_count` |
| `follows` | `src_did_id`, `dst_did_id`, `rkey`, `created_at`, PK `(src_did_id, rkey)` |
| `blocks` | `src_did_id`, `dst_did_id`, `rkey`, `created_at`, PK `(src_did_id, rkey)` |
| `posts` | `uri PK`, `author_did_id`, `rkey`, `created_at`, `reply_root_uri NULL`, `reply_parent_uri NULL`, `quote_uri NULL`, `source ENUM('record','target_only')` |
| `post_aggs` | `uri PK`, `likes`, `reposts`, `replies`, `quotes` |
| `post_media` | `uri`, `ord`, `kind ENUM('image','video','external','record','external_thumb')`, `ref STRING` |
| `likes` | `actor_did_id`, `subject_uri`, `rkey`, `created_at`, PK `(actor_did_id, rkey)` |
| `reposts` | `actor_did_id`, `subject_uri`, `rkey`, `created_at`, PK `(actor_did_id, rkey)` |

Notes:

- `post_aggs` counts come from SQL aggregations of `likes`, `reposts`, plus
  `posts WHERE reply_parent_uri = ?` for replies and `posts WHERE quote_uri =
  ?` for quotes. Counts are **all-time within this snapshot** — there is no
  per-day window in this spec because we do not ingest streaming data.
- A nontrivial fraction of `likes.subject_uri` and `reposts.subject_uri` will
  point to posts that aren't in `posts` (e.g. text-only posts that nobody else
  interacted with at the time their author wrote them, or non-bsky lexicons).
  Pass C reduces this. The hydrate stage records the orphan rate in
  `snapshot_metadata.json` and does not treat a non-zero rate as a failure.
- `active = false` actors are kept in the table; downstream consumers can
  filter as needed.

## Validation

A small recorded RocksDB fixture lives at `tests/fixtures/constellation_mini/`
covering each entity, a quote chain, a reply chain, an external embed, an
inactive DID, and a tombstone (`DidId(0)`) in `target_links`. `cargo test`
runs the full pipeline against the fixture using the local-FS object store
and asserts:

- How many people followed or blocked someone else "yesterday"
  (TID-derived).
- What % of total follows were generated yesterday.
- How many people who posted got at least one like.
- How many posts got at least one like.

Per-stage unit tests cover the rocks schema decoder
(`src/rocks/schema.rs`) and parquet writers in isolation.

## Open questions to resolve during implementation

- Confirm exactly which paths constellation indexes for media inside post
  records (i.e. whether blob CIDs at `.embed.images[N].image` actually appear
  as `Target`s, or only externally-resolvable URIs do). The list above
  reflects expected atproto paths; the rocks adapter will be updated to match
  whatever the fixture actually contains.
- Confirm eat-rocks's library entry point and the path it produces. The spec
  treats the rocksdb directory as the contract — whatever eat-rocks names is
  fine as long as it's openable read-only with the four CFs above.

## Reference implementations

- Constellation: https://tangled.org/microcosm.blue/microcosm-rs
- RocksDB schema:
  https://tangled.org/microcosm.blue/microcosm-rs/blob/main/constellation/src/storage/rocks_store.rs
- eat-rocks: https://tangled.org/microcosm.blue/eat-rocks
- Prior art: https://docs.bsky.app/blog/introducing-tap

## Implementation guidance

- `tokio` for async I/O at stage boundaries (object store, http monitor).
  Plain threads + bounded channels for the rocks → parquet pipeline.
- `anyhow` at binary edges, `thiserror` in libraries.
- One module per stage (`mirror.rs`, `stage.rs`, `hydrate.rs`, `publish.rs`),
  orchestrated through a `Stage` trait.
- All rocks-schema knowledge in exactly one module.
- Red/green tests; one logical commit at a time.

## README updates

Update `README.md` with: goal, mental-model overview, ERD in mermaid for the
tables above, the single CLI command + flags, and a "Deferred" section
listing the non-goals so users know what's intentionally missing.
