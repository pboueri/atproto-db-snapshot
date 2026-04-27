# ATProto Analytic Snapshot

## Goal

Produce a periodic, public DuckDB snapshot of the Bluesky social graph and
post-relationship graph for low-ceremony analytics on commodity hardware. The
snapshot is derived end-to-end from the **microcosm.blue constellation** index
— constellation already maintains a RocksDB index of every relevant atproto
record reference (follows, blocks, posts, replies, quote subjects, likes,
reposts, profile records, media embeds), so we can reconstruct the graph
without re-crawling the network.

This spec covers a **full, from-scratch build** of the snapshot. The following
are explicit non-goals and are deferred to a future spec:

- Streaming / incremental updates (jetstream ingestion).
- Post text, profile bios, display names, alt text.
- Media blob downloads / CDN mirroring.
- Language / labeler filters (these return when text is back in scope).

## Mental model

```
constellation rocksdb  ──(eat-rocks)──▶  local rocks mirror
local rocks mirror     ──(stage)──────▶  staging parquet (per entity)
staging parquet        ──(hydrate)────▶  snapshot.duckdb (bounded RAM)
snapshot.duckdb + parquet  ──(publish)─▶  object store (R2 by default)
```

Each stage is a single-responsibility module. The binary exposes one command
today; the modules are wired so a future split into per-stage subcommands is
mechanical.

## Final output

```
data/
  raw/YYYY-MM-DD/
    actors.parquet
    follows.parquet
    blocks.parquet
    posts.parquet
    post_media.parquet
    likes.parquet
    reposts.parquet
  snapshot/YYYY-MM-DD/
    snapshot.duckdb
    snapshot_metadata.json
```

`snapshot.duckdb` is the single analytic artifact. `snapshot_metadata.json`
captures the constellation source revision, eat-rocks revision, schema
version, host stats, and per-table row counts.

## Technologies

- Language: **Rust** (stable).
- RocksDB mirror: **eat-rocks**
  (https://tangled.org/microcosm.blue/eat-rocks), pulled in as a Cargo
  dependency and called as a library — not shelled out to.
- Source schema reference:
  https://tangled.org/microcosm.blue/microcosm-rs/blob/main/constellation/src/storage/rocks_store.rs
  — the staging stage's rocks adapter is the only module that knows this
  layout, so when constellation's storage changes, only that file changes.
- Analytic DB: DuckDB (via the `duckdb` crate).
- Columnar staging: Parquet (`parquet` + `arrow` crates).
- Object store: S3-compatible API via the `object_store` crate; **R2 default**,
  local filesystem backend for tests.

## CLI

A single command today. Internals are split per stage so subcommands can be
exposed later without refactoring.

```
at-snapshot build [flags]
```

Flags:

- `--config <path>`: TOML config (preferred). All flags below can also be set
  via config or env.
- `--work-dir <path>`: local working directory (default `./var`).
- `--snapshot-date <YYYY-MM-DD>`: namespace for outputs; defaults to today UTC.
- `--memory-limit <size>`: DuckDB working-memory cap (default `4GiB`).
- `--batch-size <n>`: rocks → parquet streaming batch size (default `100_000`).
- `--object-store <url>`: e.g. `s3://bucket/prefix`, `r2://...`, or
  `file:///tmp/out` for tests.
- `--skip-mirror | --skip-stage | --skip-hydrate | --skip-publish`: per-stage
  opt-out for resumes and tests.
- `--monitor-addr <host:port>` (optional): exposes per-stage progress as JSON
  for external scraping. Structured INFO logs are emitted regardless so a
  sidecar tail can display status without the server.

Each stage is idempotent and resumable; re-running the command picks up where
it left off.

## Stages

### 1. Mirror — eat-rocks library call

- Reads the constellation source URL from config.
- Calls eat-rocks's library API to clone / refresh `./var/rocks/`.
- Source revision / cursor is checkpointed in `./var/rocks/.cursor`. A re-run
  is a no-op if the cursor matches; otherwise eat-rocks resumes.

### 2. Stage — rocks → parquet

- One reader per logical entity. Each reader iterates the relevant rocksdb key
  range (per the `rocks_store.rs` schema referenced above) and emits Arrow
  `RecordBatch`es of `--batch-size` rows.
- Each reader writes `./var/staging/<table>.parquet` via
  `parquet::arrow::ArrowWriter`. Writers go to a temp file and rename on
  success, so partial runs never leave readable-but-truncated parquet.
- Memory is bounded by batch size; we never load a full table into memory.
- The rocks adapter (`src/rocks/schema.rs`) is the single source of truth for
  rocks key/value layout.

Logical entities produced at this stage map 1:1 to the parquet files listed in
**Final output**.

### 3. Hydrate — parquet → duckdb

- Open `./var/snapshot.duckdb` with
  `SET memory_limit='<flag>'; SET temp_directory='./var/duckdb_spill';`.
- For each base table:
  `CREATE TABLE x AS SELECT * FROM read_parquet('./var/staging/x.parquet');`
  followed by primary-key / index creation.
- Aggregate tables (`actor_aggs`, `post_aggs`) are computed in SQL over the
  base tables — no manual counting in Rust.
- Validation queries (see **Validation**) run after hydrate; failures abort
  publish.

### 4. Publish

- Streams `raw/YYYY-MM-DD/*.parquet`, `snapshot/YYYY-MM-DD/snapshot.duckdb`,
  and `snapshot_metadata.json` to the configured object store via streaming
  multipart upload.
- Local-filesystem backend used for tests.

## Data model

All entities use **constellation's native numeric IDs** as primary keys (we
do not re-intern). The mapping back to the source string is preserved in
`actors` and `posts` so consumers can join back to DIDs and AT-URIs. A
constellation ID mapping table can be exported separately if downstream tools
need lookup outside `actors` / `posts`.

`created_at` is **decoded from the atproto TID** in the record key (rkey), not
from record content. This matches the data we have from constellation without
parsing record bodies.

| Table | Columns |
|---|---|
| `actors` | `did_id PK`, `did STRING`, `profile_uri_id NULL`, `created_at` |
| `actor_aggs` | `did_id PK`, `follows`, `followers`, `blocks_out`, `blocks_in`, `posts`, `likes_out`, `likes_in`, `reposts_out`, `reposts_in`, `replies_out`, `quotes_out`, `quoted_count` |
| `follows` | `uri_id PK`, `src_did_id`, `dst_did_id`, `created_at` |
| `blocks` | `uri_id PK`, `src_did_id`, `dst_did_id`, `created_at` |
| `posts` | `uri_id PK`, `uri STRING`, `author_did_id`, `created_at`, `reply_root_uri_id NULL`, `reply_parent_uri_id NULL`, `quote_uri_id NULL` |
| `post_aggs` | `uri_id PK`, `likes`, `reposts`, `replies`, `quotes` |
| `post_media` | `uri_id`, `ord`, `kind ENUM('image','video','external','record')`, `ref_url NULL`, `blob_cid NULL` |
| `likes` | `uri_id PK`, `actor_did_id`, `subject_uri_id`, `created_at` |
| `reposts` | `uri_id PK`, `actor_did_id`, `subject_uri_id`, `created_at` |

Notes:

- Counts in `actor_aggs` and `post_aggs` are **all-time within the snapshot**
  — there is no per-day window in this spec because no streaming data is
  ingested. Window semantics return when streaming is back in scope.
- Foreign-key relationships are by `*_id` columns. DuckDB does not enforce FKs
  at insert; the hydrate stage runs orphan-rate / null-rate checks against
  thresholds from config and fails the build if breached.
- No post text, profile bio, display name, or alt text — those tables /
  columns will be added by a future spec.

## Operational considerations

- **Idempotent**: every stage is safe to re-run. Outputs are written via
  temp + rename; the duckdb file is built fresh per run.
- **Bounded RAM**: configurable `memory_limit` for DuckDB, configurable
  `batch_size` for parquet writers, streaming reads from rocks.
- **Resumable**: mirror checkpoints constellation's cursor; stage and hydrate
  are deterministic given the rocks state, so a re-run reproduces the same
  outputs.
- **Single-binary, modular internals**: each stage lives behind a `Stage`
  trait so the future split into subcommands is mechanical.
- **Logging**: every stage emits INFO-level structured progress events
  (counts, bytes, batch index) so `--monitor-addr` and a log-tailing sidecar
  can both report status.

## Validation

A small recorded RocksDB fixture is committed under `tests/fixtures/`,
covering each entity plus a quote chain, a reply chain, an external embed,
and a deleted-then-recreated record. `cargo test` runs the full pipeline
against the fixture using the local-filesystem object store and asserts
against the same analytic queries as prior iterations of this spec:

- How many people followed or blocked someone else yesterday?
- What % of total follows were generated yesterday?
- How many people who posted got at least one like?
- How many posts got at least one like?

Per-stage unit tests additionally cover the rocks schema decoder
(`src/rocks/schema.rs`) and parquet writers in isolation.

## Reference implementations

- Constellation: https://tangled.org/microcosm.blue/microcosm-rs
- RocksDB schema:
  https://tangled.org/microcosm.blue/microcosm-rs/blob/main/constellation/src/storage/rocks_store.rs
- eat-rocks: https://tangled.org/microcosm.blue/eat-rocks
- Prior art: https://docs.bsky.app/blog/introducing-tap

## Implementation guidance

- Use `tokio` for async I/O at stage boundaries (object store, http monitor),
  and plain threads + channels for the rocks → parquet pipeline.
- `anyhow` at binary edges, `thiserror` in libraries.
- One module per stage (`mirror.rs`, `stage.rs`, `hydrate.rs`, `publish.rs`),
  orchestrated through a single `Stage` trait.
- Keep all rocks schema knowledge in exactly one module.
- Red/green tests; commit each logical unit separately for a readable history.

## README updates

Update `README.md` to be concise with:

- The goal this repo solves.
- The mental-model overview (mirror → stage → hydrate → publish).
- The ERD in mermaid for the tables above.
- The single CLI command and its flags.
- A "Deferred" section listing non-goals (streaming, text, media, filters) so
  users know what's intentionally missing.
