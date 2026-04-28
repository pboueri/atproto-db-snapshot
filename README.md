# ATProto Snapshotter

Produces a public DuckDB snapshot of the Bluesky social graph plus
post-relationship graph, derived end-to-end from the
[microcosm.blue constellation](https://tangled.org/microcosm.blue/microcosm-rs)
RocksDB backlinks index. Cheap, no-ceremony analytics on commodity hardware.

## Mental model

```
constellation rocksdb  --(eat-rocks)-->  local rocks mirror
local rocks mirror     --(stage)----->  staging parquet (per entity)
staging parquet        --(hydrate)--->  snapshot.duckdb
snapshot.duckdb        --(upload)---->  object store (R2)
```

Each stage is a single-responsibility module with its own CLI
subcommand. `build` runs all three in sequence; you can also invoke
each phase on its own to resume mid-pipeline against an existing work
directory.

## CLI

```
at-snapshot <subcommand> [flags]
```

Subcommands:

| Subcommand | What it does |
|---|---|
| `build`   | Run mirror → stage → hydrate. |
| `mirror`  | Mirror the constellation rocksdb to `./var/rocks/`. |
| `stage`   | Convert the rocks mirror into per-entity parquet under `./var/raw/<date>/`. |
| `hydrate` | Build `./var/snapshot/<date>/snapshot.duckdb` from the parquet. |
| `upload`  | Push `raw/<date>` + `snapshot/<date>` to a configured object store. |

Every subcommand accepts the same flags (unused flags are silently
ignored, so a single config file + flag set drives any phase):

| Flag | Default | Notes |
|---|---|---|
| `--config <path>` | — | TOML config file. CLI flags override its values. |
| `--work-dir <path>` | `./var` | Local working directory. |
| `--snapshot-date <YYYY-MM-DD>` | today UTC | Namespace for outputs. |
| `--memory-limit <size>` | `4GiB` | DuckDB memory cap. |
| `--batch-size <n>` | `100000` | Parquet writer batch size. |
| `--source-url <url>` | `https://constellation.t3.storage.dev` | eat-rocks source. |
| `--backup-id <u64>` | latest | Pin a specific constellation backup. |
| `--mirror-concurrency <n>` | `32` | eat-rocks fetch concurrency. Drop to 8 if you hit timeouts. |

### Upload (R2)

`at-snapshot upload` pushes the staged + hydrated artifacts to an
S3-compatible object store. Cloudflare R2 is the only built-in backend
today; the abstraction is generic so other S3-compatible stores can be
added.

Secrets come from env. Everything else lives in the config TOML:

```toml
[upload]
kind       = "r2"
bucket     = "atproto-snapshots"
account_id = "abc123..."        # endpoint derived from this
prefix     = "atproto-snapshot" # path prefix in bucket
concurrency = 8
include    = ["raw", "snapshot"]
```

```sh
export R2_ACCESS_KEY_ID=...
export R2_SECRET_ACCESS_KEY=...
at-snapshot upload --config at-snapshot.toml --snapshot-date 2026-04-27
```

## System dependencies

| Tool | Purpose | How it links |
|---|---|---|
| `libduckdb` 1.5.x | Hydrate stage uses the `duckdb` Rust crate, dynamically linked. | system shared library (`libduckdb.dylib`) |
| RocksDB | Stage stage opens the constellation mirror read-only via the `rocksdb` crate. | vendored (`librocksdb-sys` 0.16+8.10) |

### libduckdb (system, dynamic link)

Homebrew's `duckdb` formula ships only the CLI binary. Grab the official
release zip with the headers and shared library:

```sh
mkdir -p ~/.local/duckdb-1.5.2/lib ~/.local/duckdb-1.5.2/include
cd /tmp
curl -L -o libduckdb.zip https://github.com/duckdb/duckdb/releases/download/v1.5.2/libduckdb-osx-universal.zip
unzip -q libduckdb.zip
cp libduckdb.dylib ~/.local/duckdb-1.5.2/lib/
cp duckdb.h duckdb.hpp ~/.local/duckdb-1.5.2/include/
```

Then point Cargo at it via `.cargo/config.toml`:

```toml
[env]
DUCKDB_LIB_DIR     = "/Users/<you>/.local/duckdb-1.5.2/lib"
DUCKDB_INCLUDE_DIR = "/Users/<you>/.local/duckdb-1.5.2/include"
LIBCLANG_PATH      = "/Library/Developer/CommandLineTools/usr/lib"
```

When you run the resulting binary you may need
`DYLD_LIBRARY_PATH=$HOME/.local/duckdb-1.5.2/lib` so the loader can find
the dylib at runtime. No long C++ amalgamation compile.

### RocksDB (bundled)

The latest `librocksdb-sys` (0.17.x) targets rocksdb 10.4.2 while
Homebrew ships 11, whose C API is incompatible. We pin
`rocksdb = "0.22"` / `librocksdb-sys 0.16+8.10` and let it build from
source. Cargo caches the resulting library so it compiles **once**
(~2 min), then incremental builds are seconds. If you want to switch to
a system rocksdb, set `ROCKSDB_LIB_DIR` / `ROCKSDB_INCLUDE_DIR` and
bump the crate to a matching major.

Everything else is pure Rust.

## Output layout

```
var/
  rocks/                              # local rocksdb mirror (~80 GB)
  raw/<date>/
    actors.parquet
    follows.parquet
    blocks.parquet
    likes.parquet
    reposts.parquet
    posts_from_records.parquet
    posts_from_targets.parquet
    post_media.parquet
  snapshot/<date>/
    snapshot.duckdb
    snapshot_metadata.json
```

Query it with `duckdb var/snapshot/<date>/snapshot.duckdb`.

## Tables (ERD)

```mermaid
erDiagram
    actors    ||--o{ follows  : "src_did_id"
    actors    ||--o{ blocks   : "src_did_id"
    actors    ||--o{ likes    : "actor_did_id"
    actors    ||--o{ reposts  : "actor_did_id"
    actors    ||--o{ posts    : "author_did_id"
    posts     ||--o{ likes    : "subject_uri_id = uri_id"
    posts     ||--o{ reposts  : "subject_uri_id = uri_id"
    posts     ||--o{ post_media : "uri_id"
    posts     ||--o| posts    : "reply / quote (*_uri_id)"
    actors    ||--|| actor_aggs : did_id
    posts     ||--|| post_aggs  : uri_id
```

`posts` is the source of truth for post URIs: every unique URI is assigned
a sequential `uri_id` (BIGINT). All other tables reference posts via
`uri_id` and never store the URI string — join on `posts.uri_id` to recover
the URI. `subject_uri_id` on `likes` / `reposts` is NULL when the subject
isn't a post (orphan; typically a list or feed URI). Reply / quote
references on posts are likewise `_uri_id` columns resolved via self-join.

`source` on `posts` is `'record'` when we saw the post via constellation's
`link_targets` (so reply/quote refs are present) or `'target_only'` when we
only saw it as the target of someone else's like / reply / quote. Hydrate
prefers `record` rows when deduping by `uri`.

## Deferred (intentionally out of scope)

- Streaming / incremental updates (jetstream). Each snapshot is a full
  rebuild from a constellation backup.
- Post text, profile bios, display names, alt text. Constellation is a
  backlinks index; record bodies are not in scope until we add a separate
  ingest path.
- Media blob downloads / CDN mirroring.
- Language / labeler filters (will return when text is back in scope).
- Lists, feeds and feed generators, threadgates, starter packs.

## Takedown requests

For takedowns please raise a GitHub issue and we will exclude the relevant
rows from future snapshots.
