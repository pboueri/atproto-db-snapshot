# ATProto Snapshotter
This is a work in progress. The goal is to reduce the cost of doing high quality analytics on Bluesky

This job produces a public DuckDB snapshot of the Bluesky social graph plus
post-relationship graph, derived end-to-end from the
[microcosm.blue constellation](https://tangled.org/microcosm.blue/microcosm-rs)
RocksDB backlinks index.

The current snapshot [is here](https://pub-5ef34deaa1e54c25a97cea1bcfbd6456.r2.dev/atproto-snapshot/snapshot/2026-07-31/snapshot.duckdb)
(2026-07-31, 60.4 GB). It has all actors/blocks/follows; posts/likes/reposts
are windowed to 2026-05-02 → 2026-07-31 (90 days).

| Table | Rows |
|---|---:|
| `actors` | 105,515,262 |
| `actor_aggs` | 26,180,082 |
| `follows` | 1,463,484,958 |
| `blocks` | 133,295,503 |
| `likes` | 1,626,049,718 |
| `reposts` | 250,557,929 |
| `posts` | 271,086,751 |
| `post_aggs` | 271,086,751 |

`actors` is much larger than `actor_aggs` because it now carries every DID in
the PLC directory, not just the ones constellation indexed — see
`in_microcosm` in the table notes below. Windowed counts move with the window,
not just with time: this snapshot has slightly fewer likes than the 2026-05-11
one because May–July was a quieter 90 days than February–May.

The previous snapshot (2026-05-11, 24.7 M actors, no `created_at`) is still at
the [same path with its own date](https://pub-5ef34deaa1e54c25a97cea1bcfbd6456.r2.dev/atproto-snapshot/snapshot/2026-05-11/snapshot.duckdb).


## Tables

| Table        | What it is |
|---|---|
| `actors`     | One row per DID. `did_id` is a stable u64 used by every other table. `created_at` / `tombstoned_at` come from the PLC directory export; `in_microcosm` is FALSE for DIDs that exist on PLC but that constellation never indexed (they follow/post/like nothing we saw), so filter on it when you want only accounts with graph activity. Both timestamps are NULL for `did:web` actors, which PLC doesn't cover. |
| `follows`    | `src_did_id` follows `dst_did_id`. |
| `blocks`     | `src_did_id` blocks `dst_did_id`. |
| `likes`      | `actor_did_id` liked `subject_uri_id` (NULL when the subject isn't a post). |
| `reposts`    | `actor_did_id` reposted `subject_uri_id`. |
| `posts`      | One row per unique post URI. `uri_id` (u64) is the canonical post key; `reply_parent_uri_id`, `reply_root_uri_id`, `quote_uri_id` reference other posts. `source = 'record'` if we saw the post body, `'target_only'` if we only saw it as the target of a like/reply/quote. |
| `actor_aggs` | Per-actor counts: follows / followers, blocks in/out, posts, likes in/out, reposts in/out, replies, quotes. |
| `post_aggs`  | Per-post counts: likes, reposts, replies, quotes. |

All inter-table joins go through `did_id` (actors) or `uri_id` (posts).
URI strings live only on `posts`.

## Stages

1. **mirror** — copy the constellation rocksdb to `./var/rocks/`. Incremental:
   it size-diffs against the existing mirror and pulls only what changed.
2. **stage** — read the rocks mirror and write per-entity parquet under `./var/raw/<date>/`.
3. **plc** — stream the [PLC directory](https://plc.directory) export into
   `./var/raw/<date>/plc/`, capturing account creation and tombstone ops.
   Checkpointed and resumable. Independent of the rocks mirror, so it runs
   concurrently with mirror → stage.
4. **hydrate** — load the parquet into `./var/snapshot/<date>/snapshot.duckdb`.
   Uses the PLC shards to add `created_at` / `tombstoned_at` to `actors`;
   skips that enrichment (leaving the columns off) when the `plc` stage
   didn't run.
5. **upload** — push `raw/<date>` and `snapshot/<date>` to an S3-compatible store (R2).

`at-snapshot build` runs plc + mirror → stage → hydrate. Each stage can also be
run on its own.
