# ATProto Snapshotter

Builds a DuckDB snapshot of the Bluesky social graph from the
[microcosm.blue constellation](https://tangled.org/microcosm.blue/microcosm-rs)
backlinks index.

## Tables

| Table        | What it is |
|---|---|
| `actors`     | One row per DID. `did_id` is a stable u64 used by every other table. |
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

1. **mirror** — copy the constellation rocksdb to `./var/rocks/`.
2. **stage** — read the rocks mirror and write per-entity parquet under `./var/raw/<date>/`.
3. **hydrate** — load the parquet into `./var/snapshot/<date>/snapshot.duckdb`.
4. **upload** — push `raw/<date>` and `snapshot/<date>` to an S3-compatible store (R2).

`at-snapshot build` runs mirror → stage → hydrate. Each stage can also be
run on its own.
