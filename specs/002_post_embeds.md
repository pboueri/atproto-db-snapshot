# Post embeds: structured sidecar table

Plan to widen what we capture for post embeds beyond the existing
`posts.embed_type` discriminator. Adds one new sidecar table
`post_embeds` to `current_all.duckdb`, one new daily parquet shard
`post_embeds.parquet`, and the corresponding write/read paths.

This is purely additive on top of `001_bootstrap.md`:
- `current_graph.duckdb` is unchanged.
- `posts.embed_type` is unchanged — analysts can still answer "what
  fraction of posts have any embed?" without joining the sidecar.
- The parquet manifest stays at `schema_version: "v1"` — adding a new
  optional file is non-breaking; consumers that don't know about the new
  shard ignore it.

Driving use cases:

- **External link analytics.** Today an `external` embed is recorded as
  the literal string `"external"` in `posts.embed_type` — the URL is
  thrown away. The single most-asked Bluesky analytics question
  ("what's being shared?") needs the URL. Storing the host separately
  (pre-computed) makes `GROUP BY external_domain` cheap.
- **Image accessibility.** Counting images and the subset with non-empty
  alt text answers "what fraction of image posts have alt text?" without
  looking at content.
- **Media-bearing quote posts.** `recordWithMedia` posts today resolve
  to the literal string `"recordWithMedia"`; the inner media kind
  (images / video / external) is lost. The new `kind` column encodes
  both layers in one string, e.g. `recordWithMedia:external`.

Privacy posture inherited from §1: we capture **structure and counts**,
not user-generated content. Specifically:

- Image alt-text **content** is not stored (only the count of images
  with non-empty alt). Alt text is a known PII / harassment surface.
- Embedded thumbnail blob CIDs are not stored.
- External title is captured (it's published metadata about a third-party
  page); description is not (long-form text adds storage without much
  analytics value).

---

## 1. Schema

### 1.1 `current_all.duckdb` — new table

```sql
CREATE TABLE IF NOT EXISTS post_embeds (
  author_id            BIGINT NOT NULL,
  rkey                 VARCHAR NOT NULL,
  kind                 VARCHAR NOT NULL,
  external_uri         VARCHAR,
  external_domain      VARCHAR,
  external_title       VARCHAR,
  image_count          SMALLINT,
  image_with_alt_count SMALLINT,
  video_has_alt        BOOLEAN,
  PRIMARY KEY (author_id, rkey)
);
```

**Cardinality.** One row per post that has *any* embed. Posts with no
embed have no row. The `(author_id, rkey)` key matches `posts` — joinable
without an intermediate.

**`kind` discriminator.** Exactly one of:

| `kind`                       | `posts.embed_type`  | Notes                                 |
|------------------------------|---------------------|---------------------------------------|
| `images`                     | `images`            | `image_count` ≥ 1                     |
| `video`                      | `video`             | `video_has_alt` set                   |
| `external`                   | `external`          | `external_uri` set                    |
| `record`                     | `record`            | quote target lives in `posts.quote_*` |
| `recordWithMedia:images`     | `recordWithMedia`   | inner images populated                |
| `recordWithMedia:video`      | `recordWithMedia`   | inner video populated                 |
| `recordWithMedia:external`   | `recordWithMedia`   | inner external populated              |

**Why no `quote_*` columns here.** `posts` already stores the quoted
post as `quote_author_id` / `quote_rkey` for both `record` and
`recordWithMedia` embeds. Duplicating it in the sidecar adds redundancy
without adding queryability.

**SMALLINT for counts.** Posts have at most 4 images per the Bluesky
lexicon. SMALLINT is plenty and keeps the sidecar tight.

### 1.2 Daily parquet — new shard

`<dayDir>/post_embeds.parquet`. Schema:

| column                | type        |
|-----------------------|-------------|
| `date`                | DATE        |
| `uri`                 | VARCHAR     |
| `did`                 | VARCHAR     |
| `rkey`                | VARCHAR     |
| `event_ts`            | TIMESTAMP   |
| `kind`                | VARCHAR     |
| `external_uri`        | VARCHAR     |
| `external_domain`     | VARCHAR     |
| `external_title`      | VARCHAR     |
| `image_count`         | SMALLINT    |
| `image_with_alt`      | SMALLINT    |
| `video_has_alt`       | BOOLEAN     |

Row sort within row groups: `did, rkey` (matches `posts.parquet`).
Compression: zstd level 6, 128 MB row groups (same as every other shard
per `001_bootstrap.md` §6).

**Empty-day handling.** If no post in the day's `staging_events_post`
has an `embed`, the file is **not** written (consistent with how
`run/parquet.go` handles other empty shards). Manifest's `row_counts`
includes `post_embeds.parquet: 0` so consumers can distinguish "no
embeds today" from "old build that didn't emit this shard."

---

## 2. Write paths

### 2.1 Live ingester (`run/parquet.go`)

The Jetstream → SQLite-staging → DuckDB-COPY path is the production
write path. We add a `postEmbedSelect` that reads from the same
`staging_events_post` table as `postSelect`, projecting the embed
fields out of `record_json`. The `WHERE` clause filters to rows with a
non-null `$.embed` so the parquet stays small.

Notable extraction details:

- `external_domain` is pre-computed at write time via
  `regexp_extract(uri, '^https?://([^/]+)', 1)`. Doing it once per row
  here is much cheaper than repeating it in every analytics query.
- `image_count` reads `len(json_extract(record_json, '$.embed.images'))`
  (or `$.embed.media.images` for `recordWithMedia`).
- `image_with_alt_count` is approximated by counting matches of the
  pattern `"alt":"[^"]+"` inside the `images` array. This is a simple
  count — alt text **content** is never stored.
- `recordWithMedia:<kind>` is built as `'recordWithMedia:' ||
  <inner-media-kind>`.

### 2.2 Typed-struct path (`archive/posts.go`)

For symmetry with the rest of the `archive` package — and to give
future CAR-emit paths a typed entrypoint — `Writer` gets a
`WritePostEmbeds(ctx, day, []PostEmbedEvent)` method backed by the
existing DuckDB Appender pattern. Not currently called from a
production code path; exists for testability and to lock the parquet
schema in a single place.

### 2.3 CAR backfill (`parse/car.go`)

`PostRec` is widened with structured embed fields (image count, image
with-alt count, video alt presence, external URI, external title, and
the inner-media kind for `recordWithMedia`). Today no production code
consumes `Records.Posts` (CAR backfill is graph-only per §7), but
keeping the parser's output complete avoids the next person hitting a
"why isn't this in the struct?" surprise.

---

## 3. Read path — incremental build

`build/incremental.go` gains an `applyPostEmbeds` step that runs
inside the existing per-day transaction, **after** `applyPosts`. It
reads `post_embeds.parquet`, joins `actors_registry` for the
`author_id`, and joins `posts` (already inserted in the same txn) so
that any post the lang filter dropped also drops its sidecar row
automatically — no separate filter to keep in sync.

If `post_embeds.parquet` is absent (old archive day) the step is a
no-op; no error.

`exclude_collections: [app.bsky.feed.post]` skips embeds too — there's
no separate NSID for embeds, they ride along with posts.

---

## 4. Takedowns

`build/takedowns.go` is extended so that the `app.bsky.feed.post`
branch deletes the sidecar row in addition to nullifying `posts`
content. Deleting (rather than nullifying) is appropriate because:

- The sidecar is purely metadata — no foreign keys reference it.
- `external_uri` is a primary takedown target on its own (DMCA URL
  reports, malicious links). Nulling individual columns and keeping
  the row isn't more valuable than removing the row.

The audit row in `takedowns_applied` is unchanged — one entry per URI
processed, regardless of which tables it touched.

---

## 5. Migration

`OpenAll` runs `CREATE TABLE IF NOT EXISTS post_embeds (...)` on
every open, so existing `current_all.duckdb` files gain the empty
table on first open. The next nightly `build` populates it for any
day whose `post_embeds.parquet` exists.

For days whose archive predates this spec there will be no
`post_embeds.parquet` and the sidecar will simply have no rows for
those posts. There is no backfill — the historical embed detail
isn't available in our parquet archive (it was never extracted).
Operators who care about historical embed data can re-run a
`-mode force-rebuild` against a CAR archive once CAR-backfill paths
emit `post_embeds.parquet` (out of scope for this spec).

---

## 6. Out of scope for this spec

- **Image alt-text content.** Stored neither in `post_embeds` nor in
  `post_embeds.parquet`. Adding it would require a separate takedown
  story per-image and is a privacy expansion.
- **AspectRatio.** Niche analytics value, free to add later.
- **Captions on video.** Same.
- **Self-applied record labels** (`labels` field on records). Belongs
  in a separate `record_labels` table, tangled with the labeler
  subscription (deferred per §1 of `001_bootstrap.md`).
- **Embedded thumbnail blob CIDs.** We don't fetch blobs anywhere; the
  CID alone has no analytics value.
