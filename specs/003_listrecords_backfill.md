# listRecords graph backfill

Replace the per-DID `com.atproto.sync.getRepo` CAR fetch with a per-PDS,
per-collection `com.atproto.repo.listRecords` walk. Enumerate DIDs via
the PLC export instead of the relay's `listRepos`. Optionally enrich
`actors.follower_count` from Constellation.

This supersedes the graph-backfill path defined in
[`001_bootstrap.md`](001_bootstrap.md) §7. The output schema, parquet
archive, incremental build path, takedowns, and Jetstream consumer are
unchanged.

## 1. Why

Measured against the prior implementation:

| approach                              | observed throughput | full-network ETA |
| ------------------------------------- | ------------------- | ---------------- |
| `getRepo` via bsky.network (60s tmo)  | ~1.7 fetched/sec    | ~115 days        |
| `getRepo` via bsky.network (15s tmo)  | ~3.1 fetched/sec    | ~93 days         |
| `listRecords` per-PDS (target)        | ~500 req/sec        | ~46 hours        |

Three fixes compound:

- **listRecords is JSON, not CAR.** Most accounts have <300 follows; one
  to three small JSON pages replaces a multi-megabyte CAR download.
- **Bsky operates ~50 first-party PDSes** (`*.us-east.host.bsky.network`,
  `*.us-west.host.bsky.network`). The documented per-IP rate limit is
  3000 req / 5 min **per host**. Bucketing workers by destination host
  unlocks ~50× more parallelism than a single shared pool.
- **PLC export is more complete than `listRepos`.** atproto issue #3433
  documented `bsky.network`'s `listRepos` missing ~10M DIDs. PLC's
  `/export` endpoint is the canonical enumeration.

## 2. Architecture

```
┌─────────────────────┐
│ plc.directory/export│    paginate, replay ops, derive (did, pds)
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐    persist to actors_pds (DID -> endpoint)
│ pds_endpoints table │    in current_graph.duckdb
└──────────┬──────────┘
           │  bucket by host
           ▼
┌─────────────────────┐    M workers per PDS host
│  per-PDS dispatcher │    each calls listRecords for:
└──────────┬──────────┘      app.bsky.actor.profile/self
           │                  app.bsky.graph.follow (paginated)
           │                  app.bsky.graph.block  (paginated)
           ▼
┌─────────────────────┐    same parse.Records writer as today
│   writer goroutine  │    writes actors / follows_current / blocks_current
└──────────┬──────────┘
           │  optional
           ▼
┌─────────────────────┐    /links/all?target=<did>
│ Constellation enrich│    fills actors.follower_count
└─────────────────────┘
```

## 3. PLC enumeration (`internal/plc`)

`GET https://plc.directory/export?count=1000&after=<cursor>` returns
JSONL of operations. Each line:

```json
{
  "did": "did:plc:...",
  "operation": {
    "type": "plc_operation",
    "services": {
      "atproto_pds": {"type": "AtprotoPersonalDataServer", "endpoint": "https://amanita.us-east.host.bsky.network"}
    },
    ...
  },
  "cid": "...",
  "nullified": false,
  "createdAt": "..."
}
```

A DID may have many ops (key rotation, PDS migration, handle change,
tombstone). We track **the last non-nullified op per DID** in memory —
its `services.atproto_pds.endpoint` is the current PDS. Tombstones
(`type: "plc_tombstone"`) mark a DID as deleted; we drop it from the
working set.

Cursor is integer sequence (Jan 2026 spec update). Persist to
`<DataDir>/plc_cursor.json` after every successful page so a restart
resumes from the last completed page.

Persistence: once enumeration completes (or after every ~250k DIDs
processed), upsert `(did, endpoint, resolved_at)` rows into the
`pds_endpoints` table in `current_graph.duckdb`. Resume picks up by
reading this table.

Pacing: 5 req/sec sustained against `plc.directory` (well under any
reasonable etiquette limit). User-Agent is mandatory:
`atproto-db-snapshot/0.1 (+https://github.com/pboueri/atproto-db-snapshot)`.

## 4. Per-PDS dispatch (`internal/pds`)

After enumeration, partition DIDs by `endpoint`. For each host start a
worker pool of size `cfg.PDS.PerHostWorkers` (default 8). All hosts run
concurrently — total in-flight is `len(hosts) * PerHostWorkers`.

Per-host rate limit: `cfg.PDS.PerHostRPS` (default 9 req/sec — leaves
margin under the documented 10/sec / 3000 per 5 min limit). On HTTP 429,
respect `Retry-After` if present, otherwise back off 5s, then 10s, then
30s; cap retries at 4. Treat repeated 429 as a circuit-breaker signal:
after 5 consecutive 429s on a host, sleep that host for 60s.

Per-host circuit breaker for dead PDSes: track consecutive non-429
failures; after 5 in a row, mark host `unavailable` for 5 minutes and
fast-fail subsequent fetches for it. After cooldown, reset and try one
probe; success clears the breaker.

`http.Transport` tuning per client:
- `MaxIdleConnsPerHost = 16`
- `MaxConnsPerHost = 32`
- `IdleConnTimeout = 90s`
- HTTP/2 enabled (Go default)

### 4.1 Calls per DID

Three sequential calls per DID, one per collection. Each is paginated:

```
GET <endpoint>/xrpc/com.atproto.repo.listRecords?repo=<did>&collection=app.bsky.actor.profile&limit=1
GET <endpoint>/xrpc/com.atproto.repo.listRecords?repo=<did>&collection=app.bsky.graph.follow&limit=100[&cursor=<rkey>]
GET <endpoint>/xrpc/com.atproto.repo.listRecords?repo=<did>&collection=app.bsky.graph.block&limit=100[&cursor=<rkey>]
```

Response:

```json
{"cursor": "...", "records": [{"uri": "at://did/coll/rkey", "cid": "...", "value": {...}}]}
```

`value` shapes:

- `app.bsky.actor.profile`: `{displayName?, description?, avatar?, createdAt?}`
- `app.bsky.graph.follow`: `{subject: "did:plc:...", createdAt: "..."}`
- `app.bsky.graph.block`:  `{subject: "did:plc:...", createdAt: "..."}`

### 4.2 Failure semantics

| status                                   | action                                                   |
| ---------------------------------------- | -------------------------------------------------------- |
| 200                                      | record + advance cursor                                  |
| 400 `RepoNotFound`/`InvalidRequest`      | skip DID, mark `repo_processed=true` so we don't retry   |
| 404                                      | skip DID, log Debug                                      |
| 429                                      | retry with backoff per §4                                |
| 5xx                                      | retry with exponential backoff (1s, 2s, 4s, 8s)          |
| timeout / connection error               | retry, then circuit breaker per §4                       |
| any other                                | log Warn, increment errors counter, skip DID             |

`http-timeout` default drops to 10s for listRecords (small JSON
responses; if a PDS hasn't answered in 10s it's effectively dead).

## 5. Constellation enrichment (`internal/constellation`)

Optional. Behind `-use-constellation` (default off). When enabled, after
the listRecords pass, fan out:

```
GET https://constellation.microcosm.blue/links/all?target=<did>
User-Agent: atproto-db-snapshot/0.1 (+...)
```

Response is `{collection: {path: count}}`. We map:

- `app.bsky.graph.follow.subject`  → `actors.follower_count`
- `app.bsky.graph.block.subject`   → `actors.blocks_received_count` (new column? see §6)
- `app.bsky.feed.like.subject`     → `actors.likes_received_count`
- `app.bsky.feed.repost.subject`   → `actors.reposts_received_count`

Pace at `cfg.Constellation.RPS` (default 5 req/sec — Constellation runs
on a Pi at the maintainer's house; be polite). Run after the main pass
so a Constellation outage doesn't block the crawl.

## 6. Schema changes

### 6.1 New table in `current_graph.duckdb`

```sql
CREATE TABLE IF NOT EXISTS pds_endpoints (
  did         VARCHAR PRIMARY KEY,
  endpoint    VARCHAR NOT NULL,
  resolved_at TIMESTAMP NOT NULL
);
```

Populated by the PLC enumeration phase. Read on resume to skip a fresh
PLC dump if `cfg.PLC.RefreshDays` hasn't elapsed since the most recent
`resolved_at`.

### 6.2 No change to `actors`, `follows_current`, `blocks_current`

`blocks_received_count` is **not** added in this spec; deferred. The
follower-count enrichment is opt-in and writes to existing
`actors.likes_received_count` / `actors.reposts_received_count` only
when `-use-constellation` is set.

## 7. Resume

| state                                    | persistence                                  | resume action                                 |
| ---------------------------------------- | -------------------------------------------- | --------------------------------------------- |
| PLC enumeration progress                 | `<DataDir>/plc_cursor.json` (atomic rename)  | resume from cursor                            |
| (DID, PDS) mapping                       | `pds_endpoints` table                        | skip enumeration if recent (`RefreshDays`)    |
| listRecords completion per DID           | `actors.repo_processed = TRUE`               | skip DIDs already marked processed            |
| Constellation enrichment per DID         | bumped value in `actors.*_count`             | re-run is idempotent (overwrites)             |

## 8. Configuration

New `Config` fields (`internal/config/config.go`):

```go
PLC struct {
    Endpoint     string        // default "https://plc.directory"
    PageSize     int           // default 1000
    RPS          float64       // default 5
    RefreshDays  int           // default 7 — re-dump PLC if older
}

PDS struct {
    PerHostWorkers     int           // default 8
    PerHostRPS         float64       // default 9
    HTTPTimeout        time.Duration // default 10s
    MaxRetries         int           // default 4
    BreakerThreshold   int           // default 5 consecutive failures
    BreakerCooldown    time.Duration // default 5m
}

Constellation struct {
    Endpoint string  // default "https://constellation.microcosm.blue"
    RPS      float64 // default 5
}
```

Removed: `Config.RelayHost`, `Config.Workers` (replaced by `PDS.PerHostWorkers`),
`Config.RateLimitRPS` (replaced by `PDS.PerHostRPS`). `HTTPTimeout` repurposed
to PLC client timeout.

CLI flags (`cmd/at-snapshotter/main.go`):

| flag                          | replaces / new                                       |
| ----------------------------- | ---------------------------------------------------- |
| `-plc-endpoint`               | new                                                  |
| `-plc-rps`                    | new                                                  |
| `-plc-refresh-days`           | new                                                  |
| `-pds-workers-per-host`       | replaces `-workers`                                  |
| `-pds-rps-per-host`           | replaces `-rps`                                      |
| `-http-timeout`               | repurposed (PLC client only)                         |
| `-pds-timeout`                | new (listRecords client)                             |
| `-use-constellation`          | new (default false)                                  |
| `-constellation-endpoint`     | new                                                  |
| `-limit`                      | unchanged (cap DIDs processed)                       |
| `-relay`                      | **removed**                                          |

## 9. Removals

- `internal/relay/` — entire package; no other callers.
- `internal/parse/car.go`, `car_test.go` — CAR parsing only used by old
  `getRepo` path.
- `internal/parse/types.go` — **kept** as `parse.Records`/`parse.FollowRec`
  etc. The new listRecords producer emits these same types, so the
  writer (`build/build_graph.go::writeOne` and `writerLoop`) is reused
  unchanged.

## 10. Etiquette

- Constant `User-Agent` on every outbound request:
  `atproto-db-snapshot/0.1 (+https://github.com/pboueri/atproto-db-snapshot)`
- Honor `Retry-After`. Never tighter than 1 sec between retries on the
  same key.
- Constellation: cap at 5 req/sec, no parallelism beyond that. The
  service runs on a Pi; respect that.
- If `bnewbold@bsky.app` opens an issue asking us to slow down, we slow
  down.

## 11. Testing

Unit:
- `internal/plc`: pagination, cursor persistence, op replay (last-write-wins).
- `internal/pds`: pagination, 429 retry, circuit breaker, JSON decode of
  `value` payloads into `parse.Records`.
- `internal/constellation`: response decode, error path.

Integration:
- `internal/build`: graph backfill against an `httptest.Server` that
  stubs PLC + 2 PDS hosts. Asserts the expected actors/follows/blocks
  rows land in DuckDB and that resume skips already-processed DIDs.

## 12. Out of scope

- Pre-resolved DID→PDS map persisted across multiple snapshots
  (could share via R2; deferred).
- Hubble integration as a `getRepo` backstop (per the bootstrap report,
  Hubble isn't GA yet).
- Spacedust/Jetstream-shaped link feed (live link deltas — covered by
  the existing Jetstream consumer, not a backfill problem).
- Multi-IP fanout for >500 req/sec aggregate.
