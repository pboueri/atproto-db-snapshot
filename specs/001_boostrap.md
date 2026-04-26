# ATProto Analytic Snapshot

## Goal

The goal of the AT Proto Analytics snapshot is to consume the atmosphere and publish a windowed snapshot of all posts and the follower graph for public consumption to do analytics in an easy-to-use manner without too much ceremony. This will be hosted on a public object store and backed up nightly. This will be running on commodity. The program will be described below

## Overall Flow

The way this project will work is there's an initial bootstrap which will generate the entire follower graph As a starting point, then it will start consuming from the jet stream and write posts, likes, follows, deletes, and other information incrementally from the initial bootstrap

## Final Output

The final output will be the following files in object storage, with R2 being the default implementation. The fields and schema of the files are described in later sections.

data/
- bootstrap/YYYY-MM-DD/
  - social_graph.duckdb # Written once for bootstrap; never overwritten
- raw/YYYY-MM-DD/{posts, post_media, likes, reposts, follows, blocks, profiles}.parquet
  # post_media is derived from post record embeds, not a separate firehose collection
- snapshot/
  - current_all.duckdb
  - current_graph.duckdb
  - snapshot_metadata.json  

## Technologies to use:

- Programming language: Go
- Data storage:
  - local_staging: sqlite or json for cursor metadata -- use the local filesystem for any working data.
  - date partitioned raw_records for backfill: parquet -- written to object storage and a buffer kept locally
    - example data/YYYY-MM-DD/{likes, follows, profiles, posts}.parquet
    - All the data generated just on that day
  - analytic_database: duckdb
    - The final output that is snapshotted with N days of history, generated from raw_records (default 30)
    - current_all.duckdb: The full social graph + posts
    - current_graph.duckdb: The social graph no posts

## Workflow + CLI commands

### at-snapshot bootstrap
This command bootstraps the social graph from historicals and writes it once to `bootstrap/YYYY-MM-DD/social_graph.duckdb`. The file is never overwritten after a successful bootstrap; subsequent `snapshot` runs read it as the baseline and apply deltas from `raw/`. If interrupted it resumes from the in-progress `social_graph.duckdb` and assumes any DIDs already present are complete. It generates the following tables:
  - Actors / Follows / Blocks
Profile, follow, and block records are pulled from constellation per DID. It does not backfill posts, likes, or reposts -- doing so would 10x the requirements. Post engagement aggregates exist only for the window of data in the snapshot.

### at-snapshot run
This command is a long running process to read from the jetstream and write out the parquet files and upload them to the object storage. It's meant to be fault tolerant and persistent. It keeps RAM requirements low by flushing to disk and cleaning up old parquet files beyond a buffer once they have been written to object storage. Data is written to the data of the UTC of the timestamp, which means there may be multiple active at once based on the firehose. Those that are > 2 days in the future or past are ignored.

The jetstream cursor is checkpointed to local sqlite and mirrored to object storage. Only one jetstream connection is active at a time, with failover across the configured endpoints. On resume, if the persisted cursor is too old for jetstream to replay, the process logs a warning and reconnects at "now," accepting the gap.

### at-snapshot snapshot
This command reads from object storage the past N days of data in duckdb to compute aggregates to materialize the duckdb schema required for current_all.duckdb and current_graph.duckdb. It is typically scheduled as a cron on the host machine. It's important to constrain the duckdb's working memory so as not to overwhelm the machine with a configurable limit. It should not rely on local files, instead only rely on reading from object storage.

Semantics:
- The social graph (Actors / Follows / Blocks in `current_graph.duckdb`) is *current state*: the bootstrap baseline plus all follow/block creates and deletes observed in `raw/` since bootstrap. It is not bounded by the lookback window.
- Post-related entities (Posts / Likes / Reposts / post_media / post_aggs) and per-actor engagement aggregates *are* bounded by the N-day window. Window-bounded fields are named with an `_in_window` suffix (e.g. `total_posts_in_window`, `likes_in_window`) so consumers don't confuse them with all-time totals.

### at-snapshot monitor
This is a lightweight http server that reads the logs from the jobs and outputs the current status and if the process is healhty for remote monitoring. It is pointed at the output directory where all the logs and files are and outputs useful views that someone would want to know to confirm that the following commands are working well:
  - boostrap
  - run
  - snapshot

### For all commands
- Periodic statistics about where it is in the job (# of records etc) are output at the INFO level such that it can be parsed by the monitor
- They can be started again and check for data in this order to resume:
  - locally
  - object storage
  - rebuild from scratch
- Ensure there are end to end tests with mocks for each workflow in addition to unit tests so that they are flexed all together. This means running a `bootstrap -> run -> snapshot + monitor` work with simulate data from sources to verify they all work together and write to object storage. The object sotrage and sources should be mocked but everything else in the program should be real. We should verify they can be interrupted and be picked up from where they left off too

### Configurations
- The object storage bucket, url, and access keys (env var) -- assume s3 compatible API with R2 as default and a local filesystem for testing
- The output data directory where everything writes to
- The snapshot lookback window (default 30 days)
- The filters for raw records (default lang: en and bsky moderation). Filters apply only to record types that carry the relevant metadata (posts). Engagement records (likes, follows, reposts, blocks) are kept regardless, since they don't carry language or moderation metadata themselves.

## Sources of data:

It is critical to use the right sources of data in order to work efficiently. The wrong endpoint can mean that things take 100x as long as they should.
- getting all dids: plc.directory/export -- prefer reimplementing the export pull in pure Go, or importing `goat` as a library if its packages are clean. Avoid shelling out to a binary.
- getting follow / block / profile records during bootstrap: https://constellation.microcosm.blue/ -- using listRecords for the relevant collection for a given DID or batch of DIDs
- getting live ATProto records: wss://jetstream{1,2}.{us-east,us-west}.bsky.network/subscribe
  - one active connection at a time with failover across endpoints
  - it should be configurable to filter to ONLY the record types we want and we can subset based on language (default: en) and labelers (default bsky moderation) on records that carry those fields

## Data model

The entities we are interested in are the following. 
Social Graph:
- Actor: app.bsky.actor.profile (we want a current snapshot of the actors, with their bios, create time)
  - actor_aggs: current followers / follows / blocks (all-time, derived from the current-state graph) plus posts / likes / quotes / replies *within the snapshot window*. Window-scoped fields use the `_in_window` suffix to keep the scope explicit.
- Follows: app.bsky.graph.follow
  - we care about src_id and dest_id of the interned DIDs and timestamp
- Blocks: app.bsky.graph.block
  - we care about src_id and dest_id of the interned DIDs and timestamp
Posts: 
- Posts: app.bsky.feed.post
  - the initial immutable post and accompanying information (we care about the content, timestsamp, actor, reply_parent, quote_parent)
  - post_media in a separate table (we dont care about alt text or the actual content, just the links, type, and timestamps)
  - post_aggs in a separate table (likes / retweets) -- updated over time to reflect the current aggregates from likes / reposts / posts for the current snapshot
- Likes: app.bsky.feed.like (we care about the actor, post, and timestamp)
- Reposts: app.bsky.feed.repost

Additionally to keep the tables memory efficient we will intern the following fields to reduce their footprint small:
- URI: post URIs are long and will have an id that can be referenced for likes, aggs, and media
- DIDs: Profiles will have their DIDs interned
- We will use bigints for all of these to ensure we can reach sufficient scale
- Interning is done by deterministic 64-bit hash (e.g. xxhash64) of the source string, so no central ID allocator or shared state is needed across `bootstrap` / `run` / `snapshot`. The original strings are preserved alongside the id in the `actors` and `posts` tables, so collisions can be detected and reported at snapshot time.

## Operational Considerations

- Idempotent: As a good data engineering practice all the operations should be idempotent so that running them multiple times after failure does not cause a different state 
- Checkpointed: Since this will be streaming live data and hang-ups may occur or machines may reboot It's important to checkpoint work both in the local file disk as well as remotely when it makes sense to. The job should resume from the most recent checkpoint if available 
- Simple: The implementation should be simple and use Go strengths with coroutines, channels, and buffers in order to parallelize work as much as possible while keeping the footprint small

## Additional Notes
- Update the README.md to be concise with the following
  - The goal this repo solves
  - A mental model overview at the beginning
  - the ERD in mermaid of the result 
  - The CLI commands 

## Reference implementations to look at:
- https://docs.bsky.app/blog/introducing-tap
- https://github.com/blacksky-algorithms/rsky/tree/main/rsky-wintermute
- https://tangled.org/microcosm.blue/microcosm-rs/blob/main/constellation/readme.md

## Implementation Guidance:
- Use Agents where there are clear well defined steps to parallelize work and preserve context windows
- Use Red/Green tests to build up to the end state. Wire and end to end test with mocks to start as well as a skeleton
- Incrementally commit logical isolated units to create a linear history that's understandable
- Validate the end result by running a subset of DIDs locally and validating everything generates. The test should be that you can run analytic queries like:
  - How many people followed or blocked someone else yesterday
  - What % of total follows were generated yesterday
  - How many people who posted got 1 like
  - How many posts got at least one like
- If the implementation is no good we will wipe eveyrthing and restart with updated guidelines. Try to be as self consistent, logical, DRY, and use Go best practices as much as possible.
