package snapshot

import (
	"fmt"
	"time"
)

// graphSQL returns the ordered CREATE TABLE AS SELECT statements that
// materialize the current-state social graph in the working database.
//
// The current-state semantics are:
//   - actors: bootstrap baseline UNION any new profiles seen in raw, with
//     latest-indexed-at winning per DID.
//   - follows: bootstrap follows UNION raw follows with op=create, MINUS
//     rows whose (src_did_id, rkey) appears with op=delete in raw at a later
//     indexed_at than its latest create. Bootstrap-baseline follows count as
//     creates with their bootstrap indexed_at.
//   - blocks: same shape as follows.
//   - actor_aggs: derived from the now-current state, all-time scope.
//
// We render an explicit empty SELECT when a collection has no parquet shards
// in raw/ — read_parquet on an empty file list errors out, and the rest of
// the pipeline references the temp tables unconditionally.
func graphSQL(files rawFiles) []string {
	rawProfiles := rawProfilesSelect(files.profiles)
	rawFollows := rawFollowsSelect(files.follows)
	rawBlocks := rawBlocksSelect(files.blocks)

	return []string{
		// Stage raw profile deltas (only creates carry meaningful payload;
		// profile deletes for atproto are rare and we do not propagate them
		// as actor removals — actors keep representing observed identities).
		fmt.Sprintf(`CREATE TEMP TABLE _raw_profiles AS %s`, rawProfiles),

		// Pick the most recent profile observation per DID.
		`
            CREATE TEMP TABLE _latest_profile AS
            SELECT * EXCLUDE (rn) FROM (
                SELECT *, row_number() OVER (PARTITION BY did_id ORDER BY indexed_at DESC) AS rn
                FROM _raw_profiles
            )
            WHERE rn = 1
        `,

		// actors: bootstrap baseline overridden by newer profile observations.
		// We left-join the latest delta onto the baseline and coalesce field
		// by field so a partial profile in raw doesn't blank out a populated
		// bootstrap row.
		`
            CREATE TABLE actors AS
            WITH base AS (
                SELECT did_id, did, handle, display_name, description,
                       avatar_cid, banner_cid, created_at, indexed_at, source
                FROM bootstrap.actors
            ),
            merged AS (
                SELECT
                    coalesce(b.did_id, p.did_id) AS did_id,
                    coalesce(b.did, p.did) AS did,
                    coalesce(p.handle, b.handle) AS handle,
                    coalesce(p.display_name, b.display_name) AS display_name,
                    coalesce(p.description, b.description) AS description,
                    coalesce(p.avatar_cid, b.avatar_cid) AS avatar_cid,
                    coalesce(p.banner_cid, b.banner_cid) AS banner_cid,
                    coalesce(p.created_at, b.created_at) AS created_at,
                    -- indexed_at: newest of the two so consumers can ORDER BY
                    -- indexed_at DESC and see the freshest observation first.
                    CASE
                        WHEN p.indexed_at IS NOT NULL AND (b.indexed_at IS NULL OR p.indexed_at > b.indexed_at)
                        THEN p.indexed_at ELSE b.indexed_at
                    END AS indexed_at,
                    CASE WHEN p.did_id IS NULL THEN b.source ELSE p.source END AS source
                FROM base b
                FULL OUTER JOIN _latest_profile p USING (did_id)
            )
            SELECT * FROM merged
        `,

		// Stage follow deltas with a row number per natural key so we can
		// find the latest event for each (src_did_id, rkey).
		fmt.Sprintf(`CREATE TEMP TABLE _raw_follows AS %s`, rawFollows),

		`
            CREATE TEMP TABLE _follow_state AS
            SELECT * EXCLUDE (rn) FROM (
                SELECT *, row_number() OVER (PARTITION BY src_did_id, rkey ORDER BY indexed_at DESC) AS rn
                FROM _raw_follows
            )
            WHERE rn = 1
        `,

		// follows: bootstrap baseline minus any (src,rkey) whose latest delta
		// is a delete, plus any (src,rkey) whose latest delta is a create.
		// New creates win when both sides hold the same key (raw replays).
		`
            CREATE TABLE follows AS
            WITH baseline AS (
                SELECT src_did_id, rkey, dst_did_id, src_did, dst_did,
                       created_at, indexed_at, source
                FROM bootstrap.follows
            ),
            tombstones AS (
                SELECT src_did_id, rkey FROM _follow_state WHERE op = 'delete'
            ),
            kept_baseline AS (
                SELECT b.* FROM baseline b
                LEFT JOIN tombstones t USING (src_did_id, rkey)
                WHERE t.src_did_id IS NULL
            ),
            new_creates AS (
                SELECT src_did_id, rkey, dst_did_id, src_did, dst_did,
                       created_at, indexed_at, source
                FROM _follow_state
                WHERE op = 'create'
            )
            SELECT src_did_id, rkey, dst_did_id, src_did, dst_did,
                   created_at, indexed_at, source
            FROM (
                SELECT *, row_number() OVER (PARTITION BY src_did_id, rkey ORDER BY indexed_at DESC) AS rn
                FROM (
                    SELECT * FROM kept_baseline
                    UNION ALL
                    SELECT * FROM new_creates
                )
            )
            WHERE rn = 1
        `,

		// blocks mirror follows.
		fmt.Sprintf(`CREATE TEMP TABLE _raw_blocks AS %s`, rawBlocks),

		`
            CREATE TEMP TABLE _block_state AS
            SELECT * EXCLUDE (rn) FROM (
                SELECT *, row_number() OVER (PARTITION BY src_did_id, rkey ORDER BY indexed_at DESC) AS rn
                FROM _raw_blocks
            )
            WHERE rn = 1
        `,

		`
            CREATE TABLE blocks AS
            WITH baseline AS (
                SELECT src_did_id, rkey, dst_did_id, src_did, dst_did,
                       created_at, indexed_at, source
                FROM bootstrap.blocks
            ),
            tombstones AS (
                SELECT src_did_id, rkey FROM _block_state WHERE op = 'delete'
            ),
            kept_baseline AS (
                SELECT b.* FROM baseline b
                LEFT JOIN tombstones t USING (src_did_id, rkey)
                WHERE t.src_did_id IS NULL
            ),
            new_creates AS (
                SELECT src_did_id, rkey, dst_did_id, src_did, dst_did,
                       created_at, indexed_at, source
                FROM _block_state
                WHERE op = 'create'
            )
            SELECT src_did_id, rkey, dst_did_id, src_did, dst_did,
                   created_at, indexed_at, source
            FROM (
                SELECT *, row_number() OVER (PARTITION BY src_did_id, rkey ORDER BY indexed_at DESC) AS rn
                FROM (
                    SELECT * FROM kept_baseline
                    UNION ALL
                    SELECT * FROM new_creates
                )
            )
            WHERE rn = 1
        `,

		// actor_aggs: per-actor counts derived from the now-current graph.
		`
            CREATE TABLE actor_aggs AS
            WITH followers AS (
                SELECT dst_did_id AS did_id, count(*) AS followers
                FROM follows GROUP BY 1
            ),
            following AS (
                SELECT src_did_id AS did_id, count(*) AS following
                FROM follows GROUP BY 1
            ),
            blocks_in AS (
                SELECT dst_did_id AS did_id, count(*) AS blocks_in
                FROM blocks GROUP BY 1
            ),
            blocks_out AS (
                SELECT src_did_id AS did_id, count(*) AS blocks_out
                FROM blocks GROUP BY 1
            )
            SELECT
                a.did_id,
                a.did,
                coalesce(f.followers, 0) AS followers,
                coalesce(g.following, 0) AS following,
                coalesce(bi.blocks_in, 0) AS blocks_in,
                coalesce(bo.blocks_out, 0) AS blocks_out
            FROM actors a
            LEFT JOIN followers f USING (did_id)
            LEFT JOIN following g USING (did_id)
            LEFT JOIN blocks_in bi USING (did_id)
            LEFT JOIN blocks_out bo USING (did_id)
        `,
	}
}

// windowSQL returns the ordered CTAS statements that produce the post-related
// tables and the in-window aggregates inside current_all.duckdb.
//
// The window is anchored on indexed_at: that's the snapshotter's observation
// time, which is monotonic and tamper-evident. created_at on a record can be
// set to anything by the publishing client.
func windowSQL(files rawFiles, windowStart, windowEnd time.Time) []string {
	startLit := timestampLiteral(windowStart)
	endLit := timestampLiteral(windowEnd)

	postsSelect := windowedPostsSelect(files.posts, startLit, endLit)
	mediaSelect := windowedMediaSelect(files.media, startLit, endLit)
	likesSelect := windowedLikesSelect(files.likes, startLit, endLit)
	repostsSelect := windowedRepostsSelect(files.reposts, startLit, endLit)

	return []string{
		fmt.Sprintf(`CREATE TABLE posts AS %s`, postsSelect),
		fmt.Sprintf(`CREATE TABLE post_media AS %s`, mediaSelect),
		fmt.Sprintf(`CREATE TABLE likes AS %s`, likesSelect),
		fmt.Sprintf(`CREATE TABLE reposts AS %s`, repostsSelect),

		// post_aggs: rebuild engagement counts strictly from in-window rows,
		// keyed by the posts table so we don't surface aggregates for posts
		// that aren't themselves in the window. A like on an old post still
		// counts as a like emitted in-window for actor_aggs purposes, but it
		// doesn't materialize a phantom row in post_aggs.
		`
            CREATE TABLE post_aggs AS
            WITH likes_c AS (
                SELECT subject_uri_id AS uri_id, count(*) AS likes_in_window
                FROM likes GROUP BY 1
            ),
            reposts_c AS (
                SELECT subject_uri_id AS uri_id, count(*) AS reposts_in_window
                FROM reposts GROUP BY 1
            ),
            quotes_c AS (
                SELECT quote_parent_uri_id AS uri_id, count(*) AS quotes_in_window
                FROM posts WHERE quote_parent_uri_id IS NOT NULL AND quote_parent_uri_id != 0
                GROUP BY 1
            ),
            replies_c AS (
                SELECT reply_parent_uri_id AS uri_id, count(*) AS replies_in_window
                FROM posts WHERE reply_parent_uri_id IS NOT NULL AND reply_parent_uri_id != 0
                GROUP BY 1
            )
            SELECT
                p.uri_id,
                coalesce(l.likes_in_window, 0)   AS likes_in_window,
                coalesce(r.reposts_in_window, 0) AS reposts_in_window,
                coalesce(q.quotes_in_window, 0)  AS quotes_in_window,
                coalesce(rp.replies_in_window, 0) AS replies_in_window
            FROM posts p
            LEFT JOIN likes_c    l  USING (uri_id)
            LEFT JOIN reposts_c  r  USING (uri_id)
            LEFT JOIN quotes_c   q  USING (uri_id)
            LEFT JOIN replies_c  rp USING (uri_id)
        `,

		// Extend actor_aggs with `_in_window` columns. We rebuild rather than
		// ALTER + UPDATE so the operation is atomic from a reader's POV and
		// the resulting file has a stable column order regardless of run.
		`
            CREATE TABLE _actor_aggs_window AS
            WITH posts_by_actor AS (
                SELECT did_id, count(*) AS total_posts_in_window
                FROM posts GROUP BY 1
            ),
            likes_received AS (
                SELECT p.did_id, count(*) AS total_likes_received_in_window
                FROM likes l JOIN posts p ON p.uri_id = l.subject_uri_id
                GROUP BY 1
            ),
            likes_given AS (
                SELECT actor_did_id AS did_id, count(*) AS total_likes_given_in_window
                FROM likes GROUP BY 1
            ),
            reposts_received AS (
                SELECT p.did_id, count(*) AS total_reposts_received_in_window
                FROM reposts r JOIN posts p ON p.uri_id = r.subject_uri_id
                GROUP BY 1
            ),
            quotes_received AS (
                SELECT pp.did_id, count(*) AS total_quotes_received_in_window
                FROM posts q JOIN posts pp ON pp.uri_id = q.quote_parent_uri_id
                WHERE q.quote_parent_uri_id IS NOT NULL AND q.quote_parent_uri_id != 0
                GROUP BY 1
            ),
            replies_received AS (
                SELECT pp.did_id, count(*) AS total_replies_received_in_window
                FROM posts r JOIN posts pp ON pp.uri_id = r.reply_parent_uri_id
                WHERE r.reply_parent_uri_id IS NOT NULL AND r.reply_parent_uri_id != 0
                GROUP BY 1
            )
            SELECT
                a.did_id,
                a.did,
                a.followers,
                a.following,
                a.blocks_in,
                a.blocks_out,
                coalesce(pa.total_posts_in_window, 0)            AS total_posts_in_window,
                coalesce(lr.total_likes_received_in_window, 0)   AS total_likes_received_in_window,
                coalesce(lg.total_likes_given_in_window, 0)      AS total_likes_given_in_window,
                coalesce(rr.total_reposts_received_in_window, 0) AS total_reposts_received_in_window,
                coalesce(qr.total_quotes_received_in_window, 0)  AS total_quotes_received_in_window,
                coalesce(rep.total_replies_received_in_window, 0) AS total_replies_received_in_window
            FROM actor_aggs a
            LEFT JOIN posts_by_actor   pa  USING (did_id)
            LEFT JOIN likes_received   lr  USING (did_id)
            LEFT JOIN likes_given      lg  USING (did_id)
            LEFT JOIN reposts_received rr  USING (did_id)
            LEFT JOIN quotes_received  qr  USING (did_id)
            LEFT JOIN replies_received rep USING (did_id)
        `,

		`DROP TABLE actor_aggs`,
		`ALTER TABLE _actor_aggs_window RENAME TO actor_aggs`,
	}
}

// timestampLiteral returns a DuckDB literal for a UTC time value.
func timestampLiteral(t time.Time) string {
	return fmt.Sprintf("TIMESTAMP '%s'", t.UTC().Format("2006-01-02 15:04:05.000000"))
}

// rawProfilesSelect returns a SELECT yielding (did_id, did, handle,
// display_name, description, avatar_cid, banner_cid, created_at, indexed_at,
// op, source) — either from the actual parquet shards or as an empty stub
// with the expected column types when no shards exist.
func rawProfilesSelect(files []string) string {
	if expr := readParquetExpr(files); expr != "" {
		return fmt.Sprintf(`SELECT * FROM %s WHERE op = 'create'`, expr)
	}
	return `
        SELECT
            CAST(NULL AS BIGINT)    AS did_id,
            CAST(NULL AS VARCHAR)   AS did,
            CAST(NULL AS VARCHAR)   AS handle,
            CAST(NULL AS VARCHAR)   AS display_name,
            CAST(NULL AS VARCHAR)   AS description,
            CAST(NULL AS VARCHAR)   AS avatar_cid,
            CAST(NULL AS VARCHAR)   AS banner_cid,
            CAST(NULL AS TIMESTAMP) AS created_at,
            CAST(NULL AS TIMESTAMP) AS indexed_at,
            CAST(NULL AS VARCHAR)   AS op,
            CAST(NULL AS VARCHAR)   AS source
        WHERE 1 = 0
    `
}

// rawFollowsSelect mirrors rawProfilesSelect for follow records. Stub schema
// matches model.Follow's parquet columns.
func rawFollowsSelect(files []string) string {
	if expr := readParquetExpr(files); expr != "" {
		return fmt.Sprintf(`SELECT * FROM %s`, expr)
	}
	return graphRecordEmptyStub()
}

// rawBlocksSelect mirrors rawFollowsSelect — block records share the same shape.
func rawBlocksSelect(files []string) string {
	if expr := readParquetExpr(files); expr != "" {
		return fmt.Sprintf(`SELECT * FROM %s`, expr)
	}
	return graphRecordEmptyStub()
}

// graphRecordEmptyStub is the shared empty schema for follows / blocks.
func graphRecordEmptyStub() string {
	return `
        SELECT
            CAST(NULL AS VARCHAR)   AS src_did,
            CAST(NULL AS BIGINT)    AS src_did_id,
            CAST(NULL AS VARCHAR)   AS dst_did,
            CAST(NULL AS BIGINT)    AS dst_did_id,
            CAST(NULL AS VARCHAR)   AS rkey,
            CAST(NULL AS TIMESTAMP) AS created_at,
            CAST(NULL AS TIMESTAMP) AS indexed_at,
            CAST(NULL AS VARCHAR)   AS op,
            CAST(NULL AS VARCHAR)   AS source
        WHERE 1 = 0
    `
}

// windowedPostsSelect renders the in-window post pipeline: filter by op +
// indexed_at, then keep the latest observation per uri_id.
func windowedPostsSelect(files []string, startLit, endLit string) string {
	if expr := readParquetExpr(files); expr != "" {
		return fmt.Sprintf(`
            WITH raw AS (
                SELECT * FROM %s
                WHERE op = 'create'
                  AND indexed_at > %s AND indexed_at <= %s
            )
            SELECT * EXCLUDE (rn) FROM (
                SELECT *, row_number() OVER (PARTITION BY uri_id ORDER BY indexed_at DESC) AS rn
                FROM raw
            )
            WHERE rn = 1
        `, expr, startLit, endLit)
	}
	return postsEmptyStub()
}

func windowedMediaSelect(files []string, startLit, endLit string) string {
	if expr := readParquetExpr(files); expr != "" {
		return fmt.Sprintf(`
            SELECT * FROM %s
            WHERE indexed_at > %s AND indexed_at <= %s
        `, expr, startLit, endLit)
	}
	return mediaEmptyStub()
}

func windowedLikesSelect(files []string, startLit, endLit string) string {
	if expr := readParquetExpr(files); expr != "" {
		return fmt.Sprintf(`
            WITH raw AS (
                SELECT * FROM %s
                WHERE op = 'create'
                  AND indexed_at > %s AND indexed_at <= %s
            )
            SELECT * EXCLUDE (rn) FROM (
                SELECT *, row_number() OVER (PARTITION BY actor_did_id, rkey ORDER BY indexed_at DESC) AS rn
                FROM raw
            )
            WHERE rn = 1
        `, expr, startLit, endLit)
	}
	return engagementEmptyStub()
}

func windowedRepostsSelect(files []string, startLit, endLit string) string {
	// Reposts share the same shape as likes.
	return windowedLikesSelect(files, startLit, endLit)
}

// postsEmptyStub mirrors model.Post's parquet columns.
func postsEmptyStub() string {
	return `
        SELECT
            CAST(NULL AS VARCHAR)   AS uri,
            CAST(NULL AS BIGINT)    AS uri_id,
            CAST(NULL AS VARCHAR)   AS did,
            CAST(NULL AS BIGINT)    AS did_id,
            CAST(NULL AS VARCHAR)   AS rkey,
            CAST(NULL AS VARCHAR)   AS cid,
            CAST(NULL AS VARCHAR)   AS text,
            CAST(NULL AS VARCHAR)   AS langs,
            CAST(NULL AS VARCHAR)   AS labels,
            CAST(NULL AS VARCHAR)   AS reply_parent_uri,
            CAST(NULL AS BIGINT)    AS reply_parent_uri_id,
            CAST(NULL AS VARCHAR)   AS reply_root_uri,
            CAST(NULL AS BIGINT)    AS reply_root_uri_id,
            CAST(NULL AS VARCHAR)   AS quote_parent_uri,
            CAST(NULL AS BIGINT)    AS quote_parent_uri_id,
            CAST(NULL AS BOOLEAN)   AS has_media,
            CAST(NULL AS TIMESTAMP) AS created_at,
            CAST(NULL AS TIMESTAMP) AS indexed_at,
            CAST(NULL AS VARCHAR)   AS op,
            CAST(NULL AS VARCHAR)   AS source
        WHERE 1 = 0
    `
}

// mediaEmptyStub mirrors model.PostMedia's parquet columns.
func mediaEmptyStub() string {
	return `
        SELECT
            CAST(NULL AS VARCHAR)   AS post_uri,
            CAST(NULL AS BIGINT)    AS post_uri_id,
            CAST(NULL AS VARCHAR)   AS did,
            CAST(NULL AS BIGINT)    AS did_id,
            CAST(NULL AS INTEGER)   AS idx,
            CAST(NULL AS VARCHAR)   AS media_type,
            CAST(NULL AS VARCHAR)   AS url,
            CAST(NULL AS VARCHAR)   AS blob_cid,
            CAST(NULL AS TIMESTAMP) AS created_at,
            CAST(NULL AS TIMESTAMP) AS indexed_at
        WHERE 1 = 0
    `
}

// engagementEmptyStub mirrors model.Like / model.Repost (identical shape).
func engagementEmptyStub() string {
	return `
        SELECT
            CAST(NULL AS VARCHAR)   AS actor_did,
            CAST(NULL AS BIGINT)    AS actor_did_id,
            CAST(NULL AS VARCHAR)   AS subject_uri,
            CAST(NULL AS BIGINT)    AS subject_uri_id,
            CAST(NULL AS VARCHAR)   AS rkey,
            CAST(NULL AS TIMESTAMP) AS created_at,
            CAST(NULL AS TIMESTAMP) AS indexed_at,
            CAST(NULL AS VARCHAR)   AS op,
            CAST(NULL AS VARCHAR)   AS source
        WHERE 1 = 0
    `
}
