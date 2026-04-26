package run

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
)

// parquetExportSpec describes the SQL projection for one staging table →
// one daily parquet shard.
type parquetExportSpec struct {
	table   string // staging table name
	parquet string // parquet basename
	kind    string // "post" | "post_embed" | "like" | "repost" | "follow" | "block" | "profile" | "delete"
}

// dayManifest is the on-disk shape of `_manifest.json` per §6.
type dayManifest struct {
	Date                 string           `json:"date"`
	SchemaVersion        string           `json:"schema_version"`
	JetstreamCursorStart int64            `json:"jetstream_cursor_start"`
	JetstreamCursorEnd   int64            `json:"jetstream_cursor_end"`
	BuiltAt              time.Time        `json:"built_at"`
	RowCounts            map[string]int64 `json:"row_counts"`
	Bytes                map[string]int64 `json:"bytes"`
}

func writeManifest(path string, m dayManifest) error {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

// exportParquet runs a DuckDB COPY that reads from a SQLite-attached
// staging table and writes a parquet shard with zstd compression at
// level 6 (§4 step 4 of §8).
//
// Strategy: open an in-memory DuckDB; LOAD the bundled `sqlite_scanner`
// extension; ATTACH the staging.db read-only; COPY a per-collection
// SELECT to parquet. If sqlite_scanner is unavailable (e.g. the bundled
// extension can't be installed in offline environments) we fall back to
// a pure-Go path that streams rows from sqlite into a DuckDB temp table
// and then COPYs that.
func exportParquet(ctx context.Context, sqlitePath, day string, spec parquetExportSpec, outPath string) error {
	// Best-effort: ensure no stale parquet shadows a fresh write.
	_ = os.Remove(outPath)

	// Build the SELECT for this collection. The schemas come from §6.
	sel, fromTable, ok := selectForKind(spec.kind, day)
	if !ok {
		return fmt.Errorf("unknown export kind %q", spec.kind)
	}

	if err := exportViaSQLiteScanner(ctx, sqlitePath, sel, fromTable, spec.table, outPath); err == nil {
		return nil
	} else {
		// Fall through to fallback. Log via stderr through a sentinel error
		// so callers can choose to surface it.
		_ = err
	}

	return exportViaCopyFallback(ctx, sqlitePath, sel, spec.table, day, outPath)
}

// exportViaSQLiteScanner installs/loads sqlite_scanner, ATTACHes the
// staging db read-only, and COPYs out via DuckDB.
func exportViaSQLiteScanner(ctx context.Context, sqlitePath, sel, fromTable, stagingTable, outPath string) error {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return err
	}
	defer db.Close()

	for _, q := range []string{
		`INSTALL sqlite;`,
		`LOAD sqlite;`,
	} {
		if _, err := db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("duckdb sqlite ext: %w", err)
		}
	}
	attach := fmt.Sprintf(`ATTACH '%s' AS s (TYPE sqlite, READ_ONLY);`, escapeSingleQuotes(sqlitePath))
	if _, err := db.ExecContext(ctx, attach); err != nil {
		return fmt.Errorf("duckdb attach sqlite: %w", err)
	}

	// `fromTable` is "s.<table>" so the SELECT references the attached db.
	fullSelect := strings.Replace(sel, "{{FROM}}", fromTable, 1)
	copyStmt := fmt.Sprintf(
		`COPY (%s) TO '%s' (FORMAT PARQUET, COMPRESSION zstd, COMPRESSION_LEVEL 6);`,
		fullSelect,
		escapeSingleQuotes(outPath),
	)
	if _, err := db.ExecContext(ctx, copyStmt); err != nil {
		return fmt.Errorf("duckdb copy parquet: %w", err)
	}
	return nil
}

// exportViaCopyFallback streams the SQLite rows into a DuckDB temp table
// and then COPYs that to parquet. Slower but extension-free.
func exportViaCopyFallback(ctx context.Context, sqlitePath, sel, stagingTable, day, outPath string) error {
	// Open SQLite (modernc) read-only.
	sdsn := fmt.Sprintf("file:%s?mode=ro&_pragma=busy_timeout(5000)", sqlitePath)
	src, err := sql.Open("sqlite", sdsn)
	if err != nil {
		return err
	}
	defer src.Close()

	cols, _, err := stagingTableColumns(stagingTable)
	if err != nil {
		return err
	}

	rows, err := src.QueryContext(ctx,
		fmt.Sprintf(`SELECT %s FROM %s WHERE day = ?`, strings.Join(cols, ","), stagingTable),
		day,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	dst, err := sql.Open("duckdb", "")
	if err != nil {
		return err
	}
	defer dst.Close()

	// Create raw staging table in DuckDB.
	createStmt, err := duckdbCreateForStaging(stagingTable)
	if err != nil {
		return err
	}
	if _, err := dst.ExecContext(ctx, createStmt); err != nil {
		return err
	}

	// Stream rows.
	insertStmt, err := dst.PrepareContext(ctx, duckdbInsertForStaging(stagingTable))
	if err != nil {
		return err
	}
	defer insertStmt.Close()

	for rows.Next() {
		args, err := scanStagingRow(rows, stagingTable)
		if err != nil {
			return err
		}
		if _, err := insertStmt.ExecContext(ctx, args...); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	fullSelect := strings.Replace(sel, "{{FROM}}", "raw", 1)
	copyStmt := fmt.Sprintf(
		`COPY (%s) TO '%s' (FORMAT PARQUET, COMPRESSION zstd, COMPRESSION_LEVEL 6);`,
		fullSelect,
		escapeSingleQuotes(outPath),
	)
	if _, err := dst.ExecContext(ctx, copyStmt); err != nil {
		return fmt.Errorf("duckdb copy fallback: %w", err)
	}
	return nil
}

// stagingTableColumns returns the column list of a staging table, in the
// order needed for the fallback INSERT. `staging_events_delete` lacks
// `cid` and `record_json`.
func stagingTableColumns(table string) ([]string, bool, error) {
	if table == "staging_events_delete" {
		return []string{"did", "collection", "rkey", "operation", "time_us", "day"}, false, nil
	}
	if _, ok := tableInWhitelist(table); !ok {
		return nil, false, fmt.Errorf("unknown staging table %q", table)
	}
	return []string{"did", "collection", "rkey", "operation", "time_us", "cid", "day", "record_json"}, true, nil
}

func tableInWhitelist(table string) (string, bool) {
	whitelist := []string{
		"staging_events_post",
		"staging_events_like",
		"staging_events_repost",
		"staging_events_follow",
		"staging_events_block",
		"staging_events_profile",
		"staging_events_delete",
	}
	for _, t := range whitelist {
		if t == table {
			return t, true
		}
	}
	return "", false
}

func duckdbCreateForStaging(table string) (string, error) {
	if table == "staging_events_delete" {
		return `CREATE TEMP TABLE raw (
  did VARCHAR, collection VARCHAR, rkey VARCHAR, operation VARCHAR,
  time_us BIGINT, day VARCHAR
);`, nil
	}
	if _, ok := tableInWhitelist(table); !ok {
		return "", fmt.Errorf("unknown staging table %q", table)
	}
	return `CREATE TEMP TABLE raw (
  did VARCHAR, collection VARCHAR, rkey VARCHAR, operation VARCHAR,
  time_us BIGINT, cid VARCHAR, day VARCHAR, record_json VARCHAR
);`, nil
}

func duckdbInsertForStaging(table string) string {
	if table == "staging_events_delete" {
		return `INSERT INTO raw (did, collection, rkey, operation, time_us, day) VALUES (?, ?, ?, ?, ?, ?);`
	}
	return `INSERT INTO raw (did, collection, rkey, operation, time_us, cid, day, record_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?);`
}

func scanStagingRow(rows *sql.Rows, table string) ([]any, error) {
	if table == "staging_events_delete" {
		var did, collection, rkey, operation, day string
		var timeUS int64
		if err := rows.Scan(&did, &collection, &rkey, &operation, &timeUS, &day); err != nil {
			return nil, err
		}
		return []any{did, collection, rkey, operation, timeUS, day}, nil
	}
	var did, collection, rkey, operation, day string
	var cid, recJSON sql.NullString
	var timeUS int64
	if err := rows.Scan(&did, &collection, &rkey, &operation, &timeUS, &cid, &day, &recJSON); err != nil {
		return nil, err
	}
	return []any{did, collection, rkey, operation, timeUS,
		nullStringTo(cid), day, nullStringTo(recJSON)}, nil
}

func nullStringTo(s sql.NullString) any {
	if !s.Valid {
		return nil
	}
	return s.String
}

// selectForKind returns a SELECT projection that turns one staging-table
// row into the per-collection parquet schema documented in §6. The
// `{{FROM}}` placeholder is filled in by callers — `s.staging_events_post`
// when reading via sqlite_scanner, or `raw` for the fallback path.
func selectForKind(kind, day string) (string, string, bool) {
	dayLit := fmt.Sprintf("DATE '%s'", day)
	switch kind {
	case "post":
		return postSelect(dayLit), "s.staging_events_post", true
	case "post_embed":
		return postEmbedSelect(dayLit), "s.staging_events_post", true
	case "like":
		return likeSelect(dayLit), "s.staging_events_like", true
	case "repost":
		return repostSelect(dayLit), "s.staging_events_repost", true
	case "follow":
		return followSelect(dayLit), "s.staging_events_follow", true
	case "block":
		return blockSelect(dayLit), "s.staging_events_block", true
	case "profile":
		return profileSelect(dayLit), "s.staging_events_profile", true
	case "delete":
		return deleteSelect(dayLit), "s.staging_events_delete", true
	}
	return "", "", false
}

// Common helpers — JSON path extraction with NULL on missing keys, plus
// AT-URI composition.
//
// json_extract_string returns the unquoted string at the given path, NULL
// if not present. record_ts is parsed via try_strptime / try_cast; since
// AT-Proto timestamps vary, we accept the parse failure as NULL.

const recordTSExpr = `try_cast(json_extract_string(record_json, '$.createdAt') AS TIMESTAMP)`

func atURIExpr(coll string) string {
	return fmt.Sprintf(`'at://' || did || '/%s/' || rkey`, coll)
}

func postSelect(dayLit string) string {
	return fmt.Sprintf(`SELECT
    %s AS date,
    CASE operation WHEN 'create' THEN 'create' WHEN 'update' THEN 'update' ELSE operation END AS event_type,
    %s AS uri,
    did,
    rkey,
    cid,
    make_timestamp(time_us) AS event_ts,
    %s AS record_ts,
    json_extract_string(record_json, '$.text') AS text,
    json_extract_string(record_json, '$.langs[0]') AS lang,
    json_extract_string(record_json, '$.reply.root.uri') AS reply_root_uri,
    json_extract_string(record_json, '$.reply.parent.uri') AS reply_parent_uri,
    coalesce(
        json_extract_string(record_json, '$.embed.record.uri'),
        json_extract_string(record_json, '$.embed.record.record.uri')
    ) AS quote_uri,
    CASE
        WHEN json_extract_string(record_json, '$.embed.$type') = 'app.bsky.embed.images' THEN 'images'
        WHEN json_extract_string(record_json, '$.embed.$type') = 'app.bsky.embed.video' THEN 'video'
        WHEN json_extract_string(record_json, '$.embed.$type') = 'app.bsky.embed.external' THEN 'external'
        WHEN json_extract_string(record_json, '$.embed.$type') = 'app.bsky.embed.record' THEN 'record'
        WHEN json_extract_string(record_json, '$.embed.$type') = 'app.bsky.embed.recordWithMedia' THEN 'recordWithMedia'
        ELSE NULL
    END AS embed_type
  FROM {{FROM}}
  WHERE day = '%s' AND operation IN ('create','update')
  ORDER BY did, rkey`,
		dayLit, atURIExpr("app.bsky.feed.post"), recordTSExpr, dayValueFromLit(dayLit))
}

// postEmbedSelect projects the per-post embed detail into the
// post_embeds.parquet schema (002 §1.2). Reads from staging_events_post —
// no separate staging table — and filters to rows that actually carry
// an `embed`. `kind` collapses recordWithMedia + inner-media into a
// single string so analysts get the full shape in one column.
//
// `image_with_alt` is approximated by counting matches of `"alt":"<non-empty>"`
// inside the images array. Alt-text content itself is never read or stored.
func postEmbedSelect(dayLit string) string {
	const embedTypeExpr = `json_extract_string(record_json, '$.embed.$type')`
	const innerTypeExpr = `json_extract_string(record_json, '$.embed.media.$type')`
	// Coalesced JSON paths so the same expression covers plain embeds and
	// the inner media of recordWithMedia.
	const imagesArrExpr = `coalesce(
        json_extract(record_json, '$.embed.images')::VARCHAR,
        json_extract(record_json, '$.embed.media.images')::VARCHAR
    )`
	const externalURIExpr = `coalesce(
        json_extract_string(record_json, '$.embed.external.uri'),
        json_extract_string(record_json, '$.embed.media.external.uri')
    )`
	const externalTitleExpr = `coalesce(
        json_extract_string(record_json, '$.embed.external.title'),
        json_extract_string(record_json, '$.embed.media.external.title')
    )`
	const videoAltExpr = `coalesce(
        json_extract_string(record_json, '$.embed.alt'),
        json_extract_string(record_json, '$.embed.media.alt')
    )`
	return fmt.Sprintf(`SELECT
    %s AS date,
    %s AS uri,
    did,
    rkey,
    make_timestamp(time_us) AS event_ts,
    CASE
        WHEN %s = 'app.bsky.embed.images'           THEN 'images'
        WHEN %s = 'app.bsky.embed.video'            THEN 'video'
        WHEN %s = 'app.bsky.embed.external'         THEN 'external'
        WHEN %s = 'app.bsky.embed.record'           THEN 'record'
        WHEN %s = 'app.bsky.embed.recordWithMedia' THEN
            'recordWithMedia:' || coalesce(
                CASE %s
                    WHEN 'app.bsky.embed.images'   THEN 'images'
                    WHEN 'app.bsky.embed.video'    THEN 'video'
                    WHEN 'app.bsky.embed.external' THEN 'external'
                END, 'unknown')
        ELSE NULL
    END AS kind,
    %s AS external_uri,
    regexp_extract(%s, '^https?://([^/]+)', 1) AS external_domain,
    %s AS external_title,
    CASE
        WHEN %s IN ('app.bsky.embed.images', 'app.bsky.embed.recordWithMedia')
        THEN coalesce(json_array_length(%s), 0)::SMALLINT
        ELSE NULL
    END AS image_count,
    CASE
        WHEN %s IN ('app.bsky.embed.images', 'app.bsky.embed.recordWithMedia')
        THEN coalesce(len(regexp_extract_all(%s, '"alt":"[^"]+"')), 0)::SMALLINT
        ELSE NULL
    END AS image_with_alt,
    CASE
        WHEN %s = 'app.bsky.embed.video'
            OR (%s = 'app.bsky.embed.recordWithMedia' AND %s = 'app.bsky.embed.video')
        THEN length(coalesce(trim(%s), '')) > 0
        ELSE NULL
    END AS video_has_alt
  FROM {{FROM}}
  WHERE day = '%s' AND operation IN ('create','update')
    AND json_extract(record_json, '$.embed') IS NOT NULL
  ORDER BY did, rkey`,
		dayLit, atURIExpr("app.bsky.feed.post"),
		embedTypeExpr, embedTypeExpr, embedTypeExpr, embedTypeExpr, embedTypeExpr,
		innerTypeExpr,
		externalURIExpr, externalURIExpr, externalTitleExpr,
		embedTypeExpr, imagesArrExpr,
		embedTypeExpr, imagesArrExpr,
		embedTypeExpr, embedTypeExpr, innerTypeExpr, videoAltExpr,
		dayValueFromLit(dayLit))
}

// Likes/reposts/follows/blocks parquets carry an `event_type` column
// (matches archive/edges.go schema and is required by build's incremental
// replay, which filters `WHERE event_type IN ('create','update')`).
// `operation` here is always 'create' or 'update' due to the WHERE below.

func likeSelect(dayLit string) string {
	return fmt.Sprintf(`SELECT
    %s AS date,
    %s AS uri,
    did AS liker_did,
    json_extract_string(record_json, '$.subject.uri') AS subject_uri,
    json_extract_string(record_json, '$.subject.cid') AS subject_cid,
    make_timestamp(time_us) AS event_ts,
    %s AS record_ts,
    operation AS event_type
  FROM {{FROM}}
  WHERE day = '%s' AND operation IN ('create','update')
  ORDER BY did, rkey`,
		dayLit, atURIExpr("app.bsky.feed.like"), recordTSExpr, dayValueFromLit(dayLit))
}

func repostSelect(dayLit string) string {
	return fmt.Sprintf(`SELECT
    %s AS date,
    %s AS uri,
    did AS reposter_did,
    json_extract_string(record_json, '$.subject.uri') AS subject_uri,
    json_extract_string(record_json, '$.subject.cid') AS subject_cid,
    make_timestamp(time_us) AS event_ts,
    %s AS record_ts,
    operation AS event_type
  FROM {{FROM}}
  WHERE day = '%s' AND operation IN ('create','update')
  ORDER BY did, rkey`,
		dayLit, atURIExpr("app.bsky.feed.repost"), recordTSExpr, dayValueFromLit(dayLit))
}

func followSelect(dayLit string) string {
	return fmt.Sprintf(`SELECT
    %s AS date,
    %s AS uri,
    did AS src_did,
    json_extract_string(record_json, '$.subject') AS dst_did,
    make_timestamp(time_us) AS event_ts,
    %s AS record_ts,
    operation AS event_type
  FROM {{FROM}}
  WHERE day = '%s' AND operation IN ('create','update')
  ORDER BY did, rkey`,
		dayLit, atURIExpr("app.bsky.graph.follow"), recordTSExpr, dayValueFromLit(dayLit))
}

func blockSelect(dayLit string) string {
	return fmt.Sprintf(`SELECT
    %s AS date,
    %s AS uri,
    did AS src_did,
    json_extract_string(record_json, '$.subject') AS dst_did,
    make_timestamp(time_us) AS event_ts,
    %s AS record_ts,
    operation AS event_type
  FROM {{FROM}}
  WHERE day = '%s' AND operation IN ('create','update')
  ORDER BY did, rkey`,
		dayLit, atURIExpr("app.bsky.graph.block"), recordTSExpr, dayValueFromLit(dayLit))
}

func profileSelect(dayLit string) string {
	return fmt.Sprintf(`SELECT
    %s AS date,
    did,
    NULL AS handle,
    json_extract_string(record_json, '$.displayName') AS display_name,
    json_extract_string(record_json, '$.description') AS description,
    json_extract_string(record_json, '$.avatar.ref.$link') AS avatar_cid,
    make_timestamp(time_us) AS event_ts
  FROM {{FROM}}
  WHERE day = '%s' AND operation IN ('create','update')
  ORDER BY did, rkey`,
		dayLit, dayValueFromLit(dayLit))
}

func deleteSelect(dayLit string) string {
	return fmt.Sprintf(`SELECT
    %s AS date,
    collection,
    'at://' || did || '/' || collection || '/' || rkey AS uri,
    did,
    make_timestamp(time_us) AS event_ts
  FROM {{FROM}}
  WHERE day = '%s' AND operation = 'delete'
  ORDER BY did, rkey`,
		dayLit, dayValueFromLit(dayLit))
}

// dayValueFromLit pulls the bare YYYY-MM-DD out of "DATE 'YYYY-MM-DD'".
func dayValueFromLit(dayLit string) string {
	// dayLit is "DATE '2026-04-24'" — extract the inner literal.
	start := strings.Index(dayLit, "'")
	end := strings.LastIndex(dayLit, "'")
	if start < 0 || end <= start {
		return ""
	}
	return dayLit[start+1 : end]
}

func escapeSingleQuotes(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
