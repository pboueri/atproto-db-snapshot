// Package bootstrap emits a graph bootstrap archive per spec §7.
//
// The bootstrap is a "state snapshot" of profiles + follows + blocks
// extracted from current_graph.duckdb (which is materialized graph state)
// into raw, self-describing parquet files. Raw DIDs are used (not interned
// actor_ids) so the archive is decodable without the registry — same
// philosophy as the daily parquet archive.
//
// Layout written to dayDir:
//
//	dayDir/profiles.parquet
//	dayDir/follows.parquet
//	dayDir/blocks.parquet
//	dayDir/_manifest.json
//
// The caller (build.publishContext) is responsible for uploading the
// directory under bootstrap/<date>/ in the object store — bootstrap.Emit
// itself only writes locally.
package bootstrap

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
)

// Manifest is the on-disk shape of _manifest.json per §7.
type Manifest struct {
	Date            string           `json:"date"`
	SchemaVersion   string           `json:"schema_version"`
	SnapshotTS      time.Time        `json:"snapshot_ts"`
	JetstreamCursor int64            `json:"jetstream_cursor"`
	RowCounts       map[string]int64 `json:"row_counts"`
	PriorBootstrap  string           `json:"prior_bootstrap,omitempty"`
}

// Emit reads current_graph.duckdb at graphDBPath and writes the three
// parquet files plus _manifest.json into dayDir. cursor is the
// jetstream microsecond cursor at snapshot time and is stored verbatim
// in the manifest.
func Emit(ctx context.Context, graphDBPath, dayDir string, cursor int64) (Manifest, error) {
	if err := os.MkdirAll(dayDir, 0o755); err != nil {
		return Manifest{}, err
	}
	date := filepath.Base(dayDir)
	now := time.Now().UTC()

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return Manifest{}, err
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		`ATTACH '%s' AS g (READ_ONLY)`, escapeSingle(graphDBPath))); err != nil {
		return Manifest{}, fmt.Errorf("attach graph: %w", err)
	}

	tsLit := fmt.Sprintf("TIMESTAMP '%s'", now.Format("2006-01-02 15:04:05"))

	rowCounts := map[string]int64{}

	// profiles.parquet — actors with handle + display fields. Inline raw DID.
	profilesPath := filepath.Join(dayDir, "profiles.parquet")
	_ = os.Remove(profilesPath)
	profilesQuery := fmt.Sprintf(`COPY (
	  SELECT
	    a.did,
	    a.handle,
	    a.display_name,
	    a.description,
	    a.avatar_cid,
	    a.created_at,
	    a.indexed_at,
	    %s AS snapshot_ts
	  FROM g.actors a
	  ORDER BY a.did
	) TO '%s' (FORMAT PARQUET, COMPRESSION zstd, COMPRESSION_LEVEL 6)`,
		tsLit, escapeSingle(profilesPath))
	if _, err := db.ExecContext(ctx, profilesQuery); err != nil {
		return Manifest{}, fmt.Errorf("copy profiles: %w", err)
	}
	if n, err := scalarInt(ctx, db, `SELECT count(*) FROM g.actors`); err == nil {
		rowCounts["profiles"] = n
	}

	// follows.parquet — compose at:// URI from src_did + rkey.
	followsPath := filepath.Join(dayDir, "follows.parquet")
	_ = os.Remove(followsPath)
	followsQuery := fmt.Sprintf(`COPY (
	  SELECT
	    'at://' || s.did || '/app.bsky.graph.follow/' || f.rkey AS uri,
	    s.did AS src_did,
	    d.did AS dst_did,
	    f.created_at,
	    %s AS snapshot_ts
	  FROM g.follows_current f
	  JOIN g.actors s ON s.actor_id = f.src_id
	  JOIN g.actors d ON d.actor_id = f.dst_id
	  ORDER BY s.did, f.rkey
	) TO '%s' (FORMAT PARQUET, COMPRESSION zstd, COMPRESSION_LEVEL 6)`,
		tsLit, escapeSingle(followsPath))
	if _, err := db.ExecContext(ctx, followsQuery); err != nil {
		return Manifest{}, fmt.Errorf("copy follows: %w", err)
	}
	if n, err := scalarInt(ctx, db, `SELECT count(*) FROM g.follows_current`); err == nil {
		rowCounts["follows"] = n
	}

	// blocks.parquet — same shape as follows.
	blocksPath := filepath.Join(dayDir, "blocks.parquet")
	_ = os.Remove(blocksPath)
	blocksQuery := fmt.Sprintf(`COPY (
	  SELECT
	    'at://' || s.did || '/app.bsky.graph.block/' || b.rkey AS uri,
	    s.did AS src_did,
	    d.did AS dst_did,
	    b.created_at,
	    %s AS snapshot_ts
	  FROM g.blocks_current b
	  JOIN g.actors s ON s.actor_id = b.src_id
	  JOIN g.actors d ON d.actor_id = b.dst_id
	  ORDER BY s.did, b.rkey
	) TO '%s' (FORMAT PARQUET, COMPRESSION zstd, COMPRESSION_LEVEL 6)`,
		tsLit, escapeSingle(blocksPath))
	if _, err := db.ExecContext(ctx, blocksQuery); err != nil {
		return Manifest{}, fmt.Errorf("copy blocks: %w", err)
	}
	if n, err := scalarInt(ctx, db, `SELECT count(*) FROM g.blocks_current`); err == nil {
		rowCounts["blocks"] = n
	}

	mf := Manifest{
		Date:            date,
		SchemaVersion:   "v1",
		SnapshotTS:      now,
		JetstreamCursor: cursor,
		RowCounts:       rowCounts,
	}
	manifestPath := filepath.Join(dayDir, "_manifest.json")
	body, err := json.MarshalIndent(mf, "", "  ")
	if err != nil {
		return mf, err
	}
	if err := os.WriteFile(manifestPath, body, 0o644); err != nil {
		return mf, err
	}
	return mf, nil
}

func scalarInt(ctx context.Context, db *sql.DB, q string) (int64, error) {
	row := db.QueryRowContext(ctx, q)
	var n int64
	if err := row.Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

func escapeSingle(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}
