package snapshot

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

// rawFiles enumerates the parquet shards present for each collection under
// rawRoot. The pipeline switches behavior on whether any files exist for a
// given collection: an empty list cannot be passed to read_parquet without
// erroring.
type rawFiles struct {
	profiles []string
	follows  []string
	blocks   []string
	posts    []string
	media    []string
	likes    []string
	reposts  []string
}

// listRawFiles globs each collection's shards under rawRoot/*/<collection>-*.parquet.
// Returns empty slices for collections with no shards rather than errors so the
// SQL builder can render an empty stub.
func listRawFiles(rawRoot string) (rawFiles, error) {
	var f rawFiles
	for _, pair := range []struct {
		dst        *[]string
		collection string
	}{
		{&f.profiles, "profiles"},
		{&f.follows, "follows"},
		{&f.blocks, "blocks"},
		{&f.posts, "posts"},
		{&f.media, "post_media"},
		{&f.likes, "likes"},
		{&f.reposts, "reposts"},
	} {
		matches, err := filepath.Glob(filepath.Join(rawRoot, "*", pair.collection+"-*.parquet"))
		if err != nil {
			return rawFiles{}, fmt.Errorf("glob %s: %w", pair.collection, err)
		}
		*pair.dst = matches
	}
	return f, nil
}

// buildGraphDB materializes the current-state social graph DuckDB file.
//
// The output schema mirrors the bootstrap baseline plus an actor_aggs table.
// The graph tables are NOT bounded by the snapshot window — bootstrap captured
// the world at one moment and we apply every observed create/delete since.
func buildGraphDB(ctx context.Context, dst, bootstrapPath, rawRoot, memLimit string, _, _ time.Time) (map[string]int64, error) {
	files, err := listRawFiles(rawRoot)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("duckdb", dst)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	if err := applyMemoryLimit(ctx, db, memLimit); err != nil {
		return nil, err
	}
	if err := attachAndBuildGraph(ctx, db, bootstrapPath, files); err != nil {
		return nil, err
	}
	return tableCounts(ctx, db, "actors", "follows", "blocks", "actor_aggs")
}

// buildAllDB materializes current_all.duckdb: the graph plus window-bounded
// post / engagement tables and aggregates. The window is half-open
// (start, end]; we anchor on indexed_at because that is the snapshotter's
// own observation timestamp and is monotonic regardless of clock skew on the
// authoring side.
func buildAllDB(ctx context.Context, dst, bootstrapPath, rawRoot, memLimit string, windowStart, windowEnd time.Time) (map[string]int64, error) {
	files, err := listRawFiles(rawRoot)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("duckdb", dst)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	if err := applyMemoryLimit(ctx, db, memLimit); err != nil {
		return nil, err
	}
	if err := attachAndBuildGraph(ctx, db, bootstrapPath, files); err != nil {
		return nil, err
	}
	if err := buildWindowedPosts(ctx, db, files, windowStart, windowEnd); err != nil {
		return nil, err
	}
	return tableCounts(ctx, db,
		"actors", "follows", "blocks", "actor_aggs",
		"posts", "post_media", "likes", "reposts", "post_aggs",
	)
}

// attachAndBuildGraph wires the bootstrap baseline as ATTACH then runs the
// CREATE TABLE AS SELECT pipeline that produces actors / follows / blocks /
// actor_aggs in the working database.
func attachAndBuildGraph(ctx context.Context, db *sql.DB, bootstrapPath string, files rawFiles) error {
	// READ_ONLY is critical: the bootstrap file is treated as immutable per
	// the spec, and DuckDB will error on attach if a stale lock file is
	// present without it.
	attach := fmt.Sprintf("ATTACH '%s' AS bootstrap (READ_ONLY)", escapeSQLLiteral(bootstrapPath))
	if _, err := db.ExecContext(ctx, attach); err != nil {
		return fmt.Errorf("attach bootstrap %s: %w", bootstrapPath, err)
	}
	defer func() {
		_, _ = db.ExecContext(ctx, "DETACH bootstrap")
	}()

	stmts := graphSQL(files)
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("graph stmt failed: %w\nsql: %s", err, s)
		}
	}
	return nil
}

// buildWindowedPosts produces posts / post_media / likes / reposts / post_aggs
// and extends actor_aggs with `_in_window` columns. Run after the graph
// pipeline so actor_aggs already exists.
func buildWindowedPosts(ctx context.Context, db *sql.DB, files rawFiles, windowStart, windowEnd time.Time) error {
	stmts := windowSQL(files, windowStart, windowEnd)
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("window stmt failed: %w\nsql: %s", err, s)
		}
	}
	return nil
}

// tableCounts queries SELECT count(*) for each named table.
func tableCounts(ctx context.Context, db *sql.DB, tables ...string) (map[string]int64, error) {
	out := make(map[string]int64, len(tables))
	for _, t := range tables {
		var n int64
		if err := db.QueryRowContext(ctx, fmt.Sprintf("SELECT count(*) FROM %s", t)).Scan(&n); err != nil {
			return nil, fmt.Errorf("count %s: %w", t, err)
		}
		out[t] = n
	}
	return out, nil
}

// escapeSQLLiteral returns s with single-quotes doubled — adequate for the
// values we splice (filesystem paths from objstore.URL).
func escapeSQLLiteral(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			out = append(out, '\'')
		}
		out = append(out, s[i])
	}
	return string(out)
}

// readParquetExpr renders a read_parquet([...]) expression for the given
// concrete file list, with union_by_name=true so older parquet schemas missing
// newer columns are tolerated. Returns "" when the list is empty so callers
// can branch to an empty-stub SELECT.
func readParquetExpr(files []string) string {
	if len(files) == 0 {
		return ""
	}
	quoted := make([]string, 0, len(files))
	for _, f := range files {
		quoted = append(quoted, "'"+escapeSQLLiteral(f)+"'")
	}
	return fmt.Sprintf("read_parquet([%s], union_by_name=true)", strings.Join(quoted, ", "))
}
