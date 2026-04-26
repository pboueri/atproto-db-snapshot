package serve

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
	_ "modernc.org/sqlite"
)

// readCursor reads DataDir/cursor.json. Missing file is not an error; the
// returned bool indicates presence.
type cursorState struct {
	Cursor    int64     `json:"cursor"`
	Endpoint  string    `json:"endpoint"`
	UpdatedAt time.Time `json:"updated_at"`
	Schema    string    `json:"schema_version"`
}

func readCursor(dataDir string) (cursorState, bool, error) {
	path := filepath.Join(dataDir, "cursor.json")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cursorState{}, false, nil
		}
		return cursorState{}, false, err
	}
	var c cursorState
	if err := json.Unmarshal(data, &c); err != nil {
		return cursorState{}, false, err
	}
	return c, true, nil
}

// readLatest returns the contents of latest.json as a generic JSON object,
// preserving forward-compat (additive) fields. Missing file is not an
// error.
func readLatest(dataDir string) (map[string]any, bool, error) {
	path := filepath.Join(dataDir, "latest.json")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, err
	}
	var v map[string]any
	if err := json.Unmarshal(data, &v); err != nil {
		return nil, false, err
	}
	return v, true, nil
}

// tailFile reads the last n lines of a text file. For a 20k-line tail of
// a slog JSONL log this is fast enough; we don't bother seeking backward.
func tailFile(path string, n int) ([]string, error) {
	if n <= 0 {
		return nil, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	// slog JSONL lines can be long; allow up to 1 MiB per line.
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	ring := make([]string, 0, n)
	for scanner.Scan() {
		line := scanner.Text()
		if len(ring) < n {
			ring = append(ring, line)
		} else {
			copy(ring, ring[1:])
			ring[len(ring)-1] = line
		}
	}
	if err := scanner.Err(); err != nil {
		return ring, err
	}
	return ring, nil
}

// logRecord is the subset of a slog JSONL record we care about.
type logRecord struct {
	Time   time.Time
	Level  string
	Msg    string
	Fields map[string]any
}

// parseLogLine decodes a single slog JSONL record. Returns ok=false on a
// non-JSON line (our dashboard is forgiving).
func parseLogLine(line string) (logRecord, bool) {
	var raw map[string]any
	if err := json.Unmarshal([]byte(line), &raw); err != nil {
		return logRecord{}, false
	}
	rec := logRecord{Fields: raw}
	if t, ok := raw["time"].(string); ok {
		if parsed, err := time.Parse(time.RFC3339Nano, t); err == nil {
			rec.Time = parsed
		}
	}
	if lvl, ok := raw["level"].(string); ok {
		rec.Level = strings.ToUpper(lvl)
	}
	if m, ok := raw["msg"].(string); ok {
		rec.Msg = m
	}
	return rec, true
}

// eventsPerSec scans tail lines for slog records that look like Jetstream
// events (level=DEBUG/INFO, msg often "event" or fields including
// time_us/collection) and computes a rolling rate over the last 60s.
//
// We're permissive: any record with field "collection" set counts as an
// event. Returns zero when there's no signal.
func eventsPerSec(lines []string) (perSec float64, byKind map[string]int) {
	byKind = make(map[string]int)
	if len(lines) == 0 {
		return 0, byKind
	}
	now := time.Now()
	since := now.Add(-60 * time.Second)
	count := 0
	var earliest time.Time
	for _, ln := range lines {
		rec, ok := parseLogLine(ln)
		if !ok {
			continue
		}
		coll, _ := rec.Fields["collection"].(string)
		if coll == "" {
			continue
		}
		if !rec.Time.IsZero() && rec.Time.Before(since) {
			continue
		}
		if earliest.IsZero() || rec.Time.Before(earliest) {
			earliest = rec.Time
		}
		count++
		byKind[coll]++
	}
	if count == 0 {
		return 0, byKind
	}
	window := 60.0
	if !earliest.IsZero() {
		w := now.Sub(earliest).Seconds()
		if w > 0 && w < window {
			window = w
		}
	}
	if window <= 0 {
		window = 1
	}
	return float64(count) / window, byKind
}

// errorSummary counts WARN/ERROR records in a tail and returns a small
// sample of the most recent ones.
type errorSummary struct {
	WarnCount  int             `json:"warn_count"`
	ErrorCount int             `json:"error_count"`
	Samples    []errorSample   `json:"samples"`
	ByLevel    map[string]int  `json:"by_level"`
}

type errorSample struct {
	Time  time.Time `json:"time"`
	Level string    `json:"level"`
	Msg   string    `json:"msg"`
}

func summarizeErrors(lines []string, since time.Duration, sampleN int) errorSummary {
	out := errorSummary{ByLevel: make(map[string]int)}
	cutoff := time.Now().Add(-since)
	// Walk newest-first to fill the sample window.
	for i := len(lines) - 1; i >= 0; i-- {
		rec, ok := parseLogLine(lines[i])
		if !ok {
			continue
		}
		if since > 0 && !rec.Time.IsZero() && rec.Time.Before(cutoff) {
			continue
		}
		switch rec.Level {
		case "ERROR":
			out.ErrorCount++
			out.ByLevel["ERROR"]++
		case "WARN", "WARNING":
			out.WarnCount++
			out.ByLevel["WARN"]++
		default:
			continue
		}
		if len(out.Samples) < sampleN {
			out.Samples = append(out.Samples, errorSample{
				Time:  rec.Time,
				Level: rec.Level,
				Msg:   rec.Msg,
			})
		}
	}
	return out
}

// dailyDirInfo describes one sealed day directory under DataDir/daily/.
type dailyDirInfo struct {
	Date  string `json:"date"`
	Bytes int64  `json:"bytes"`
}

// listDailyDirs walks DataDir/daily/, returning one entry per
// `YYYY-MM-DD` subdirectory sorted ascending by date.
func listDailyDirs(dataDir string) ([]dailyDirInfo, error) {
	root := filepath.Join(dataDir, "daily")
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var out []dailyDirInfo
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		// Only YYYY-MM-DD shaped names.
		if _, err := time.Parse("2006-01-02", name); err != nil {
			continue
		}
		size, _ := dirSize(filepath.Join(root, name))
		out = append(out, dailyDirInfo{Date: name, Bytes: size})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Date < out[j].Date })
	return out, nil
}

// dirSize sums the sizes of all regular files under root. Errors on
// individual entries are swallowed — best-effort measurement.
func dirSize(root string) (int64, error) {
	var total int64
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // tolerate transient errors
		}
		if d.IsDir() {
			return nil
		}
		if info, err := d.Info(); err == nil {
			total += info.Size()
		}
		return nil
	})
	return total, err
}

// largestFile returns the path and size of the largest regular file under
// root (recursive). Returns ("", 0) if root does not exist.
func largestFile(root string) (string, int64) {
	var bestPath string
	var bestSize int64
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		if info.Size() > bestSize {
			bestSize = info.Size()
			bestPath = path
		}
		return nil
	})
	return bestPath, bestSize
}

// todayShard summarizes the day-in-progress in DataDir/staging.db. Counts
// rows per collection where day = today (UTC). Returns (nil, false) if
// staging.db is absent or unreadable.
type todayShardInfo struct {
	Date         string         `json:"date"`
	Events       int64          `json:"events"`
	ByCollection map[string]int64 `json:"by_collection"`
}

func todayShard(ctx context.Context, dataDir string) (todayShardInfo, bool, error) {
	path := filepath.Join(dataDir, "staging.db")
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return todayShardInfo{}, false, nil
		}
		return todayShardInfo{}, false, err
	}
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(2000)&mode=ro", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return todayShardInfo{}, false, err
	}
	defer db.Close()

	// Same staging-table layout used by internal/run/staging.go.
	tables := map[string]string{
		"app.bsky.feed.post":     "staging_events_post",
		"app.bsky.feed.like":     "staging_events_like",
		"app.bsky.feed.repost":   "staging_events_repost",
		"app.bsky.graph.follow":  "staging_events_follow",
		"app.bsky.graph.block":   "staging_events_block",
		"app.bsky.actor.profile": "staging_events_profile",
	}
	today := time.Now().UTC().Format("2006-01-02")
	out := todayShardInfo{Date: today, ByCollection: make(map[string]int64)}
	for coll, tbl := range tables {
		var n int64
		row := db.QueryRowContext(ctx, fmt.Sprintf(
			"SELECT COUNT(*) FROM %s WHERE day = ?", tbl), today)
		if err := row.Scan(&n); err != nil {
			// Table may not exist yet; treat as zero.
			continue
		}
		out.ByCollection[coll] = n
		out.Events += n
	}
	return out, true, nil
}

// graphMeta opens current_graph.duckdb in READ_ONLY mode and returns the
// `_meta` row plus key row counts.
type graphMetaInfo struct {
	SchemaVersion   string         `json:"schema_version"`
	BuiltAt         time.Time      `json:"built_at"`
	JetstreamCursor int64          `json:"jetstream_cursor"`
	BuildMode       string         `json:"build_mode"`
	FilterConfig    string         `json:"filter_config"`
	RowCounts       map[string]int64 `json:"row_counts"`
}

func graphMeta(ctx context.Context, dataDir string) (graphMetaInfo, bool, error) {
	path := filepath.Join(dataDir, "current_graph.duckdb")
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return graphMetaInfo{}, false, nil
		}
		return graphMetaInfo{}, false, err
	}

	// Open an in-memory DuckDB and ATTACH the file READ_ONLY so we never
	// block a writer.
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return graphMetaInfo{}, false, err
	}
	defer db.Close()
	// 2-second timeout — if we can't attach quickly, just give up.
	attachCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if _, err := db.ExecContext(attachCtx, fmt.Sprintf(
		"ATTACH '%s' AS g (READ_ONLY)", strings.ReplaceAll(path, "'", "''"))); err != nil {
		return graphMetaInfo{}, false, err
	}

	out := graphMetaInfo{RowCounts: make(map[string]int64)}
	// _meta is single-row by spec §5.
	row := db.QueryRowContext(attachCtx, `SELECT schema_version, built_at, jetstream_cursor, build_mode, filter_config FROM g._meta LIMIT 1`)
	var (
		sv   sql.NullString
		bat  sql.NullTime
		cur  sql.NullInt64
		mode sql.NullString
		fc   sql.NullString
	)
	if err := row.Scan(&sv, &bat, &cur, &mode, &fc); err == nil {
		out.SchemaVersion = sv.String
		if bat.Valid {
			out.BuiltAt = bat.Time
		}
		out.JetstreamCursor = cur.Int64
		out.BuildMode = mode.String
		out.FilterConfig = fc.String
	}
	for _, t := range []string{"actors", "follows_current", "blocks_current"} {
		var n int64
		if err := db.QueryRowContext(attachCtx, fmt.Sprintf("SELECT COUNT(*) FROM g.%s", t)).Scan(&n); err == nil {
			out.RowCounts[t] = n
		}
	}
	return out, true, nil
}
