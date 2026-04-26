package archive

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	duckdb "github.com/marcboeker/go-duckdb/v2"
)

// Writer emits one day of parquet event-log files plus a _manifest.json
// to a local directory. Backed by a single in-memory DuckDB connection
// that is reused across WriteX calls.
//
// Not safe for concurrent use. Each WriteX should be called at most
// once per Writer (per day).
type Writer struct {
	dir       string
	connector *duckdb.Connector
	conn      driver.Conn
}

// NewWriter prepares dayDir (mkdir -p) and opens an in-memory DuckDB
// constrained to a small memory + thread footprint — the writer should
// not eat the machine while a Jetstream consumer or build job is running
// alongside it.
func NewWriter(ctx context.Context, dayDir string) (*Writer, error) {
	if err := os.MkdirAll(dayDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %q: %w", dayDir, err)
	}

	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		return nil, fmt.Errorf("duckdb connector: %w", err)
	}
	conn, err := connector.Connect(ctx)
	if err != nil {
		connector.Close()
		return nil, fmt.Errorf("duckdb connect: %w", err)
	}

	w := &Writer{dir: dayDir, connector: connector, conn: conn}

	// Constrain the in-memory DuckDB so the archive writer stays a
	// well-behaved sidecar. 512 MB / 2 threads is plenty for a day's
	// worth of events, which is small relative to a build-time
	// snapshot.
	for _, q := range []string{
		`SET memory_limit = '512MB'`,
		`SET threads = 2`,
		// Required so ROW_GROUP_SIZE_BYTES takes effect on COPY ... TO PARQUET.
		// We supply ORDER BY in the COPY SELECT explicitly when sort matters.
		`SET preserve_insertion_order = false`,
	} {
		if err := execNoArgs(ctx, conn, q); err != nil {
			w.Close()
			return nil, fmt.Errorf("duckdb pragma %q: %w", q, err)
		}
	}
	return w, nil
}

// Close releases the in-memory DuckDB. Always safe to call.
func (w *Writer) Close() error {
	var firstErr error
	if w.conn != nil {
		if err := w.conn.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		w.conn = nil
	}
	if w.connector != nil {
		if err := w.connector.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		w.connector = nil
	}
	return firstErr
}

// outPath returns the absolute path for a given parquet basename.
func (w *Writer) outPath(name string) string {
	return filepath.Join(w.dir, name)
}

// copyOptions is the COPY ... TO suffix used for every parquet file:
// flat layout, zstd level 6, 128 MB row groups (§6).
const copyOptions = `(FORMAT PARQUET, COMPRESSION zstd, COMPRESSION_LEVEL 6, ROW_GROUP_SIZE_BYTES 134217728)`

// stageAndCopy is the common pattern: create a TEMP staging table from
// `createSQL`, run `appendFn` against an Appender bound to it, then COPY
// (selectSQL) TO outPath. selectSQL must contain a literal `DATE 'YYYY-MM-DD'`
// projection for the `date` column and an ORDER BY for sort-within-row-group.
//
// Returns (rows, bytes, err). If rows == 0 the file is NOT written and
// outPath stays absent (per task notes: don't create zero-row parquet).
func (w *Writer) stageAndCopy(
	ctx context.Context,
	stageTable string,
	createSQL string,
	selectSQL string,
	outPath string,
	appendFn func(app *duckdb.Appender) (int64, error),
) (int64, int64, error) {
	// Drop any previous staging table by the same name so a Writer can
	// be reused (defensive — normal flow uses a fresh Writer per day).
	_ = execNoArgs(ctx, w.conn, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, stageTable))
	if err := execNoArgs(ctx, w.conn, createSQL); err != nil {
		return 0, 0, fmt.Errorf("create %s: %w", stageTable, err)
	}
	defer execNoArgs(ctx, w.conn, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, stageTable))

	app, err := duckdb.NewAppenderFromConn(w.conn, "", stageTable)
	if err != nil {
		return 0, 0, fmt.Errorf("appender %s: %w", stageTable, err)
	}
	rows, appendErr := appendFn(app)
	if appendErr != nil {
		_ = app.Close()
		return 0, 0, fmt.Errorf("append %s: %w", stageTable, appendErr)
	}
	if err := app.Close(); err != nil {
		return 0, 0, fmt.Errorf("close appender %s: %w", stageTable, err)
	}

	if rows == 0 {
		return 0, 0, nil
	}

	// Best-effort: blow away any stale shadow before writing.
	_ = os.Remove(outPath)

	copyStmt := fmt.Sprintf(
		`COPY (%s) TO '%s' %s`,
		selectSQL,
		escapeSingleQuotes(outPath),
		copyOptions,
	)
	if err := execNoArgs(ctx, w.conn, copyStmt); err != nil {
		return 0, 0, fmt.Errorf("copy %s: %w", stageTable, err)
	}

	fi, err := os.Stat(outPath)
	if err != nil {
		return 0, 0, fmt.Errorf("stat %s: %w", outPath, err)
	}
	return rows, fi.Size(), nil
}

// dateLit returns the SQL DATE literal for a Day, e.g. DATE '2026-04-24'.
func dateLit(day Day) string {
	return fmt.Sprintf("DATE '%s'", escapeSingleQuotes(string(day)))
}

// execNoArgs runs a no-arg statement against a driver.Conn.
func execNoArgs(ctx context.Context, conn driver.Conn, query string) error {
	execer, ok := conn.(driver.ExecerContext)
	if !ok {
		return fmt.Errorf("connection does not support ExecerContext")
	}
	_, err := execer.ExecContext(ctx, query, nil)
	return err
}

func escapeSingleQuotes(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// nilIfEmpty returns nil when s is "" so that DuckDB stores NULL.
func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// WriteManifest writes _manifest.json to the day directory and returns
// its absolute path. Overwrites any pre-existing manifest.
func (w *Writer) WriteManifest(ctx context.Context, m Manifest) (string, error) {
	if m.SchemaVersion == "" {
		m.SchemaVersion = "v1"
	}
	if m.RowCounts == nil {
		m.RowCounts = map[string]int64{}
	}
	if m.Bytes == nil {
		m.Bytes = map[string]int64{}
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return "", err
	}
	path := filepath.Join(w.dir, "_manifest.json")
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return "", err
	}
	return path, nil
}
