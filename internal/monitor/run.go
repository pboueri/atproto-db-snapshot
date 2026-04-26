package monitor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "modernc.org/sqlite" // pure-Go sqlite driver, registers as "sqlite"

	"github.com/pboueri/atproto-db-snapshot/internal/config"
)

// readRun inspects the run-state cursor sqlite (if present) and the raw/
// tree in object storage to report run-pipeline health.
func readRun(ctx context.Context, cfg config.Config, deps Deps) RunStatus {
	st := RunStatus{
		RawDates:             []string{},
		RawFilesByCollection: map[string]int{},
	}

	cursorPath := filepath.Join(cfg.DataDir, "run-state", "cursor.sqlite")
	if _, err := os.Stat(cursorPath); err == nil {
		if err := fillCursorFromSQLite(ctx, cursorPath, &st); err != nil {
			slog.Warn("monitor: read run cursor", "path", cursorPath, "err", err)
			st.Error = errMessage(err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		st.Error = errMessage(err)
	}

	if err := fillRawFromObjStore(ctx, deps, &st); err != nil {
		slog.Warn("monitor: list raw/", "err", err)
		if st.Error == "" {
			st.Error = errMessage(err)
		}
	}
	return st
}

// fillCursorFromSQLite opens the sqlite cursor file read-only and reads the
// jetstream cursor row. We open with mode=ro so we never block a concurrent
// writer.
func fillCursorFromSQLite(ctx context.Context, path string, st *RunStatus) error {
	db, err := openSQLiteReadOnly(path)
	if err != nil {
		return err
	}
	defer db.Close()

	// We deliberately read the row with the largest micros value rather
	// than a fixed primary key — the run command writes a single 'jetstream'
	// row, but reading the max keeps the monitor compatible if the schema
	// later grows additional named cursors.
	var micros sql.NullInt64
	row := db.QueryRowContext(ctx, "SELECT max(micros) FROM cursor")
	switch err := row.Scan(&micros); {
	case err == nil:
		if micros.Valid {
			m := micros.Int64
			st.CursorMicros = &m
			t := time.UnixMicro(m).UTC()
			st.CursorAt = &t
		}
	case errors.Is(err, sql.ErrNoRows):
		// Empty cursor table — leave fields nil.
	default:
		return fmt.Errorf("read cursor: %w", err)
	}
	return nil
}

// openSQLiteReadOnly opens path with the modernc sqlite driver in read-only
// mode and Pings it to surface schema or open errors immediately.
func openSQLiteReadOnly(path string) (*sql.DB, error) {
	// modernc.org/sqlite reads file: URIs when the path starts with file:.
	// Using a URI lets us pass mode=ro reliably across platforms.
	uri := "file:" + url.PathEscape(path) + "?mode=ro"
	db, err := sql.Open("sqlite", uri)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if err := db.PingContext(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// fillRawFromObjStore lists raw/ in objstore and aggregates per-day and
// per-collection counts. The raw layout is
// raw/YYYY-MM-DD/{collection}-{...}.parquet (see internal/rawio); we group
// on those two segments.
func fillRawFromObjStore(ctx context.Context, deps Deps, st *RunStatus) error {
	objs, err := deps.ObjStore.List(ctx, "raw/")
	if err != nil {
		return err
	}
	dateSet := map[string]struct{}{}
	for _, o := range objs {
		// Ignore non-parquet files (e.g. recovery sidecars).
		if filepath.Ext(o.Path) != ".parquet" {
			continue
		}
		parts := strings.Split(o.Path, "/")
		if len(parts) < 3 || parts[0] != "raw" {
			continue
		}
		date := parts[1]
		// rawio writes recovered files to raw/recovered/ — count them in
		// the totals but don't pollute raw_dates with a non-date label.
		isDate := true
		if _, perr := time.Parse("2006-01-02", date); perr != nil {
			isDate = false
		}
		base := parts[len(parts)-1]
		// rawio writes {collection}-{nanos}-{seq}.parquet; the collection
		// is the slice up to the first dash. Files that don't follow the
		// pattern are still counted as files but bucketed under "other".
		coll := "other"
		if idx := strings.Index(base, "-"); idx > 0 {
			coll = base[:idx]
		}
		if isDate {
			dateSet[date] = struct{}{}
		}
		st.RawFilesByCollection[coll]++
		st.RawFilesTotal++
	}
	for d := range dateSet {
		st.RawDates = append(st.RawDates, d)
	}
	sort.Strings(st.RawDates)
	return nil
}
