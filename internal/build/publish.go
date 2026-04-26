package build

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"

	"github.com/pboueri/atproto-db-snapshot/internal/bootstrap"
	"github.com/pboueri/atproto-db-snapshot/internal/config"
	"github.com/pboueri/atproto-db-snapshot/internal/objstore"
)

// publishContext carries everything the post-build steps need. Constructed
// inside Run() once the on-disk artifacts exist.
type publishContext struct {
	cfg    config.Config
	opts   Options
	logger *slog.Logger

	graphPath string
	allPath   string // empty for graph-only modes

	buildMode  string
	buildStart time.Time

	emitBootstrap bool
}

// runGraphPublish handles the publish path for graph-backfill /
// force-rebuild modes (no current_all.duckdb).
func (p publishContext) runGraphPublish(ctx context.Context) error {
	graphSummary, err := hashLocalFile(p.graphPath)
	if err != nil {
		return fmt.Errorf("hash graph: %w", err)
	}

	// Bootstrap (always for force-rebuild; opt-in for plain backfill).
	var bootstrapDate string
	if p.emitBootstrap {
		date := p.opts.Date
		bootDir := filepath.Join(p.cfg.DataDir, "bootstrap", date)
		mf, err := bootstrap.Emit(ctx, p.graphPath, bootDir, 0)
		if err != nil {
			return fmt.Errorf("emit bootstrap: %w", err)
		}
		p.logger.Info("bootstrap emitted", "date", mf.Date, "rows", mf.RowCounts)
		bootstrapDate = string(mf.Date)
		if err := p.uploadBootstrap(ctx, bootDir, date); err != nil {
			return fmt.Errorf("upload bootstrap: %w", err)
		}
	}

	if err := p.uploadSnapshot(ctx, p.graphPath, remoteGraphKey, graphSummary); err != nil {
		return err
	}

	latest := LatestJSON{
		SchemaVersion:        "v1",
		BuiltAt:              time.Now().UTC(),
		BuildMode:            p.buildMode,
		BuildDurationSeconds: int64(time.Since(p.buildStart).Seconds()),
		CurrentGraph:         &ArtifactRef{URL: p.publicURL(remoteGraphKey), SizeBytes: graphSummary.size, SHA256: graphSummary.hex},
		ArchiveBase:          p.publicURL("daily/"),
		BootstrapBase:        p.publicURL("bootstrap/"),
		LatestBootstrapDate:  bootstrapDate,
		FilterConfig:         filterConfigForJSON(p.cfg.Filters),
	}
	return p.uploadLatest(ctx, latest)
}

// runFullPublish handles the publish path for incremental builds:
// upload current_all → current_graph → registry/actors.parquet → latest.json.
func (p publishContext) runFullPublish(ctx context.Context) error {
	// hash + upload current_all
	allSummary, err := hashLocalFile(p.allPath)
	if err != nil {
		return fmt.Errorf("hash all: %w", err)
	}
	graphSummary, err := hashLocalFile(p.graphPath)
	if err != nil {
		return fmt.Errorf("hash graph: %w", err)
	}

	// Optional bootstrap (incremental only emits if --bootstrap set).
	var bootstrapDate string
	if p.emitBootstrap {
		date := p.opts.Date
		bootDir := filepath.Join(p.cfg.DataDir, "bootstrap", date)
		mf, err := bootstrap.Emit(ctx, p.graphPath, bootDir, 0)
		if err != nil {
			return fmt.Errorf("emit bootstrap: %w", err)
		}
		p.logger.Info("bootstrap emitted", "date", mf.Date, "rows", mf.RowCounts)
		bootstrapDate = string(mf.Date)
		if err := p.uploadBootstrap(ctx, bootDir, date); err != nil {
			return fmt.Errorf("upload bootstrap: %w", err)
		}
	}

	// Order matters: snapshots first, then registry, then latest.json (§9).
	if err := p.uploadSnapshot(ctx, p.allPath, remoteAllKey, allSummary); err != nil {
		return err
	}
	if err := p.uploadSnapshot(ctx, p.graphPath, remoteGraphKey, graphSummary); err != nil {
		return err
	}
	regSummary, err := p.publishRegistry(ctx)
	if err != nil {
		return fmt.Errorf("publish registry: %w", err)
	}

	// Read counts for latest.json.
	rowCounts, jetstreamCursor, _ := readAllStoreSummary(p.allPath)

	latest := LatestJSON{
		SchemaVersion:        "v1",
		BuiltAt:              time.Now().UTC(),
		BuildMode:            p.buildMode,
		BuildDurationSeconds: int64(time.Since(p.buildStart).Seconds()),
		CurrentGraph:         &ArtifactRef{URL: p.publicURL(remoteGraphKey), SizeBytes: graphSummary.size, SHA256: graphSummary.hex},
		CurrentAll:           &ArtifactRef{URL: p.publicURL(remoteAllKey), SizeBytes: allSummary.size, SHA256: allSummary.hex},
		ArchiveBase:          p.publicURL("daily/"),
		BootstrapBase:        p.publicURL("bootstrap/"),
		LatestBootstrapDate:  bootstrapDate,
		JetstreamCursor:      jetstreamCursor,
		RowCounts:            rowCounts,
		FilterConfig:         filterConfigForJSON(p.cfg.Filters),
	}
	if regSummary != nil {
		latest.Registry = regSummary
	}

	// Date bounds for the daily archive — useful for consumers.
	latest.OldestDailyDate, latest.NewestDailyDate = readArchiveDateBounds(p.cfg.DataDir)

	if err := p.uploadLatest(ctx, latest); err != nil {
		return err
	}

	// Retention pruning happens AFTER successful publish, never before
	// (§9 step 6/7). Failures here are warnings, not errors.
	if err := pruneLocalDaily(p.cfg.DataDir, p.opts.Date, p.opts.RetainDays, p.logger); err != nil {
		p.logger.Warn("local retention prune failed", "err", err)
	}
	if p.opts.Store != nil && !p.opts.NoUpload {
		if err := pruneRemoteDaily(ctx, p.opts.Store, p.opts.Date, p.cfg.Retention.ParquetDays, p.logger); err != nil {
			p.logger.Warn("remote retention prune failed", "err", err)
		}
	}
	return nil
}

// uploadSnapshot uploads a DuckDB snapshot using the §9 atomic publish
// pattern: PutAtomic to key (which itself uses .new + copy + delete on
// R2/S3, or write-tmp+rename on FileStore).
func (p publishContext) uploadSnapshot(ctx context.Context, path, key string, sum fileSummary) error {
	if p.opts.Store == nil || p.opts.NoUpload {
		p.logger.Info("upload skipped", "key", key, "no_upload", p.opts.NoUpload)
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := p.opts.Store.PutAtomic(ctx, key, f, sum.size); err != nil {
		return fmt.Errorf("upload %s: %w", key, err)
	}
	p.logger.Info("uploaded", "key", key, "bytes", sum.size, "sha256", sum.hex)
	return nil
}

// publishRegistry exports actors_registry from current_all.duckdb to a
// parquet file and uploads it as registry/actors.parquet. Returns the
// uploaded summary or nil if upload was skipped.
func (p publishContext) publishRegistry(ctx context.Context) (*RegistryRef, error) {
	if p.allPath == "" {
		return nil, nil
	}
	regParquet := filepath.Join(p.cfg.DataDir, "registry", "actors.parquet")
	if err := os.MkdirAll(filepath.Dir(regParquet), 0o755); err != nil {
		return nil, err
	}
	_ = os.Remove(regParquet)

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, fmt.Sprintf(
		`ATTACH '%s' AS s (READ_ONLY)`, sqlEscapeSingle(p.allPath))); err != nil {
		return nil, fmt.Errorf("attach all: %w", err)
	}
	copyStmt := fmt.Sprintf(
		`COPY (SELECT actor_id, did, first_seen FROM s.actors_registry ORDER BY actor_id)
		   TO '%s' (FORMAT PARQUET, COMPRESSION zstd, COMPRESSION_LEVEL 6)`,
		sqlEscapeSingle(regParquet),
	)
	if _, err := db.ExecContext(ctx, copyStmt); err != nil {
		return nil, fmt.Errorf("copy registry parquet: %w", err)
	}
	row := db.QueryRowContext(ctx, `SELECT count(*) FROM s.actors_registry`)
	var rowCount int64
	if err := row.Scan(&rowCount); err != nil {
		return nil, fmt.Errorf("count registry: %w", err)
	}

	if p.opts.Store == nil || p.opts.NoUpload {
		fi, _ := os.Stat(regParquet)
		ref := &RegistryRef{
			URL:       p.publicURL(remoteRegistryKey),
			SizeBytes: fi.Size(),
			RowCount:  rowCount,
		}
		return ref, nil
	}

	sum, err := hashLocalFile(regParquet)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(regParquet)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	if err := p.opts.Store.PutAtomic(ctx, remoteRegistryKey, f, sum.size); err != nil {
		return nil, fmt.Errorf("upload registry: %w", err)
	}
	p.logger.Info("uploaded registry", "key", remoteRegistryKey, "rows", rowCount, "bytes", sum.size)
	return &RegistryRef{
		URL:       p.publicURL(remoteRegistryKey),
		SizeBytes: sum.size,
		RowCount:  rowCount,
	}, nil
}

// uploadBootstrap uploads every file in `dir` under the
// bootstrap/<date>/ prefix.
func (p publishContext) uploadBootstrap(ctx context.Context, dir, date string) error {
	if p.opts.Store == nil || p.opts.NoUpload {
		p.logger.Info("bootstrap upload skipped", "no_upload", p.opts.NoUpload)
		return nil
	}
	prefix := "bootstrap/" + date + "/"
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	// Upload data files first; manifest last so consumers don't see a
	// pointer that races ahead of the contents.
	var manifest os.DirEntry
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if e.Name() == "_manifest.json" {
			manifest = e
			continue
		}
		if err := p.uploadFile(ctx, filepath.Join(dir, e.Name()), prefix+e.Name()); err != nil {
			return err
		}
	}
	if manifest != nil {
		if err := p.uploadFile(ctx, filepath.Join(dir, manifest.Name()), prefix+manifest.Name()); err != nil {
			return err
		}
	}
	return nil
}

func (p publishContext) uploadFile(ctx context.Context, path, key string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return err
	}
	if err := p.opts.Store.PutAtomic(ctx, key, f, fi.Size()); err != nil {
		return fmt.Errorf("upload %s: %w", key, err)
	}
	p.logger.Info("uploaded", "key", key, "bytes", fi.Size())
	return nil
}

// uploadLatest writes latest.json LAST (§3.1), so anything that reads
// latest.json sees a fully-coherent set of artifacts.
func (p publishContext) uploadLatest(ctx context.Context, latest LatestJSON) error {
	// Mirror to local data dir first for `serve`.
	latest.JetstreamEndpoint = firstNonEmpty(p.cfg.Jetstream.Endpoints)

	body, err := json.MarshalIndent(latest, "", "  ")
	if err != nil {
		return err
	}
	localPath := filepath.Join(p.cfg.DataDir, "latest.json")
	if err := os.WriteFile(localPath, body, 0o644); err != nil {
		return err
	}
	p.logger.Info("wrote local latest.json", "path", localPath)

	if p.opts.Store == nil || p.opts.NoUpload {
		return nil
	}
	if err := p.opts.Store.PutAtomic(ctx, "latest.json", bytes.NewReader(body), int64(len(body))); err != nil {
		return fmt.Errorf("upload latest.json: %w", err)
	}
	p.logger.Info("uploaded latest.json", "size", len(body))
	return nil
}

func (p publishContext) publicURL(key string) string {
	if p.opts.Store == nil {
		return ""
	}
	return p.opts.Store.PublicURL(key)
}

// fileSummary captures size + hex sha256 of a local file.
type fileSummary struct {
	size int64
	hex  string
}

// hashLocalFile returns size + sha256 of `path` in one streaming pass.
func hashLocalFile(path string) (fileSummary, error) {
	f, err := os.Open(path)
	if err != nil {
		return fileSummary{}, err
	}
	defer f.Close()
	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return fileSummary{}, err
	}
	return fileSummary{size: n, hex: hex.EncodeToString(h.Sum(nil))}, nil
}

// readAllStoreSummary returns row counts (per §3.1) and the largest
// jetstream cursor recorded in _meta of current_all.duckdb. Tolerant: any
// failure returns zeros so latest.json still publishes.
func readAllStoreSummary(allPath string) (map[string]int64, int64, error) {
	out := map[string]int64{}
	db, err := sql.Open("duckdb", fmt.Sprintf("%s?access_mode=READ_ONLY", allPath))
	if err != nil {
		return out, 0, err
	}
	defer db.Close()
	for _, t := range []string{
		"actors", "posts", "likes_current", "reposts_current",
		"follows_current", "blocks_current",
	} {
		row := db.QueryRow("SELECT count(*) FROM " + t)
		var n int64
		if err := row.Scan(&n); err == nil {
			out[t] = n
		}
	}
	var cursor sql.NullInt64
	row := db.QueryRow(`SELECT max(jetstream_cursor) FROM _meta`)
	_ = row.Scan(&cursor)
	c := int64(0)
	if cursor.Valid {
		c = cursor.Int64
	}
	return out, c, nil
}

// readArchiveDateBounds scans data/daily for the min/max YYYY-MM-DD on disk.
// Soft: returns ("", "") if the directory is empty / missing.
func readArchiveDateBounds(dataDir string) (oldest, newest string) {
	dirs, err := listDailyDirs(filepath.Join(dataDir, "daily"))
	if err != nil || len(dirs) == 0 {
		return "", ""
	}
	return dirs[0].day, dirs[len(dirs)-1].day
}

// pruneLocalDaily deletes ./data/daily/YYYY-MM-DD/ directories whose date
// is older than (buildDate - retain).
func pruneLocalDaily(dataDir, buildDate string, retain int, logger *slog.Logger) error {
	if retain <= 0 {
		return nil
	}
	cutoff, err := computeCutoff(buildDate, retain)
	if err != nil {
		return err
	}
	dirs, err := listDailyDirs(filepath.Join(dataDir, "daily"))
	if err != nil {
		return err
	}
	for _, d := range dirs {
		if d.day < cutoff {
			logger.Info("prune local daily", "day", d.day)
			if err := os.RemoveAll(d.path); err != nil {
				logger.Warn("prune failed", "day", d.day, "err", err)
			}
		}
	}
	return nil
}

// pruneRemoteDaily deletes object-store keys under daily/<YYYY-MM-DD>/
// where the date is older than (buildDate - retain). Bootstraps are NEVER
// pruned.
func pruneRemoteDaily(ctx context.Context, store objstore.ObjectStore, buildDate string, retain int, logger *slog.Logger) error {
	if retain <= 0 {
		return nil
	}
	cutoff, err := computeCutoff(buildDate, retain)
	if err != nil {
		return err
	}
	objs, err := store.List(ctx, "daily/")
	if err != nil {
		return err
	}
	for _, o := range objs {
		// Key shape: daily/YYYY-MM-DD/...
		parts := strings.SplitN(o.Key, "/", 3)
		if len(parts) < 3 || !looksLikeYMD(parts[1]) {
			continue
		}
		if parts[1] < cutoff {
			if err := store.Delete(ctx, o.Key); err != nil {
				logger.Warn("remote prune failed", "key", o.Key, "err", err)
				continue
			}
			logger.Info("pruned remote daily", "key", o.Key)
		}
	}
	return nil
}

// computeCutoff returns the YYYY-MM-DD that's `retain` days strictly
// before `buildDate`. Days with date < cutoff are pruned.
func computeCutoff(buildDate string, retain int) (string, error) {
	t, err := time.Parse("2006-01-02", buildDate)
	if err != nil {
		return "", fmt.Errorf("invalid build date %q: %w", buildDate, err)
	}
	return t.AddDate(0, 0, -retain).Format("2006-01-02"), nil
}

func firstNonEmpty(ss []string) string {
	for _, s := range ss {
		if s != "" {
			return s
		}
	}
	return ""
}

