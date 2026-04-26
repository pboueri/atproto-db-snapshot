package serve

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pboueri/atproto-db-snapshot/internal/objstore"
)

// server wires Options + logger to a set of HTTP handlers. Stateless —
// no per-request mutex or cache.
type server struct {
	opts   Options
	logger *slog.Logger
	tmpl   *template.Template
}

func newServer(opts Options, logger *slog.Logger) *server {
	if logger == nil {
		logger = newDiscardLogger()
	}
	return &server{
		opts:   opts,
		logger: logger,
		tmpl:   template.Must(template.New("dashboard").Funcs(tmplFuncs).Parse(dashboardHTML)),
	}
}

// newDiscardLogger returns a slog.Logger that throws messages away.
// Convenient for tests.
func newDiscardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(discardWriter{}, &slog.HandlerOptions{
		Level: slog.LevelError,
	}))
}

type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleDashboard)
	mux.HandleFunc("/api/summary", s.handleSummary)
	mux.HandleFunc("/api/jetstream", s.handleJetstream)
	mux.HandleFunc("/api/build", s.handleBuild)
	mux.HandleFunc("/api/disk", s.handleDisk)
	mux.HandleFunc("/api/retention", s.handleRetention)
	mux.HandleFunc("/api/errors", s.handleErrors)
	mux.HandleFunc("/metrics", s.handleMetrics)
	return mux
}

// summary is the umbrella JSON returned by /api/summary. Cards in the
// dashboard each correspond to one field. Anything unavailable is null;
// the template renders a `—` for nulls.
type summary struct {
	GeneratedAt time.Time `json:"generated_at"`
	DataDir     string    `json:"data_dir"`

	Jetstream   *jetstreamCard `json:"jetstream"`
	LastBuild   *buildCard     `json:"last_build"`
	TodayShard  *todayCard     `json:"today_shard"`
	Retention   *retentionCard `json:"retention"`
	Disk        *diskCard      `json:"disk"`
	ObjectStore *objstoreCard  `json:"object_store"`
	Errors      *errorSummary  `json:"errors"`
}

type jetstreamCard struct {
	Cursor        int64           `json:"cursor"`
	Endpoint      string          `json:"endpoint"`
	UpdatedAt     time.Time       `json:"updated_at"`
	LagSeconds    float64         `json:"lag_seconds"`
	EventsPerSec  float64         `json:"events_per_sec"`
	ByKind        map[string]int  `json:"by_kind"`
	LastFlush     string          `json:"last_flush"`
}

type buildCard struct {
	BuiltAt          time.Time         `json:"built_at"`
	Mode             string            `json:"mode"`
	DurationSeconds  float64           `json:"duration_seconds"`
	RowCounts        map[string]any    `json:"row_counts"`
	SchemaVersion    string            `json:"schema_version"`
	Source           string            `json:"source"` // "latest.json" or "_meta"
	JetstreamCursor  int64             `json:"jetstream_cursor"`
}

type todayCard struct {
	Date            string           `json:"date"`
	Bytes           int64            `json:"bytes"`
	Events          int64            `json:"events"`
	ByCollection    map[string]int64 `json:"by_collection"`
	EstSealUTC      string           `json:"est_seal_utc"`
}

type retentionCard struct {
	Days        int            `json:"days"`
	TotalBytes  int64          `json:"total_bytes"`
	OldestDate  string         `json:"oldest_date"`
	NewestDate  string         `json:"newest_date"`
	NextDrops   string         `json:"next_drops"`
	Entries     []dailyDirInfo `json:"entries"`
}

type diskCard struct {
	Mount        string `json:"mount"`
	TotalBytes   uint64 `json:"total_bytes"`
	FreeBytes    uint64 `json:"free_bytes"`
	UsedBytes    uint64 `json:"used_bytes"`
	DataDirBytes int64  `json:"data_dir_bytes"`
	LargestFile  string `json:"largest_file"`
	LargestBytes int64  `json:"largest_bytes"`
}

type objstoreCard struct {
	Available      bool   `json:"available"`
	DailyCount     int    `json:"daily_count"`
	BootstrapCount int    `json:"bootstrap_count"`
	LatestBootstrap string `json:"latest_bootstrap"`
}

// ---------- handlers ----------

func (s *server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	sum := s.buildSummary(r.Context())
	data := struct {
		Summary        summary
		RefreshSeconds int
	}{Summary: sum, RefreshSeconds: s.opts.RefreshSeconds}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.tmpl.Execute(w, data); err != nil {
		s.logger.Warn("dashboard render", "err", err)
	}
}

func (s *server) handleSummary(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, s.buildSummary(r.Context()))
}

func (s *server) handleJetstream(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, s.jetstreamSummary())
}

func (s *server) handleBuild(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, s.buildSummaryCard(r.Context()))
}

func (s *server) handleDisk(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, s.diskSummary())
}

func (s *server) handleRetention(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, s.retentionSummary())
}

func (s *server) handleErrors(w http.ResponseWriter, r *http.Request) {
	since := 10 * time.Minute
	if v := r.URL.Query().Get("since"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			since = d
		}
	}
	lines := s.combinedLogTail()
	writeJSON(w, summarizeErrors(lines, since, 20))
}

// handleMetrics writes Prometheus text-format metrics. We hand-roll the
// format to avoid pulling in client_golang as a direct dep.
func (s *server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	sum := s.buildSummary(r.Context())

	type metric struct {
		name  string
		help  string
		typ   string
		lines []string
	}
	var ms []metric

	// jetstream_cursor_us
	if sum.Jetstream != nil {
		ms = append(ms, metric{
			name: "at_snapshotter_jetstream_cursor_us",
			help: "Largest Jetstream cursor (microseconds since UNIX epoch) committed to staging.",
			typ:  "gauge",
			lines: []string{fmt.Sprintf("at_snapshotter_jetstream_cursor_us %d", sum.Jetstream.Cursor)},
		})
		ms = append(ms, metric{
			name: "at_snapshotter_jetstream_lag_seconds",
			help: "Seconds between now and the latest Jetstream cursor.",
			typ:  "gauge",
			lines: []string{fmt.Sprintf("at_snapshotter_jetstream_lag_seconds %g", sum.Jetstream.LagSeconds)},
		})
		var kindLines []string
		// stable order
		kinds := make([]string, 0, len(sum.Jetstream.ByKind))
		for k := range sum.Jetstream.ByKind {
			kinds = append(kinds, k)
		}
		sort.Strings(kinds)
		for _, k := range kinds {
			kindLines = append(kindLines, fmt.Sprintf(
				"at_snapshotter_jetstream_events_total{kind=%q} %d",
				k, sum.Jetstream.ByKind[k]))
		}
		ms = append(ms, metric{
			name:  "at_snapshotter_jetstream_events_total",
			help:  "Count of Jetstream events seen in the recent log tail, by collection.",
			typ:   "counter",
			lines: kindLines,
		})
	}

	// build_*
	if sum.LastBuild != nil {
		var lines []string
		if !sum.LastBuild.BuiltAt.IsZero() {
			lines = append(lines, fmt.Sprintf(
				"at_snapshotter_build_last_success_timestamp %d", sum.LastBuild.BuiltAt.Unix()))
		}
		ms = append(ms, metric{
			name: "at_snapshotter_build_last_success_timestamp",
			help: "Unix timestamp of the last successful build (built_at).",
			typ:  "gauge", lines: lines,
		})
		ms = append(ms, metric{
			name: "at_snapshotter_build_duration_seconds",
			help: "Wall-clock duration of the last build in seconds.",
			typ:  "gauge",
			lines: []string{fmt.Sprintf("at_snapshotter_build_duration_seconds %g", sum.LastBuild.DurationSeconds)},
		})
		var rowLines []string
		keys := make([]string, 0, len(sum.LastBuild.RowCounts))
		for k := range sum.LastBuild.RowCounts {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			n, ok := numericish(sum.LastBuild.RowCounts[k])
			if !ok {
				continue
			}
			rowLines = append(rowLines, fmt.Sprintf(
				"at_snapshotter_build_rows{table=%q} %d", k, n))
		}
		ms = append(ms, metric{
			name:  "at_snapshotter_build_rows",
			help:  "Row counts by table from the last build.",
			typ:   "gauge",
			lines: rowLines,
		})
	}

	// disk_free_bytes
	if sum.Disk != nil {
		ms = append(ms, metric{
			name: "at_snapshotter_disk_free_bytes",
			help: "Free bytes on the filesystem hosting DataDir.",
			typ:  "gauge",
			lines: []string{fmt.Sprintf(
				"at_snapshotter_disk_free_bytes{mount=%q} %d",
				sum.Disk.Mount, sum.Disk.FreeBytes)},
		})
	}

	// retention_local_days
	if sum.Retention != nil {
		ms = append(ms, metric{
			name: "at_snapshotter_retention_local_days",
			help: "Number of YYYY-MM-DD directories present under DataDir/daily/.",
			typ:  "gauge",
			lines: []string{fmt.Sprintf("at_snapshotter_retention_local_days %d", sum.Retention.Days)},
		})
	}

	// errors_total
	if sum.Errors != nil {
		var lines []string
		levels := make([]string, 0, len(sum.Errors.ByLevel))
		for k := range sum.Errors.ByLevel {
			levels = append(levels, k)
		}
		sort.Strings(levels)
		for _, k := range levels {
			lines = append(lines, fmt.Sprintf(
				"at_snapshotter_errors_total{level=%q} %d", k, sum.Errors.ByLevel[k]))
		}
		ms = append(ms, metric{
			name:  "at_snapshotter_errors_total",
			help:  "Count of WARN/ERROR records in the recent log tail.",
			typ:   "counter",
			lines: lines,
		})
	}

	var b strings.Builder
	for _, m := range ms {
		fmt.Fprintf(&b, "# HELP %s %s\n", m.name, m.help)
		fmt.Fprintf(&b, "# TYPE %s %s\n", m.name, m.typ)
		for _, ln := range m.lines {
			fmt.Fprintln(&b, ln)
		}
	}
	_, _ = w.Write([]byte(b.String()))
}

// ---------- card builders ----------

func (s *server) buildSummary(ctx context.Context) summary {
	out := summary{
		GeneratedAt: time.Now().UTC(),
		DataDir:     s.opts.DataDir,
	}
	out.Jetstream = s.jetstreamSummary()
	out.LastBuild = s.buildSummaryCard(ctx)
	out.TodayShard = s.todaySummary(ctx)
	out.Retention = s.retentionSummary()
	out.Disk = s.diskSummary()
	out.ObjectStore = s.objstoreSummary(ctx)

	// errors: default last 10 minutes for the dashboard.
	errs := summarizeErrors(s.combinedLogTail(), 10*time.Minute, 5)
	out.Errors = &errs
	return out
}

func (s *server) jetstreamSummary() *jetstreamCard {
	c, ok, err := readCursor(s.opts.DataDir)
	if err != nil {
		s.logger.Debug("read cursor", "err", err)
	}
	if !ok {
		return nil
	}
	card := &jetstreamCard{
		Cursor:    c.Cursor,
		Endpoint:  c.Endpoint,
		UpdatedAt: c.UpdatedAt,
	}
	if c.Cursor > 0 {
		// cursor is microseconds since epoch.
		curT := time.Unix(0, c.Cursor*int64(time.Microsecond))
		card.LagSeconds = time.Since(curT).Seconds()
	}
	// Tail run.log only — build.log won't have firehose events.
	lines, err := tailFile(filepath.Join(s.opts.DataDir, "logs", "run.log"), s.opts.LogTailLines)
	if err != nil {
		s.logger.Debug("tail run.log", "err", err)
	}
	rate, byKind := eventsPerSec(lines)
	card.EventsPerSec = rate
	card.ByKind = byKind
	// Last flush: scan tail for the most recent record with msg "flush" or
	// containing "rollover".
	for i := len(lines) - 1; i >= 0; i-- {
		rec, ok := parseLogLine(lines[i])
		if !ok {
			continue
		}
		m := strings.ToLower(rec.Msg)
		if strings.Contains(m, "flush") || strings.Contains(m, "rollover") || strings.Contains(m, "checkpoint") {
			if !rec.Time.IsZero() {
				card.LastFlush = rec.Time.Format(time.RFC3339)
			}
			break
		}
	}
	return card
}

func (s *server) buildSummaryCard(ctx context.Context) *buildCard {
	card := &buildCard{Source: "latest.json", RowCounts: map[string]any{}}
	latest, ok, err := readLatest(s.opts.DataDir)
	if err != nil {
		s.logger.Debug("read latest.json", "err", err)
	}
	if ok {
		if t, ok := latest["built_at"].(string); ok {
			if parsed, err := time.Parse(time.RFC3339, t); err == nil {
				card.BuiltAt = parsed
			}
		}
		if m, ok := latest["build_mode"].(string); ok {
			card.Mode = m
		}
		if d, ok := latest["build_duration_seconds"].(float64); ok {
			card.DurationSeconds = d
		}
		if sv, ok := latest["schema_version"].(string); ok {
			card.SchemaVersion = sv
		}
		if rc, ok := latest["row_counts"].(map[string]any); ok {
			card.RowCounts = rc
		}
		if c, ok := latest["jetstream_cursor"].(float64); ok {
			card.JetstreamCursor = int64(c)
		}
		return card
	}

	// Fall back to _meta from current_graph.duckdb.
	meta, ok, err := graphMeta(ctx, s.opts.DataDir)
	if err != nil {
		s.logger.Debug("graph meta", "err", err)
	}
	if !ok {
		return nil
	}
	card.Source = "_meta"
	card.BuiltAt = meta.BuiltAt
	card.Mode = meta.BuildMode
	card.SchemaVersion = meta.SchemaVersion
	card.JetstreamCursor = meta.JetstreamCursor
	for k, v := range meta.RowCounts {
		card.RowCounts[k] = v
	}
	return card
}

func (s *server) todaySummary(ctx context.Context) *todayCard {
	today := time.Now().UTC().Format("2006-01-02")
	card := &todayCard{Date: today, ByCollection: map[string]int64{}}

	if info, ok, err := todayShard(ctx, s.opts.DataDir); err != nil {
		s.logger.Debug("today shard", "err", err)
	} else if ok {
		card.Events = info.Events
		card.ByCollection = info.ByCollection
	}

	// Bytes: sum any in-progress staging files for today. Best-effort: we
	// look at staging.db size as a proxy.
	stagingPath := filepath.Join(s.opts.DataDir, "staging.db")
	if info, err := osStatSize(stagingPath); err == nil {
		card.Bytes = info
	}

	// Estimated seal: midnight UTC + RolloverGrace (10m default).
	now := time.Now().UTC()
	tomorrow := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 10, 0, 0, time.UTC)
	card.EstSealUTC = tomorrow.Format(time.RFC3339)
	return card
}

func (s *server) retentionSummary() *retentionCard {
	dirs, err := listDailyDirs(s.opts.DataDir)
	if err != nil {
		s.logger.Debug("list daily", "err", err)
	}
	card := &retentionCard{Entries: dirs}
	if len(dirs) == 0 {
		return card
	}
	card.Days = len(dirs)
	for _, d := range dirs {
		card.TotalBytes += d.Bytes
	}
	card.OldestDate = dirs[0].Date
	card.NewestDate = dirs[len(dirs)-1].Date
	// Which one drops next: the oldest. Build does the actual pruning,
	// but the dashboard just surfaces the candidate.
	card.NextDrops = dirs[0].Date
	return card
}

func (s *server) diskSummary() *diskCard {
	d, err := statfs(s.opts.DataDir)
	if err != nil {
		s.logger.Debug("statfs", "err", err)
		return nil
	}
	card := &diskCard{
		Mount:      d.Mount,
		TotalBytes: d.TotalBytes,
		FreeBytes:  d.FreeBytes,
		UsedBytes:  d.UsedBytes,
	}
	card.DataDirBytes, _ = dirSize(s.opts.DataDir)
	card.LargestFile, card.LargestBytes = largestFile(s.opts.DataDir)
	return card
}

func (s *server) objstoreSummary(ctx context.Context) *objstoreCard {
	card := &objstoreCard{}
	if s.opts.Store == nil {
		return card
	}
	card.Available = true
	if items, err := s.opts.Store.List(ctx, "daily/"); err == nil {
		card.DailyCount = countDateDirs(items, "daily/")
	} else {
		s.logger.Debug("list daily/ object store", "err", err)
	}
	if items, err := s.opts.Store.List(ctx, "bootstrap/"); err == nil {
		card.BootstrapCount = countDateDirs(items, "bootstrap/")
		card.LatestBootstrap = latestDateDir(items, "bootstrap/")
	} else {
		s.logger.Debug("list bootstrap/ object store", "err", err)
	}
	return card
}

func countDateDirs(items []objstore.ObjectInfo, prefix string) int {
	seen := map[string]struct{}{}
	for _, it := range items {
		k := strings.TrimPrefix(it.Key, prefix)
		// take first path component
		if i := strings.IndexByte(k, '/'); i >= 0 {
			k = k[:i]
		}
		if _, err := time.Parse("2006-01-02", k); err != nil {
			continue
		}
		seen[k] = struct{}{}
	}
	return len(seen)
}

func latestDateDir(items []objstore.ObjectInfo, prefix string) string {
	seen := map[string]struct{}{}
	for _, it := range items {
		k := strings.TrimPrefix(it.Key, prefix)
		if i := strings.IndexByte(k, '/'); i >= 0 {
			k = k[:i]
		}
		if _, err := time.Parse("2006-01-02", k); err != nil {
			continue
		}
		seen[k] = struct{}{}
	}
	out := ""
	for k := range seen {
		if k > out {
			out = k
		}
	}
	return out
}

// combinedLogTail tails run.log and build.log and returns the lines in
// approximate chronological order (concatenated as-is — slog's ordered
// emission per file is preserved, but we don't re-sort across files).
func (s *server) combinedLogTail() []string {
	logsDir := filepath.Join(s.opts.DataDir, "logs")
	var out []string
	for _, name := range []string{"run.log", "build.log"} {
		lines, err := tailFile(filepath.Join(logsDir, name), s.opts.LogTailLines)
		if err != nil {
			s.logger.Debug("tail log", "file", name, "err", err)
		}
		out = append(out, lines...)
	}
	return out
}

// ---------- helpers ----------

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func numericish(v any) (int64, bool) {
	switch t := v.(type) {
	case int64:
		return t, true
	case int:
		return int64(t), true
	case float64:
		return int64(t), true
	case string:
		n, err := strconv.ParseInt(t, 10, 64)
		return n, err == nil
	}
	return 0, false
}

func osStatSize(path string) (int64, error) {
	st, err := osStatFn(path)
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}
