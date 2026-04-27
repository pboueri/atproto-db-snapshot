package monitor

import (
	"context"
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/pboueri/atproto-db-snapshot/internal/config"
	"github.com/pboueri/atproto-db-snapshot/internal/objstore"
)

// homeData is the rendered template's view model. We pre-compute everything
// the template needs in Go so the template itself stays declarative.
type homeData struct {
	Now            time.Time
	RefreshSeconds int

	Config struct {
		ObjectStore     string
		Bucket          string
		S3Endpoint      string
		DataDir         string
		LookbackDays    int
		Languages       string
	}

	Bootstrap     BootstrapStatus
	BootstrapAge  string

	Run     RunStatus
	RunAge  string

	Snapshot     SnapshotStatus
	SnapshotAge  string

	// ObjStoreTree summarizes the prefixes the operator usually wants to
	// confirm at a glance (without listing every parquet shard by name).
	ObjStoreTree []objStoreEntry

	// RawDates lists the per-day breakdown of raw/ files.
	RawDates []rawDateRow
}

type objStoreEntry struct {
	Prefix     string
	FileCount  int
	TotalBytes int64
	Latest     time.Time
}

type rawDateRow struct {
	Date   string
	Counts map[string]int
	Total  int
}

// handleHome renders the dashboard at /. We register only "/{$}" so that
// unmatched paths still 404 cleanly through ServeMux's default handling
// instead of being swallowed by a generic root handler.
func handleHome(cfg config.Config, deps Deps) http.HandlerFunc {
	tmpl := template.Must(template.New("home").Funcs(template.FuncMap{
		"bytes":      humanBytes,
		"timeOrDash": timeOrDash,
		"orDash":     orDash,
		"join":       strings.Join,
	}).Parse(homeTemplate))

	return func(w http.ResponseWriter, r *http.Request) {
		data := buildHomeData(r.Context(), cfg, deps)
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := tmpl.Execute(w, data); err != nil {
			slog.Warn("monitor: render home", "err", err)
		}
	}
}

func buildHomeData(ctx context.Context, cfg config.Config, deps Deps) homeData {
	d := homeData{
		Now:            time.Now().UTC(),
		RefreshSeconds: 10,
	}
	d.Config.ObjectStore = cfg.ObjectStore
	d.Config.Bucket = cfg.ObjectStoreRoot
	d.Config.S3Endpoint = cfg.S3Endpoint
	d.Config.DataDir = cfg.DataDir
	d.Config.LookbackDays = cfg.LookbackDays
	d.Config.Languages = strings.Join(cfg.Languages, ",")

	d.Bootstrap = readBootstrap(ctx, cfg, deps)
	d.Run = readRun(ctx, cfg, deps)
	d.Snapshot = readSnapshot(ctx, cfg, deps)

	if d.Bootstrap.StartedAt != nil {
		d.BootstrapAge = humanAge(d.Now.Sub(*d.Bootstrap.StartedAt))
	}
	if d.Run.CursorAt != nil {
		d.RunAge = humanAge(d.Now.Sub(*d.Run.CursorAt))
	}
	if d.Snapshot.SnapshotAt != nil {
		d.SnapshotAge = humanAge(d.Now.Sub(*d.Snapshot.SnapshotAt))
	}

	d.ObjStoreTree = summarizeTree(ctx, deps.ObjStore)
	d.RawDates = summarizeRaw(ctx, deps.ObjStore)

	return d
}

// summarizeTree groups objstore listings under the four canonical prefixes
// the operator cares about, with file counts and most-recent mtimes.
func summarizeTree(ctx context.Context, obj objstore.Store) []objStoreEntry {
	prefixes := []string{"bootstrap/", "raw/", "snapshot/", "run/"}
	out := make([]objStoreEntry, 0, len(prefixes))
	for _, p := range prefixes {
		entry := objStoreEntry{Prefix: p}
		objs, err := obj.List(ctx, p)
		if err != nil {
			slog.Debug("monitor: home list", "prefix", p, "err", err)
			out = append(out, entry)
			continue
		}
		for _, o := range objs {
			entry.FileCount++
			entry.TotalBytes += o.Size
			if o.LastModified.After(entry.Latest) {
				entry.Latest = o.LastModified
			}
		}
		out = append(out, entry)
	}
	return out
}

// summarizeRaw breaks raw/ down by date and collection so the dashboard can
// show a "what days do we have" table at a glance. The collection name is
// extracted from the parquet filename via the rawio convention
// {collection}-{ts}-{seq}.parquet.
func summarizeRaw(ctx context.Context, obj objstore.Store) []rawDateRow {
	objs, err := obj.List(ctx, "raw/")
	if err != nil {
		slog.Debug("monitor: home raw list", "err", err)
		return nil
	}
	byDate := map[string]*rawDateRow{}
	for _, o := range objs {
		// Path: raw/YYYY-MM-DD/{collection}-{ts}-{seq}.parquet
		parts := strings.Split(o.Path, "/")
		if len(parts) != 3 {
			continue
		}
		date := parts[1]
		base := parts[2]
		// Defensively skip files that don't match the rawio shape.
		if !strings.HasSuffix(base, ".parquet") {
			continue
		}
		col := strings.SplitN(base, "-", 2)[0]
		row, ok := byDate[date]
		if !ok {
			row = &rawDateRow{Date: date, Counts: map[string]int{}}
			byDate[date] = row
		}
		row.Counts[col]++
		row.Total++
	}
	dates := make([]string, 0, len(byDate))
	for d := range byDate {
		dates = append(dates, d)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(dates)))
	out := make([]rawDateRow, 0, len(dates))
	for _, d := range dates {
		out = append(out, *byDate[d])
	}
	return out
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func humanAge(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd ago", int(d/(24*time.Hour)))
	}
}

func timeOrDash(t *time.Time) string {
	if t == nil || t.IsZero() {
		return "—"
	}
	return t.UTC().Format("2006-01-02 15:04:05 UTC")
}

func orDash(s any) string {
	switch v := s.(type) {
	case *string:
		if v == nil || *v == "" {
			return "—"
		}
		return *v
	case string:
		if v == "" {
			return "—"
		}
		return v
	default:
		return fmt.Sprintf("%v", v)
	}
}

// homeTemplate is intentionally inline rather than embedded from a separate
// .html file: it keeps deployment a single binary and makes future template
// edits diff-able alongside the Go that feeds it.
const homeTemplate = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>at-snapshot status</title>
<meta http-equiv="refresh" content="{{.RefreshSeconds}}">
<style>
:root { color-scheme: light dark; }
body {
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  max-width: 76em; margin: 2em auto; padding: 0 1em; line-height: 1.5;
  font-size: 14px;
}
h1, h2 { font-family: -apple-system, BlinkMacSystemFont, system-ui, sans-serif; font-weight: 600; }
h1 { font-size: 1.4em; margin-bottom: 0.2em; }
h2 { font-size: 1.05em; margin-top: 1.8em; padding-bottom: 0.3em;
     border-bottom: 1px solid color-mix(in srgb, currentColor 20%, transparent); }
.muted { color: color-mix(in srgb, currentColor 55%, transparent); }
.ok    { color: #1f8b4c; }
.warn  { color: #c08400; }
.fail  { color: #c83a30; }
table { border-collapse: collapse; margin: 0.5em 0; width: 100%; }
th, td { padding: 0.35em 0.8em; text-align: left;
         border-bottom: 1px solid color-mix(in srgb, currentColor 12%, transparent); }
th { font-weight: 600; }
td.num { text-align: right; font-variant-numeric: tabular-nums; }
.code { font-family: inherit; background: color-mix(in srgb, currentColor 8%, transparent);
        padding: 0.1em 0.4em; border-radius: 3px; word-break: break-all; }
.section-error { background: color-mix(in srgb, #c83a30 12%, transparent); padding: 0.5em 0.8em; border-radius: 4px; }
.kv { display: grid; grid-template-columns: 12em 1fr; gap: 0.2em 1em; margin: 0.3em 0 1em; }
.kv dt { color: color-mix(in srgb, currentColor 55%, transparent); }
.kv dd { margin: 0; }
.legend { font-size: 0.85em; }
@media (prefers-color-scheme: dark) {
  body { background: #0d1117; color: #e6edf3; }
}
</style>
</head>
<body>

<h1>at-snapshot</h1>
<p class="muted">refreshes every {{.RefreshSeconds}}s · {{.Now.Format "2006-01-02 15:04:05 UTC"}}</p>

<dl class="kv legend">
  <dt>object store</dt> <dd><span class="code">{{.Config.ObjectStore}}://{{.Config.Bucket}}</span></dd>
  {{if .Config.S3Endpoint}}<dt>endpoint</dt> <dd><span class="code">{{.Config.S3Endpoint}}</span></dd>{{end}}
  <dt>data dir</dt> <dd><span class="code">{{.Config.DataDir}}</span></dd>
  <dt>lookback</dt> <dd>{{.Config.LookbackDays}} days</dd>
  <dt>languages</dt> <dd>{{orDash .Config.Languages}}</dd>
</dl>

<h2>bootstrap {{if .BootstrapAge}}<span class="muted">started {{.BootstrapAge}}</span>{{end}}</h2>
{{if .Bootstrap.Error}}<p class="section-error fail">error: {{.Bootstrap.Error}}</p>{{end}}
{{if .Bootstrap.Found}}
<table>
  <tr><th>started_at</th>     <td>{{timeOrDash .Bootstrap.StartedAt}}</td></tr>
  <tr><th>completed_at</th>   <td>{{timeOrDash .Bootstrap.CompletedAt}} {{if not .Bootstrap.CompletedAt}}<span class="warn">running…</span>{{else}}<span class="ok">done</span>{{end}}</td></tr>
  <tr><th>completed_dids</th> <td class="num">{{.Bootstrap.CompletedDIDs}}</td></tr>
  <tr><th>actors</th>         <td class="num">{{.Bootstrap.Actors}}</td></tr>
  <tr><th>follows</th>        <td class="num">{{.Bootstrap.Follows}}</td></tr>
  <tr><th>blocks</th>         <td class="num">{{.Bootstrap.Blocks}}</td></tr>
  <tr><th>remote baseline</th><td><span class="code">{{orDash .Bootstrap.RemoteBaseline}}</span></td></tr>
</table>
{{else}}
<p class="muted">no bootstrap data yet — run <code>at-snapshot bootstrap</code></p>
{{end}}

<h2>run {{if .RunAge}}<span class="muted">cursor {{.RunAge}}</span>{{end}}</h2>
{{if .Run.Error}}<p class="section-error fail">error: {{.Run.Error}}</p>{{end}}
{{if .Run.CursorMicros}}
<table>
  <tr><th>cursor_at</th>          <td>{{timeOrDash .Run.CursorAt}}</td></tr>
  <tr><th>cursor_micros</th>      <td class="num">{{.Run.CursorMicros}}</td></tr>
  <tr><th>raw files (total)</th>  <td class="num">{{.Run.RawFilesTotal}}</td></tr>
  <tr><th>raw dates</th>          <td>{{join .Run.RawDates ", "}}</td></tr>
  {{range $k, $v := .Run.RawFilesByCollection}}<tr><th>raw / {{$k}}</th><td class="num">{{$v}}</td></tr>{{end}}
</table>
{{else}}
<p class="muted">no run cursor yet — run <code>at-snapshot run</code></p>
{{end}}

<h2>snapshot {{if .SnapshotAge}}<span class="muted">last {{.SnapshotAge}}</span>{{end}}</h2>
{{if .Snapshot.Error}}<p class="section-error fail">error: {{.Snapshot.Error}}</p>{{end}}
{{if .Snapshot.Found}}
<table>
  <tr><th>snapshot_at</th>           <td>{{timeOrDash .Snapshot.SnapshotAt}}</td></tr>
  <tr><th>lookback_days</th>         <td class="num">{{.Snapshot.LookbackDays}}</td></tr>
  <tr><th>current_all.duckdb</th>    <td>{{bytes .Snapshot.CurrentAllSizeBytes}}</td></tr>
  <tr><th>current_graph.duckdb</th>  <td>{{bytes .Snapshot.CurrentGraphSizeBytes}}</td></tr>
  {{range $k, $v := .Snapshot.RowCounts}}<tr><th>{{$k}}</th><td class="num">{{$v}}</td></tr>{{end}}
</table>
{{else}}
<p class="muted">no snapshot yet — run <code>at-snapshot snapshot</code></p>
{{end}}

<h2>object store</h2>
<table>
  <tr><th>prefix</th><th class="num">files</th><th class="num">size</th><th>latest</th></tr>
  {{range .ObjStoreTree}}
  <tr>
    <td><span class="code">{{.Prefix}}</span></td>
    <td class="num">{{.FileCount}}</td>
    <td class="num">{{bytes .TotalBytes}}</td>
    <td>{{if .Latest.IsZero}}—{{else}}{{.Latest.UTC.Format "2006-01-02 15:04:05 UTC"}}{{end}}</td>
  </tr>
  {{end}}
</table>

{{if .RawDates}}
<h2>raw/ by date</h2>
<table>
  <tr>
    <th>date</th><th class="num">posts</th><th class="num">post_media</th>
    <th class="num">likes</th><th class="num">reposts</th>
    <th class="num">follows</th><th class="num">blocks</th><th class="num">profiles</th>
    <th class="num">total</th>
  </tr>
  {{range .RawDates}}
  <tr>
    <td>{{.Date}}</td>
    <td class="num">{{index .Counts "posts"}}</td>
    <td class="num">{{index .Counts "post_media"}}</td>
    <td class="num">{{index .Counts "likes"}}</td>
    <td class="num">{{index .Counts "reposts"}}</td>
    <td class="num">{{index .Counts "follows"}}</td>
    <td class="num">{{index .Counts "blocks"}}</td>
    <td class="num">{{index .Counts "profiles"}}</td>
    <td class="num">{{.Total}}</td>
  </tr>
  {{end}}
</table>
{{end}}

<p class="muted">raw JSON at <a href="/status">/status</a>, <a href="/healthz">/healthz</a>.</p>

</body>
</html>
`

// pathEscape avoids importing net/url just to escape one filename in templates.
// Currently unused; kept here so a future "click to download" link doesn't
// require a re-import.
var _ = filepath.Base
