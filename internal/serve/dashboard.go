package serve

// dashboardHTML is the single-page template for `GET /`. Inline CSS, no
// JS frameworks; the only client-side script is a setInterval poll on
// /api/summary that swaps the page on success.
//
// Per spec §10, this should stay well under 50 KB.
const dashboardHTML = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>at-snapshotter — health</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
  :root {
    --bg: #0d1117;
    --fg: #c9d1d9;
    --muted: #8b949e;
    --accent: #58a6ff;
    --good: #3fb950;
    --warn: #d29922;
    --bad: #f85149;
    --border: #30363d;
    --card-bg: #161b22;
  }
  html, body {
    background: var(--bg);
    color: var(--fg);
    font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
    font-size: 13px;
    margin: 0;
    padding: 0;
    line-height: 1.4;
  }
  header {
    padding: 12px 18px;
    border-bottom: 1px solid var(--border);
    display: flex;
    justify-content: space-between;
    align-items: center;
  }
  header h1 {
    margin: 0;
    font-size: 14px;
    font-weight: 600;
    letter-spacing: 0.5px;
  }
  header .meta {
    color: var(--muted);
    font-size: 12px;
  }
  main {
    padding: 16px;
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 14px;
    max-width: 1400px;
    margin: 0 auto;
  }
  @media (max-width: 760px) {
    main { grid-template-columns: 1fr; }
  }
  .card {
    border: 1px solid var(--border);
    background: var(--card-bg);
    border-radius: 6px;
    padding: 10px 14px;
  }
  .card h2 {
    margin: 0 0 8px 0;
    font-size: 12px;
    font-weight: 600;
    letter-spacing: 0.6px;
    text-transform: uppercase;
    color: var(--accent);
    border-bottom: 1px solid var(--border);
    padding-bottom: 4px;
  }
  .row {
    display: flex;
    justify-content: space-between;
    gap: 12px;
    padding: 2px 0;
  }
  .row .k { color: var(--muted); }
  .row .v { color: var(--fg); text-align: right; word-break: break-all; }
  .ok   { color: var(--good); }
  .warn { color: var(--warn); }
  .bad  { color: var(--bad); }
  .samples {
    margin-top: 6px;
    font-size: 12px;
    color: var(--muted);
    border-top: 1px solid var(--border);
    padding-top: 6px;
  }
  .samples li { list-style: none; padding: 2px 0; }
  .samples .lvl { display: inline-block; min-width: 50px; }
  .footer {
    padding: 10px 18px;
    color: var(--muted);
    font-size: 11px;
    border-top: 1px solid var(--border);
  }
</style>
</head>
<body>
<header>
  <h1>at-snapshotter health</h1>
  <span class="meta" id="generated">{{ timeUTC .Summary.GeneratedAt }} · {{ .Summary.DataDir }}</span>
</header>

<main id="cards">
  {{ template "cards" .Summary }}
</main>

<div class="footer">
  refresh every {{ .RefreshSeconds }}s ·
  <a href="/api/summary" style="color:var(--accent)">/api/summary</a> ·
  <a href="/metrics" style="color:var(--accent)">/metrics</a>
</div>

<script>
(function () {
  const REFRESH_MS = {{ .RefreshSeconds }} * 1000;
  async function tick() {
    try {
      const r = await fetch('/api/summary', { cache: 'no-store' });
      if (!r.ok) return;
      const data = await r.json();
      // Cheap update: just re-fetch the page HTML for the cards. We render
      // the body server-side to keep the bundle tiny.
      const html = await (await fetch('/?_=' + Date.now())).text();
      const m = html.match(/<main id="cards">([\s\S]*?)<\/main>/);
      if (m) {
        document.getElementById('cards').innerHTML = m[1];
      }
      const gm = html.match(/<span class="meta" id="generated">([\s\S]*?)<\/span>/);
      if (gm) {
        document.getElementById('generated').innerHTML = gm[1];
      }
    } catch (e) { /* swallow; next tick will retry */ }
  }
  setInterval(tick, REFRESH_MS);
})();
</script>
</body>
</html>

{{ define "cards" }}

<section class="card">
  <h2>Firehose</h2>
  {{ with .Jetstream }}
    <div class="row"><span class="k">cursor</span><span class="v">{{ if .Cursor }}{{ humanInt .Cursor }}{{ else }}—{{ end }}</span></div>
    <div class="row"><span class="k">endpoint</span><span class="v">{{ orDash .Endpoint }}</span></div>
    <div class="row"><span class="k">lag</span><span class="v">{{ humanLag .LagSeconds }} (behind now)</span></div>
    <div class="row"><span class="k">events/sec</span><span class="v">{{ humanRate .EventsPerSec }} (1-min rolling)</span></div>
    {{ if .ByKind }}
      <div class="row"><span class="k">by kind</span><span class="v">
        {{ range $k := sortedKeysInt .ByKind }}{{ $k }}={{ index $.ByKind $k }}  {{ end }}
      </span></div>
    {{ end }}
    <div class="row"><span class="k">last flush</span><span class="v">{{ orDash .LastFlush }}</span></div>
  {{ else }}
    <div class="row"><span class="k">cursor</span><span class="v">—</span></div>
    <div class="row"><span class="k">lag</span><span class="v">—</span></div>
    <div class="row"><span class="k">events/sec</span><span class="v">—</span></div>
  {{ end }}
</section>

<section class="card">
  <h2>Last build</h2>
  {{ with .LastBuild }}
    <div class="row"><span class="k">built_at</span><span class="v">{{ timeUTC .BuiltAt }}</span></div>
    <div class="row"><span class="k">mode</span><span class="v">{{ orDash .Mode }}</span></div>
    <div class="row"><span class="k">duration</span><span class="v">{{ humanDuration .DurationSeconds }}</span></div>
    <div class="row"><span class="k">schema</span><span class="v">{{ orDash .SchemaVersion }}</span></div>
    <div class="row"><span class="k">source</span><span class="v">{{ orDash .Source }}</span></div>
    {{ if .RowCounts }}
      {{ range $k := sortedKeysAny .RowCounts }}
        <div class="row"><span class="k">{{ $k }}</span><span class="v">{{ orDash (index $.RowCounts $k) }}</span></div>
      {{ end }}
    {{ end }}
  {{ else }}
    <div class="row"><span class="k">built_at</span><span class="v">—</span></div>
    <div class="row"><span class="k">mode</span><span class="v">—</span></div>
  {{ end }}
</section>

<section class="card">
  <h2>Today's shard</h2>
  {{ with .TodayShard }}
    <div class="row"><span class="k">date</span><span class="v">{{ orDash .Date }}</span></div>
    <div class="row"><span class="k">staging.db</span><span class="v">{{ humanBytes .Bytes }}</span></div>
    <div class="row"><span class="k">events</span><span class="v">{{ humanInt .Events }}</span></div>
    <div class="row"><span class="k">est. seal</span><span class="v">{{ orDash .EstSealUTC }}</span></div>
    {{ range $k := sortedKeys .ByCollection }}
      <div class="row"><span class="k">{{ $k }}</span><span class="v">{{ humanInt (int64Map $.ByCollection $k) }}</span></div>
    {{ end }}
  {{ else }}
    <div class="row"><span class="k">date</span><span class="v">—</span></div>
  {{ end }}
</section>

<section class="card">
  <h2>Local retention</h2>
  {{ with .Retention }}
    <div class="row"><span class="k">days on disk</span><span class="v">{{ if .Days }}{{ .Days }}{{ else }}—{{ end }}</span></div>
    <div class="row"><span class="k">total</span><span class="v">{{ humanBytes .TotalBytes }}</span></div>
    <div class="row"><span class="k">range</span><span class="v">{{ orDash .OldestDate }} … {{ orDash .NewestDate }}</span></div>
    <div class="row"><span class="k">next drop</span><span class="v">{{ orDash .NextDrops }}</span></div>
  {{ else }}
    <div class="row"><span class="k">days on disk</span><span class="v">—</span></div>
  {{ end }}
</section>

<section class="card">
  <h2>Disk</h2>
  {{ with .Disk }}
    <div class="row"><span class="k">mount</span><span class="v">{{ orDash .Mount }}</span></div>
    <div class="row"><span class="k">free</span><span class="v">{{ humanBytes .FreeBytes }} / {{ humanBytes .TotalBytes }}</span></div>
    <div class="row"><span class="k">data dir</span><span class="v">{{ humanBytes .DataDirBytes }}</span></div>
    <div class="row"><span class="k">largest</span><span class="v">{{ orDash .LargestFile }} ({{ humanBytes .LargestBytes }})</span></div>
  {{ else }}
    <div class="row"><span class="k">mount</span><span class="v">—</span></div>
  {{ end }}
</section>

<section class="card">
  <h2>Object store</h2>
  {{ with .ObjectStore }}
    {{ if .Available }}
      <div class="row"><span class="k">daily/ count</span><span class="v">{{ .DailyCount }}</span></div>
      <div class="row"><span class="k">bootstrap/ count</span><span class="v">{{ .BootstrapCount }}</span></div>
      <div class="row"><span class="k">latest bootstrap</span><span class="v">{{ orDash .LatestBootstrap }}</span></div>
    {{ else }}
      <div class="row"><span class="k">status</span><span class="v">— (not wired)</span></div>
    {{ end }}
  {{ else }}
    <div class="row"><span class="k">status</span><span class="v">—</span></div>
  {{ end }}
</section>

<section class="card" style="grid-column: 1 / -1;">
  <h2>Errors (last 10 min)</h2>
  {{ with .Errors }}
    <div class="row"><span class="k">ERROR</span><span class="v {{ if .ErrorCount }}bad{{ else }}ok{{ end }}">{{ .ErrorCount }}</span></div>
    <div class="row"><span class="k">WARN</span><span class="v {{ if .WarnCount }}warn{{ else }}ok{{ end }}">{{ .WarnCount }}</span></div>
    {{ if .Samples }}
      <ul class="samples">
        {{ range .Samples }}
          <li><span class="lvl {{ if eq .Level "ERROR" }}bad{{ else }}warn{{ end }}">{{ .Level }}</span> <span>{{ timeUTC .Time }}</span> · {{ .Msg }}</li>
        {{ end }}
      </ul>
    {{ end }}
  {{ else }}
    <div class="row"><span class="k">status</span><span class="v">—</span></div>
  {{ end }}
</section>

{{ end }}
`
