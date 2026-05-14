"""Shared chrome + helpers for the snapshot analyses.

Every analysis module (`likes.py`, `ratio.py`, `attrition.py`, `blocks.py`)
imports from here so colors, layout, plotly template, and HTML scaffolding
stay in lockstep. The Modal app (`modal_app.py`) and the test harness both
call these analyses through the module-level `run(con, ...)` entrypoint.
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone

# Mount point for the Modal output volume. Modules don't read this
# directly (the Modal wrapper opens the DB and hands a connection to
# `run`), but the dispatcher uses it when computing on-volume paths.
OUT_VOL_DIR = "/vol-out/var"

BRAND = "#0085ff"
AXIS = "#1d2433"
GRID = "#e6e8ec"

SHARED_CSS = f"""
:root {{
  --brand: {BRAND};
  --ink: #1d2433;
  --muted: #5b6472;
  --rule: #e6e8ec;
  --bg: #fbfbfd;
}}
* {{ box-sizing: border-box; }}
html, body {{ margin: 0; padding: 0; background: var(--bg); }}
body {{
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "Helvetica Neue",
               Arial, sans-serif;
  color: var(--ink); line-height: 1.55; font-size: 16px;
}}
.wrap {{ max-width: 1080px; margin: 0 auto; padding: 56px 24px 80px; }}
.eyebrow {{
  font-size: 12px; letter-spacing: 0.12em; text-transform: uppercase;
  color: var(--muted); margin-bottom: 12px;
}}
h1 {{
  font-size: 44px; line-height: 1.1; letter-spacing: -0.02em;
  margin: 0 0 16px; font-weight: 700;
}}
h1 .accent {{ color: var(--brand); }}
.lede {{ font-size: 19px; color: var(--muted); margin: 0 0 36px; max-width: 780px; }}
.stats {{
  display: grid; grid-template-columns: repeat(4, 1fr); gap: 14px;
  margin: 32px 0 48px;
}}
.stat {{
  background: white; border: 1px solid var(--rule); border-radius: 10px;
  padding: 18px 16px;
}}
.stat .v {{ font-size: 30px; font-weight: 700; letter-spacing: -0.02em; }}
.stat .v.bad {{ color: #ef4444; }}
.stat .v.brand {{ color: var(--brand); }}
.stat .l {{ font-size: 12.5px; color: var(--muted); margin-top: 4px; }}
.stat .sub {{ font-size: 11.5px; color: var(--muted); margin-top: 2px; }}
section {{ margin: 56px 0; }}
section h2 {{
  font-size: 26px; letter-spacing: -0.01em; font-weight: 700;
  margin: 0 0 8px;
}}
section .kicker {{
  font-size: 14px; color: var(--brand); font-weight: 600;
  text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 8px;
}}
section p {{ color: var(--muted); margin: 0 0 16px; max-width: 820px; }}
section p strong {{ color: var(--ink); }}
.figure {{
  background: white; border: 1px solid var(--rule); border-radius: 10px;
  padding: 14px 10px 6px; margin-top: 18px;
}}
.pull {{
  border-left: 3px solid var(--brand); padding: 6px 0 6px 16px;
  margin: 24px 0; font-size: 21px; line-height: 1.4; color: var(--ink);
  font-weight: 500; max-width: 780px;
}}
footer {{
  margin-top: 80px; padding-top: 24px; border-top: 1px solid var(--rule);
  color: var(--muted); font-size: 13px;
}}
footer code {{
  background: white; border: 1px solid var(--rule); border-radius: 4px;
  padding: 1px 5px; font-size: 12px;
}}
@media (max-width: 720px) {{
  .stats {{ grid-template-columns: repeat(2, 1fr); }}
  h1 {{ font-size: 34px; }}
  .lede {{ font-size: 17px; }}
}}
"""


def bsky_template():
    """Plotly layout template shared across analyses for visual consistency."""
    import plotly.graph_objects as go
    return go.layout.Template(
        layout=go.Layout(
            font=dict(family="-apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
                      color=AXIS, size=13),
            paper_bgcolor="white",
            plot_bgcolor="white",
            colorway=[BRAND, "#ff5d8f", "#7c3aed", "#16a34a", "#f59e0b"],
            xaxis=dict(gridcolor=GRID, zeroline=False, linecolor=GRID),
            yaxis=dict(gridcolor=GRID, zeroline=False, linecolor=GRID),
            margin=dict(l=60, r=20, t=50, b=60),
            hoverlabel=dict(bgcolor="white", bordercolor=GRID),
        )
    )


def fig_html(fig, div_id: str) -> str:
    import plotly.io as pio
    return pio.to_html(
        fig, include_plotlyjs=False, full_html=False,
        div_id=div_id, config={"displayModeBar": False, "responsive": True},
    )


def plotlyjs_inline() -> str:
    """The bundled plotly.js as a plain string for inline <script> injection."""
    import plotly.offline as _po
    return _po.get_plotlyjs()


def install_template():
    """Register the bsky plotly template as the default for this process."""
    import plotly.io as pio
    pio.templates["bsky"] = bsky_template()


def fmt_int(n) -> str:
    return f"{int(n):,}"


def built_at_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def timed_query(con, sql: str, params: list | None = None, *, log: bool = True):
    """Run a query and log its elapsed time + first line. Returns rows.

    Used by every analysis so the Modal logs read uniformly. `log=False`
    in tests keeps pytest output clean.
    """
    t0 = time.time()
    rows = con.execute(sql, params or []).fetchall()
    if log:
        dt = time.time() - t0
        head = sql.strip().splitlines()[0][:70]
        print(f"  ({dt:5.1f}s) {head}…", flush=True)
    return rows


def persist_artifact(out_dir: str, basename: str, html: bytes, sidecar: dict) -> str:
    """Write `<out_dir>/<basename>.html` and `.json`. Returns the html path.

    Used by the Modal wrappers; tests don't go through this.
    """
    import json
    os.makedirs(out_dir, exist_ok=True)
    html_path = f"{out_dir}/{basename}.html"
    json_path = f"{out_dir}/{basename}.json"
    with open(html_path, "wb") as f:
        f.write(html)
    with open(json_path, "w") as f:
        json.dump(sidecar, f, indent=2, default=str)
    return html_path
