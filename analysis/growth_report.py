"""Single-run Bluesky growth report: compute every aggregate on Modal in
one pass, render all post-ready visuals locally into ./bsky_growth_visuals/.

  modal run growth_report.py

Data sources (2026-05-11 snapshot, on the at-snapshot-output volume):
  - growth_state_log.parquet  (per-user state intervals; churn_days=30 run)
  - snapshot.duckdb           (actor_aggs + raw event tables)

Charts produced:
  01_layer_cake          cohort retention, ≤2025-Q1 grouped as the wave
  02_participation       confound-fixed churn (2026-Q1): never-created/void/validated
  03_activity_dist       activity distribution (posts/likes/follows)
  04_smile               habit distribution: active days in last 30
  05_first_like_cliff    creators' retention by likes received
  06_flow_attribution    weekly flow, churn split by tenure
  07_retention_metrics   new-user activation vs active-base churn over time
  08_projection_*        engaged-base projections (retention/churn/volume/optimistic)
"""
import datetime as dt
import json
import os

import modal

vol = modal.Volume.from_name("at-snapshot-output", create_if_missing=False)
image = modal.Image.debian_slim(python_version="3.12").pip_install("duckdb==1.5.2")
app = modal.App("growth-report", image=image)

DATE = "2026-05-11"
OUT = "/vol-out/var"
WEEK = 168
EPOCH = dt.datetime(1970, 1, 1)


def _h(d):  # datetime -> epoch hour
    return int((d - EPOCH).total_seconds() // 3600)


@app.function(volumes={"/vol-out": vol}, cpu=8.0, memory=96 * 1024,
              ephemeral_disk=512 * 1024, timeout=60 * 45)
def compute() -> dict:
    import duckdb
    slog = f"{OUT}/analysis/{DATE}/growth_state_log.parquet"
    con = duckdb.connect(f"{OUT}/snapshot/{DATE}/snapshot.duckdb", read_only=True)
    con.execute("PRAGMA threads=8")
    con.execute("SET memory_limit='80GiB'")
    os.makedirs("/tmp/dd", exist_ok=True)
    con.execute("SET temp_directory='/tmp/dd'")

    snap_h = _h(dt.datetime(2026, 5, 11, 23))
    baseline_h = _h(dt.datetime(2025, 1, 1))

    # ---- shared interval + per-user tables (one window pass) -------------
    con.execute(f"""
    CREATE TEMP TABLE iv AS
    SELECT did_id, hour_idx AS h, state,
           LEAD(hour_idx) OVER (PARTITION BY did_id ORDER BY hour_idx) AS h_next,
           LAG(state)     OVER (PARTITION BY did_id ORDER BY hour_idx) AS prev_state,
           MIN(hour_idx)  OVER (PARTITION BY did_id) AS first_h,
           FIRST(state)   OVER (PARTITION BY did_id ORDER BY hour_idx) AS fs
    FROM read_parquet('{slog}')
    """)
    con.execute("""
    CREATE TEMP TABLE u AS
    WITH last AS (
      SELECT did_id, state,
             ROW_NUMBER() OVER (PARTITION BY did_id ORDER BY h DESC) AS rn,
             MIN(h) OVER (PARTITION BY did_id) AS first_h,
             COUNT(*) OVER (PARTITION BY did_id) AS n_rec,
             FIRST(fs) OVER (PARTITION BY did_id ORDER BY h) AS fs
      FROM iv
    ),
    fin AS (SELECT did_id, state AS final_state, first_h, n_rec, fs FROM last WHERE rn=1)
    SELECT f.did_id, (f.final_state=4) AS churned, f.final_state, f.first_h, f.n_rec, f.fs,
           a.followers, a.follows, a.likes_in, a.likes_out, a.posts,
           a.reposts_in, a.reposts_out, a.replies_out, a.quotes_out,
           (a.posts + a.replies_out + a.quotes_out) AS created
    FROM fin f JOIN actor_aggs a USING (did_id)
    """)

    res = {"snapshot": DATE}

    # ---- weekly ticks ----------------------------------------------------
    ticks = []
    t = dt.datetime(2025, 1, 1)
    while t <= dt.datetime(2026, 5, 11):
        ticks.append(_h(t))
        t += dt.timedelta(weeks=1)
    con.execute("CREATE TEMP TABLE ticks(tick BIGINT)")
    con.executemany("INSERT INTO ticks VALUES (?)", [(x,) for x in ticks])

    def wk_date(w):
        return (EPOCH + dt.timedelta(hours=int(w) * WEEK)).date().isoformat()

    # ===== 01 LAYER CAKE: retained (non-churned) by 3-mo cohort ===========
    q1_start = _h(dt.datetime(2025, 4, 1))
    rows = con.execute(f"""
    WITH base AS (
      SELECT did_id, h, COALESCE(h_next, 1::BIGINT<<60) AS h_next,
             CASE WHEN first_h < {q1_start} THEN '≤ 2025-Q1'
                  ELSE strftime(TIMESTAMP '1970-01-01' + to_hours(first_h), '%Y') || '-Q'
                       || (((EXTRACT(month FROM TIMESTAMP '1970-01-01' + to_hours(first_h))::INT-1)//3)+1)::VARCHAR
             END AS cohort
      FROM iv WHERE state <> 4
    )
    SELECT t.tick, b.cohort, COUNT(*) FROM base b JOIN ticks t
      ON t.tick >= b.h AND t.tick < b.h_next
    GROUP BY 1,2
    """).fetchall()
    res["layer_cake"] = {"weeks": [wk_date(w // WEEK) for w in ticks],
                         "rows": [[wk_date(r[0] // WEEK), r[1], r[2]] for r in rows]}

    # ===== 04 SMILE: distinct active days in last 30 (raw events) =========
    hi = f"TIMESTAMP '{DATE} 23:59:59'"
    lo = f"({hi} - INTERVAL 30 DAY)"
    srcs = [("likes", "actor_did_id"), ("reposts", "actor_did_id"),
            ("follows", "src_did_id"), ("posts", "author_did_id")]
    union = "\n UNION ALL \n".join(
        f"SELECT {c} AS did_id, CAST(created_at AS DATE) d FROM {tb} "
        f"WHERE created_at > {lo} AND created_at <= {hi} AND created_at IS NOT NULL"
        for tb, c in srcs)
    con.execute(f"CREATE TEMP TABLE ud AS SELECT did_id, COUNT(DISTINCT d) ad "
                f"FROM ({union}) GROUP BY did_id")
    smile = con.execute("SELECT ad, COUNT(*) FROM ud GROUP BY ad ORDER BY ad").fetchall()
    n_mau = con.execute("SELECT COUNT(*) FROM ud").fetchone()[0]
    res["smile"] = {"n_mau": n_mau,
                    "median": con.execute("SELECT median(ad) FROM ud").fetchone()[0],
                    "mean": con.execute("SELECT avg(ad) FROM ud").fetchone()[0],
                    "hist": [[int(a), int(n)] for a, n in smile if 1 <= a <= 30]}

    # ===== 06 FLOW ATTRIBUTION: weekly, churn split by tenure =============
    flow = con.execute(f"""
    SELECT (h // {WEEK}) wk,
      SUM(CASE WHEN state=1 AND prev_state=0 THEN 1 ELSE 0 END) new_act,
      SUM(CASE WHEN state=1 AND prev_state=4 THEN 1 ELSE 0 END) resurrected,
      SUM(CASE WHEN state=4 AND (h-first_h) <  90*24 THEN 1 ELSE 0 END) churn_new,
      SUM(CASE WHEN state=4 AND (h-first_h) >= 90*24 THEN 1 ELSE 0 END) churn_active
    FROM iv GROUP BY wk ORDER BY wk
    """).fetchall()
    res["flow"] = [[wk_date(r[0]), r[1], r[2], r[3], r[4]] for r in flow
                   if r[0] * WEEK >= _h(dt.datetime(2025, 3, 1))]

    # ===== 07 RETENTION METRICS: activation@28d + active-base churn =======
    m1 = con.execute(f"""
    WITH ret AS (
      SELECT did_id, first_h,
        MAX(CASE WHEN (first_h+672) >= h AND (first_h+672) < COALESCE(h_next,1::BIGINT<<60)
                 THEN state END) s28
      FROM iv WHERE fs = 0 GROUP BY did_id, first_h
    )
    SELECT (first_h//{WEEK}) wk, COUNT(*) n,
           SUM(CASE WHEN s28 IN (1,2) THEN 1 ELSE 0 END) act
    FROM ret WHERE first_h+672 <= {snap_h} GROUP BY wk ORDER BY wk
    """).fetchall()
    base_by_tick = dict(con.execute("""
      SELECT t.tick, COUNT(*) FROM iv l JOIN ticks t
        ON t.tick >= l.h AND t.tick < COALESCE(l.h_next,1::BIGINT<<60)
      WHERE l.state <> 4 GROUP BY t.tick""").fetchall())
    churn_by_wk = dict(con.execute(f"SELECT (h//{WEEK}), COUNT(*) FROM iv WHERE state=4 GROUP BY 1").fetchall())
    clip = _h(dt.datetime(2025, 3, 1))
    res["metrics"] = {
        "activation": [[wk_date(w), 100.0 * a / n] for w, n, a in m1
                       if w * WEEK >= clip and n >= 500],
        "churn": [[wk_date(tk // WEEK), 100.0 * churn_by_wk.get(tk // WEEK, 0) / base_by_tick[tk]]
                  for tk in ticks if tk >= clip and base_by_tick.get(tk, 0) >= 500],
    }

    # ===== 08 PROJECTION ANCHORS ==========================================
    B0 = base_by_tick[max(t for t in ticks if t <= snap_h)]
    neww = dict(con.execute(f"SELECT (first_h//{WEEK}), COUNT(*) FROM "
                            f"(SELECT DISTINCT did_id, first_h, fs FROM iv WHERE fs=0) GROUP BY 1").fetchall())
    recent = sorted(base_by_tick)[-10:-2]
    allt = sorted(base_by_tick)
    cs, ns, infl = [], [], []
    for i, tk in enumerate(allt):
        if tk not in recent:
            continue
        wk = tk // WEEK
        b = base_by_tick[tk]
        cs.append(churn_by_wk.get(wk, 0) / b)
        ns.append(neww.get(wk, 0))
        if i + 1 < len(allt):
            infl.append((base_by_tick[allt[i + 1]] - b) + churn_by_wk.get(wk, 0))
    import statistics as st
    res["proj"] = {"B0": B0, "c": st.mean(cs), "N": st.mean(ns),
                   "R": st.mean(infl) - st.mean(ns)}

    # ===== 02/03/05 CHURN DIAGNOSTIC (2026-Q1 cohort) =====================
    lo_q = _h(dt.datetime(2026, 1, 1)); hi_q = _h(dt.datetime(2026, 4, 1))
    con.execute(f"CREATE TEMP TABLE coh AS SELECT * FROM u WHERE first_h>={lo_q} AND first_h<{hi_q}")
    N = con.execute("SELECT COUNT(*) FROM coh").fetchone()[0]
    churn_rate = con.execute("SELECT AVG(CASE WHEN churned THEN 1.0 ELSE 0 END) FROM coh").fetchone()[0]

    def seg(where):
        n, ret = con.execute(f"SELECT COUNT(*), AVG(CASE WHEN churned THEN 0.0 ELSE 1 END) "
                             f"FROM coh WHERE {where}").fetchone()
        return [n, round(100 * n / N, 1), round(100 * (ret or 0), 1)]

    def dist(col, buckets):
        out = []
        for lab, a, b in buckets:
            cond = f"{col}>={a}" + (f" AND {col}<{b}" if b is not None else "")
            n, ret = con.execute(f"SELECT COUNT(*), AVG(CASE WHEN churned THEN 0.0 ELSE 1 END) "
                                 f"FROM coh WHERE {cond}").fetchone()
            out.append([lab, n, round(100 * n / N, 1), round(100 * (ret or 0), 1)])
        return out

    B6 = [("0", 0, 1), ("1-4", 1, 5), ("5-9", 5, 10), ("10-49", 10, 50),
          ("50-199", 50, 200), ("200+", 200, None)]
    LB = [("0", 0, 1), ("1-9", 1, 10), ("10-49", 10, 50), ("50-199", 50, 200), ("200+", 200, None)]
    res["diag"] = {
        "cohort": "2026-Q1", "n": N, "churn_rate": round(100 * churn_rate, 1),
        "creation_split": {
            "never_created": seg("created = 0"),
            "void": seg("created >= 1 AND likes_in = 0"),
            "validated": seg("created >= 1 AND likes_in >= 1"),
        },
        "dist_posts": dist("posts", B6),
        "dist_likes_out": dist("likes_out", B6),
        "dist_follows": dist("follows", B6),
        "creators_by_likes": [
            [lab] + list(con.execute(
                f"SELECT COUNT(*), AVG(CASE WHEN churned THEN 0.0 ELSE 1 END) FROM coh "
                f"WHERE created>=1 AND likes_in>={a}" + (f" AND likes_in<{b}" if b else "")
            ).fetchone()) for lab, a, b in LB],
    }
    return res


# ===========================================================================
# Local rendering
# ===========================================================================
# Output folder: defaults to bsky_growth_visuals/ next to this script
# (i.e. analysis/bsky_growth_visuals/). Override with BSKY_VIZ_OUT.
FOLDER = os.environ.get(
    "BSKY_VIZ_OUT",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "bsky_growth_visuals"),
)
BRAND = "#0085ff"


def _save(fig, name):
    fig.write_html(f"{FOLDER}/{name}.html", include_plotlyjs="cdn")
    fig.write_image(f"{FOLDER}/{name}.png", width=1500, height=600, scale=2)


def render(data):
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    os.makedirs(FOLDER, exist_ok=True)

    # 01 layer cake
    lc = data["layer_cake"]
    weeks = lc["weeks"]
    cohorts = sorted({r[1] for r in lc["rows"]}, key=lambda c: (c != "≤ 2025-Q1", c))
    idx = {w: i for i, w in enumerate(weeks)}
    series = {c: [0] * len(weeks) for c in cohorts}
    for wkd, c, n in lc["rows"]:
        series[c][idx[wkd]] = n
    keep = [i for i, w in enumerate(weeks) if w >= "2025-01-01"]
    xw = [weeks[i] for i in keep]
    pal = ["#2563eb", "#16a34a", "#f59e0b", "#7c3aed", "#ef4444", "#0d9488", "#db2777", "#94a3b8"]
    fig = go.Figure()
    for i, c in enumerate(cohorts):
        fig.add_trace(go.Scatter(x=xw, y=[series[c][j] for j in keep], name=c, mode="lines",
                                 stackgroup="one", line=dict(width=0.5, color=pal[i % len(pal)])))
    fig.update_layout(title=dict(text="<b>Bluesky is living off one wave</b>  ·  retained users by acquisition cohort",
                                 x=0.02, xanchor="left"),
                      xaxis_title="Week", yaxis_title="Retained users (acted within 30d)",
                      template="plotly_white", legend=dict(title="Cohort", traceorder="reversed"),
                      hovermode="x unified")
    _save(fig, "01_layer_cake")

    # 02 participation
    cspl = data["diag"]["creation_split"]
    segs = ["never_created", "void", "validated"]
    labels = ["Never created<br>any content", "Created,<br>got 0 likes", "Created &<br>got likes"]
    y = [cspl[s][2] for s in segs]; share = [cspl[s][1] for s in segs]
    fig = go.Figure(go.Bar(x=labels, y=y, marker_color=["#9ca3af", "#f59e0b", "#16a34a"],
                           text=[f"{v}% stay<br>({s}% of cohort)" for v, s in zip(y, share)],
                           textposition="outside"))
    fig.update_layout(title=dict(text="<b>83% of new users never participate</b>  ·  2026-Q1 retention by what they did",
                                 x=0.02, xanchor="left"),
                      yaxis=dict(title="% retained", range=[0, max(y) * 1.25]),
                      template="plotly_white")
    _save(fig, "02_participation")

    # 03 activity distribution
    fig = go.Figure()
    for key, name, color in [("dist_posts", "Posts authored", "#2563eb"),
                             ("dist_likes_out", "Likes given", "#ef4444"),
                             ("dist_follows", "Accounts followed", "#16a34a")]:
        d = data["diag"][key]
        fig.add_trace(go.Bar(x=[r[0] for r in d], y=[r[2] for r in d], name=name, marker_color=color))
    fig.update_layout(barmode="group",
                      title=dict(text="<b>Most signups do almost nothing</b>  ·  2026-Q1 activity distribution",
                                 x=0.02, xanchor="left"),
                      xaxis_title="Count bucket", yaxis_title="% of cohort",
                      template="plotly_white", legend=dict(orientation="h", y=-0.18))
    _save(fig, "03_activity_dist")

    # 04 smile
    sm = data["smile"]
    h = {a: n for a, n in sm["hist"]}
    days = list(range(1, 31)); users = [h.get(i, 0) for i in days]

    def col(i):
        return ("#9ca3af" if i == 1 else "#cbd5e1" if i <= 3 else "#60a5fa" if i <= 9
                else "#2563eb" if i <= 19 else "#7c3aed" if i <= 27 else "#16a34a")
    fig = go.Figure(go.Bar(x=days, y=users, marker_color=[col(i) for i in days],
                           hovertemplate="%{x} active days<br>%{y:,} users<extra></extra>"))
    one = round(100 * h.get(1, 0) / sm["n_mau"], 0)
    daily = round(100 * sum(h.get(i, 0) for i in range(28, 31)) / sm["n_mau"], 0)
    fig.update_layout(title=dict(text=f"<b>The engagement smile</b>  ·  active days in last 30  ·  "
                                      f"{sm['n_mau']/1e6:.1f}M monthly-active (median {sm['median']:.0f})",
                                 x=0.02, xanchor="left"),
                      xaxis=dict(title="Active days in last 30", dtick=2), yaxis_title="Users",
                      template="plotly_white", showlegend=False,
                      annotations=[dict(x=1, y=h.get(1, 0), text=f"{one:.0f}% one-off", showarrow=True,
                                        arrowhead=2, ax=45, ay=-25, font=dict(color="#6b7280")),
                                   dict(x=30, y=h.get(30, 0), text=f"{daily:.0f}% near-daily core",
                                        showarrow=True, arrowhead=2, ax=-35, ay=-25, font=dict(color="#16a34a"))])
    _save(fig, "04_smile")

    # 05 first-like cliff (creators only)
    cl = data["diag"]["creators_by_likes"]
    fig = go.Figure(go.Scatter(x=[r[0] for r in cl], y=[round(100 * (r[2] or 0), 1) for r in cl],
                               mode="lines+markers", line=dict(color="#16a34a", width=3),
                               marker=dict(size=9)))
    fig.update_layout(title=dict(text="<b>For people who post, the first like is everything</b>  ·  "
                                      "2026-Q1 creators' retention by likes received",
                                 x=0.02, xanchor="left"),
                      xaxis_title="Likes received on their content", yaxis=dict(title="% retained", rangemode="tozero"),
                      template="plotly_white")
    _save(fig, "05_first_like_cliff")

    # 06 flow attribution
    fl = data["flow"]
    x = [r[0] for r in fl]
    new = [r[1] for r in fl]; res_ = [r[2] for r in fl]
    cn = [r[3] for r in fl]; ca = [r[4] for r in fl]
    net = [new[i] + res_[i] - cn[i] - ca[i] for i in range(len(x))]
    fig = go.Figure()
    fig.add_trace(go.Bar(x=x, y=new, name="New activations", marker_color="#16a34a"))
    fig.add_trace(go.Bar(x=x, y=res_, name="Resurrected", marker_color=BRAND))
    fig.add_trace(go.Bar(x=x, y=[-v for v in cn], name="Churned — new (<90d)", marker_color="#f59e0b"))
    fig.add_trace(go.Bar(x=x, y=[-v for v in ca], name="Churned — active (≥90d)", marker_color="#ef4444"))
    fig.add_trace(go.Scatter(x=x, y=net, name="Net", mode="lines", line=dict(color="#111827", width=2)))
    fig.update_layout(barmode="relative",
                      title=dict(text="<b>Weekly flow, churn split by tenure</b>  ·  who's leaving — new users or the old guard",
                                 x=0.02, xanchor="left"),
                      xaxis_title="Week", yaxis_title="Actors/week (in +, churn −)",
                      template="plotly_white", legend=dict(orientation="h", y=-0.2))
    _save(fig, "06_flow_attribution")

    # 07 retention metrics
    mt = data["metrics"]
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Scatter(x=[r[0] for r in mt["activation"]], y=[r[1] for r in mt["activation"]],
                             name="New-user 28-day activation", line=dict(color="#16a34a", width=2)), secondary_y=False)
    fig.add_trace(go.Scatter(x=[r[0] for r in mt["churn"]], y=[r[1] for r in mt["churn"]],
                             name="Weekly churn of retained base", line=dict(color="#ef4444", width=2)), secondary_y=True)
    fig.update_layout(title=dict(text="<b>Activation vs. churn over time</b>  ·  Mar-2025 onward (Q1 wave clipped)",
                                 x=0.02, xanchor="left"),
                      xaxis_title="Week", template="plotly_white", hovermode="x unified",
                      legend=dict(orientation="h", y=-0.2))
    fig.update_yaxes(title_text="Activation @28d (%)", color="#16a34a", secondary_y=False, rangemode="tozero")
    fig.update_yaxes(title_text="Weekly churn (%)", color="#ef4444", secondary_y=True, rangemode="tozero")
    _save(fig, "07_retention_metrics")

    # 08 projections
    p = data["proj"]
    B0, c, N, R = p["B0"], p["c"], p["N"], p["R"]
    wks = []
    d = dt.date(2026, 5, 11)
    while d <= dt.date(2027, 12, 31):
        wks.append(d.isoformat()); d += dt.timedelta(weeks=1)
    nW = len(wks)

    def proj(cc, i_new):
        B, ys = B0, []
        for _ in range(nW):
            ys.append(B); B = B * (1 - cc) + i_new + R
        return ys

    def projfig(title, name, variants, hline=None):
        fig = go.Figure()
        for lbl, ys, color, dash in variants:
            fig.add_trace(go.Scatter(x=wks, y=ys, name=lbl, line=dict(color=color, width=2, dash=dash)))
        fig.add_hline(y=B0, line=dict(color="#111827", width=1, dash="dot"),
                      annotation_text=f"today {B0/1e6:.1f}M", annotation_position="top left")
        if hline:
            fig.add_hline(y=hline, line=dict(color="#16a34a", width=1.5, dash="dot"),
                          annotation_text=f"10× = {hline/1e6:.0f}M", annotation_position="top right")
        fig.update_layout(title=dict(text=title, x=0.02, xanchor="left"),
                          xaxis_title="Week", yaxis=dict(title="Engaged users (non-churned)", rangemode="tozero"),
                          template="plotly_white", hovermode="x unified", legend=dict(x=1.01, y=1))
        _save(fig, name)

    pal2 = ["#ef4444", "#6b7280", "#0d9488", "#2563eb", "#7c3aed"]
    projfig("<b>Projection — vary new-user retention</b>  ·  churn &amp; volume fixed", "08_projection_retention",
            [(f"{m:g}× retention → {(N*m+R)/c/1e6:.1f}M", proj(c, N * m), pal2[i],
              "dash" if m == 1 else "solid") for i, m in enumerate([0.5, 1, 2, 3, 4])])
    projfig("<b>Projection — vary churn</b>  ·  volume &amp; retention fixed", "08_projection_churn",
            [(f"churn {cc*100:.1f}%/wk → {(N+R)/cc/1e6:.1f}M", proj(cc, N), pal2[i],
              "dash" if abs(cc - c) < 0.005 else "solid") for i, cc in enumerate([c, 0.07, 0.05, 0.03, 0.02])])
    projfig("<b>Projection — vary new-user volume</b>  ·  churn &amp; retention fixed", "08_projection_volume",
            [(f"{vN/1e3:.0f}k new/wk → {(vN+R)/c/1e6:.1f}M", proj(c, vN), pal2[i],
              "dash" if vN == N else "solid") for i, vN in enumerate([N, 300_000, 600_000, 1_000_000, 2_000_000])])
    opt = proj(c / 2, N * 6.5 * 2)
    projfig("<b>Optimistic path to 10× — all three levers</b>  ·  volume ×6.5, retention ×2, churn ÷2",
            "08_projection_optimistic",
            [("Today's trajectory", proj(c, N), "#6b7280", "dash"),
             (f"Optimistic → {opt[-1]/1e6:.0f}M", opt, "#7c3aed", "solid")], hline=10 * B0)

    return sorted(os.listdir(FOLDER))


@app.local_entrypoint()
def main():
    data = compute.remote()
    os.makedirs(FOLDER, exist_ok=True)
    with open(f"{FOLDER}/_data.json", "w") as f:
        json.dump(data, f, indent=2, default=str)
    files = render(data)
    print("Wrote", len(files), "files to", FOLDER)
    for f in files:
        print("  ", f)
