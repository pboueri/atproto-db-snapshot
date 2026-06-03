"""Local account-viewer server. Zero third-party deps beyond duckdb.

  list pane   -> queries the local accounts.duckdb (built by build_accounts_db.py)
  detail pane -> hydrates one account live from the bsky public AppView API
                 (getProfile / getFollows / getAuthorFeed), cached to sqlite

Run:
    .venv/bin/python analysis/account_viewer/server.py
    # then open http://127.0.0.1:8765

Nothing here writes to the snapshot; the duckdb is opened read-only. The only
local writes are the API cache and your account labels, both in viewer.sqlite
next to this file.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import duckdb

HERE = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(HERE, "accounts.duckdb")
CACHE_PATH = os.path.join(HERE, "viewer.sqlite")
STATIC_DIR = os.path.join(HERE, "static")

APPVIEW = "https://public.api.bsky.app/xrpc"
CACHE_TTL = 7 * 24 * 3600  # re-fetch profiles older than a week

# Columns returned to the list pane (covers every sort facet so the UI can show
# whatever you order by). Still small per row.
LIST_COLS = [
    "did", "did_id", "category", "active", "follows", "followers",
    "posts", "likes_out", "likes_in", "reposts_out", "reposts_in",
    "replies_out", "quotes_out", "quoted_count", "blocks_out", "blocks_in",
    "content", "any_activity",
]

# Facets the list can be sorted by (whitelist -- these interpolate into ORDER BY).
SORT_COLS = [
    "followers", "follows", "posts", "likes_out", "likes_in", "reposts_out",
    "reposts_in", "replies_out", "quotes_out", "quoted_count", "blocks_out",
    "blocks_in", "content", "any_activity", "did_id",
]

# ----------------------------------------------------------------------------
# storage
# ----------------------------------------------------------------------------

_duck_lock = threading.Lock()
_duck = duckdb.connect(DB_PATH, read_only=True)


def _cache_conn() -> sqlite3.Connection:
    # one connection per thread (sqlite objects aren't shareable across threads)
    con = sqlite3.connect(CACHE_PATH)
    con.execute(
        "CREATE TABLE IF NOT EXISTS api_cache "
        "(key TEXT PRIMARY KEY, body TEXT, status INTEGER, fetched_at REAL)"
    )
    con.execute(
        "CREATE TABLE IF NOT EXISTS labels "
        "(did TEXT PRIMARY KEY, label TEXT, note TEXT, updated_at REAL)"
    )
    return con


# ----------------------------------------------------------------------------
# bsky api (server-side, cached)
# ----------------------------------------------------------------------------

def _http_get_json(url: str) -> tuple[int, dict]:
    req = urllib.request.Request(url, headers={"User-Agent": "atproto-account-viewer/1"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.status, json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode("utf-8"))
        except Exception:
            return e.code, {"error": "HTTPError", "message": str(e)}
    except Exception as e:  # network/timeout
        return 0, {"error": "fetch_failed", "message": str(e)}


def _cached_call(method: str, params: dict, *, force: bool = False) -> dict:
    """GET an xrpc method with disk caching. Returns the JSON body."""
    qs = urllib.parse.urlencode(params)
    key = f"{method}?{qs}"
    con = _cache_conn()
    if not force:
        row = con.execute(
            "SELECT body, fetched_at FROM api_cache WHERE key=?", (key,)
        ).fetchone()
        if row and (time.time() - row[1]) < CACHE_TTL:
            con.close()
            return json.loads(row[0])
    status, body = _http_get_json(f"{APPVIEW}/{method}?{qs}")
    # Don't cache transient network failures; do cache real API answers
    # (including 400 "profile not found" -- that's a stable, useful signal).
    if status != 0:
        con.execute(
            "INSERT OR REPLACE INTO api_cache(key, body, status, fetched_at) "
            "VALUES (?,?,?,?)",
            (key, json.dumps(body), status, time.time()),
        )
        con.commit()
    con.close()
    body["_status"] = status
    return body


def hydrate(did: str, *, force: bool = False) -> dict:
    """Pull live profile + follows + recent feed for one DID."""
    profile = _cached_call("app.bsky.actor.getProfile", {"actor": did}, force=force)
    out: dict = {"did": did, "profile": profile}

    # If the profile call failed (deactivated/deleted/takedown/etc.), that IS
    # the diagnosis -- surface it and skip the follow-up calls.
    if profile.get("error"):
        out["status_reason"] = profile.get("error")
        out["follows"] = None
        out["feed"] = None
        return out

    follows = _cached_call(
        "app.bsky.graph.getFollows", {"actor": did, "limit": 100}, force=force
    )
    feed = _cached_call(
        "app.bsky.feed.getAuthorFeed",
        {"actor": did, "limit": 30, "filter": "posts_no_replies"},
        force=force,
    )
    out["follows"] = follows.get("follows") if not follows.get("error") else None
    out["feed_error"] = feed.get("error")
    # Reduce the feed to just what the timeline sparkline + preview need.
    items = []
    for it in (feed.get("feed") or []):
        post = it.get("post", {})
        rec = post.get("record", {})
        items.append({
            "uri": post.get("uri"),
            "createdAt": rec.get("createdAt") or post.get("indexedAt"),
            "text": (rec.get("text") or "")[:280],
            "likeCount": post.get("likeCount", 0),
            "repostCount": post.get("repostCount", 0),
            "replyCount": post.get("replyCount", 0),
            "isRepost": it.get("reason", {}).get("$type", "").endswith("reasonRepost"),
            "media": _extract_media(post.get("embed")),
        })
    out["feed"] = items
    return out


def _extract_media(embed: dict | None) -> dict:
    """Normalize a hydrated post embed view into {images, external, video, quote}.

    Handles the AppView `*#view` embed types incl. recordWithMedia. URLs here
    are bsky CDN links the browser can load directly.
    """
    out = {"images": [], "external": None, "video": None, "quote": None}
    if not embed:
        return out
    t = embed.get("$type", "")
    if t.endswith("recordWithMedia#view"):
        media = _extract_media(embed.get("media"))
        out.update({k: media[k] for k in ("images", "external", "video")})
        rec = (embed.get("record") or {}).get("record") or {}
        out["quote"] = _quote_summary(rec)
        return out
    if t.endswith("images#view"):
        for im in embed.get("images", [])[:4]:
            out["images"].append({
                "thumb": im.get("thumb"), "full": im.get("fullsize"),
                "alt": im.get("alt", ""),
            })
    elif t.endswith("video#view"):
        out["video"] = {"thumb": embed.get("thumbnail"), "alt": embed.get("alt", "")}
    elif t.endswith("external#view"):
        ext = embed.get("external", {})
        out["external"] = {
            "uri": ext.get("uri"), "title": ext.get("title", ""),
            "thumb": ext.get("thumb"),
        }
    elif t.endswith("record#view"):
        out["quote"] = _quote_summary((embed.get("record") or {}))
    return out


def _quote_summary(rec: dict) -> dict | None:
    """A tiny summary of a quoted post (author handle + text)."""
    if not rec:
        return None
    author = rec.get("author", {})
    val = rec.get("value", {}) or rec.get("record", {})
    return {
        "handle": author.get("handle"),
        "text": (val.get("text") or "")[:200],
    }


# ----------------------------------------------------------------------------
# duckdb queries
# ----------------------------------------------------------------------------

def _jsonable(v):
    # did_id is u64 and can exceed JS-safe ints -> send as string
    if isinstance(v, int) and abs(v) > 2**53:
        return str(v)
    return v


def query_accounts(args: dict) -> dict:
    where = (args.get("where") or "").strip()
    raw_sql = (args.get("sql") or "").strip()
    category = (args.get("category") or "").strip()
    try:
        limit = min(int(args.get("limit", 200)), 2000)
    except ValueError:
        limit = 200
    try:
        offset = max(int(args.get("offset", 0)), 0)
    except ValueError:
        offset = 0
    randomize = args.get("random") in ("1", "true", "yes")
    try:
        seed = float(args.get("seed", 0.42))
    except ValueError:
        seed = 0.42

    # Sort facet (whitelisted column + direction). Ignored when randomizing.
    sort = args.get("sort", "followers")
    if sort not in SORT_COLS:
        sort = "followers"
    direction = "ASC" if str(args.get("dir", "desc")).lower() == "asc" else "DESC"

    cols = ", ".join(LIST_COLS)
    if raw_sql:
        # Power mode: user-supplied SELECT. Connection is read-only so this
        # can't mutate anything. We still wrap it so we can paginate.
        sql = f"SELECT * FROM ({raw_sql}) _q LIMIT {limit} OFFSET {offset}"
    else:
        clauses = []
        if category:
            clauses.append("category = ?")
        if where:
            clauses.append(f"({where})")
        wsql = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        # Secondary key on did_id keeps pagination stable across pages.
        orderby = (
            "ORDER BY random()" if randomize
            else f"ORDER BY {sort} {direction}, did_id"
        )
        sql = (
            f"SELECT {cols} FROM accounts{wsql} {orderby} "
            f"LIMIT {limit} OFFSET {offset}"
        )

    params = [category] if (category and not raw_sql) else []
    with _duck_lock:
        cur = _duck.cursor()
        # setseed must run in the same statement batch; execute separately
        if not raw_sql and randomize:
            cur.execute(f"SELECT setseed({seed})")
        cur.execute(sql, params)
        names = [d[0] for d in cur.description]
        rows = [
            {n: _jsonable(v) for n, v in zip(names, r)} for r in cur.fetchall()
        ]
    return {"rows": rows, "count": len(rows), "offset": offset, "limit": limit}


def account_row(did: str) -> dict | None:
    with _duck_lock:
        cur = _duck.cursor()
        cur.execute("SELECT * FROM accounts WHERE did = ?", [did])
        row = cur.fetchone()
        if not row:
            return None
        names = [d[0] for d in cur.description]
    return {n: _jsonable(v) for n, v in zip(names, row)}


def meta() -> dict:
    with _duck_lock:
        cur = _duck.cursor()
        try:
            snap = cur.execute("SELECT snapshot_date, built_at FROM meta").fetchone()
        except Exception:
            snap = (None, None)
        total = cur.execute("SELECT count(*) FROM accounts").fetchone()[0]
        cats = cur.execute(
            "SELECT category, count(*) n FROM accounts GROUP BY category ORDER BY n DESC"
        ).fetchall()
    return {
        "snapshot_date": str(snap[0]) if snap[0] else None,
        "built_at": snap[1],
        "total": total,
        "categories": [{"category": c, "n": n} for c, n in cats],
        "columns": [c[1] for c in _columns()],
        "sort_cols": SORT_COLS,
    }


def _columns():
    with _duck_lock:
        return _duck.execute("PRAGMA table_info('accounts')").fetchall()


# ----------------------------------------------------------------------------
# labels
# ----------------------------------------------------------------------------

def set_label(did: str, label: str, note: str) -> dict:
    con = _cache_conn()
    con.execute(
        "INSERT OR REPLACE INTO labels(did, label, note, updated_at) VALUES (?,?,?,?)",
        (did, label, note, time.time()),
    )
    con.commit()
    con.close()
    return {"ok": True, "did": did, "label": label, "note": note}


def get_label(did: str) -> dict:
    con = _cache_conn()
    row = con.execute(
        "SELECT label, note, updated_at FROM labels WHERE did=?", (did,)
    ).fetchone()
    con.close()
    if not row:
        return {"did": did, "label": None, "note": None}
    return {"did": did, "label": row[0], "note": row[1], "updated_at": row[2]}


def all_labels() -> dict:
    con = _cache_conn()
    rows = con.execute(
        "SELECT did, label, note, updated_at FROM labels ORDER BY updated_at DESC"
    ).fetchall()
    con.close()
    return {
        "labels": [
            {"did": d, "label": l, "note": nt, "updated_at": u}
            for d, l, nt, u in rows
        ]
    }


# ----------------------------------------------------------------------------
# http
# ----------------------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):  # quieter console
        pass

    def _send(self, code: int, body: bytes, ctype: str):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _json(self, obj, code=200):
        self._send(code, json.dumps(obj).encode("utf-8"), "application/json")

    def _static(self, rel: str):
        path = os.path.normpath(os.path.join(STATIC_DIR, rel))
        if not path.startswith(STATIC_DIR) or not os.path.isfile(path):
            return self._send(404, b"not found", "text/plain")
        ctype = {
            ".html": "text/html", ".js": "application/javascript",
            ".css": "text/css",
        }.get(os.path.splitext(path)[1], "application/octet-stream")
        with open(path, "rb") as f:
            self._send(200, f.read(), ctype + "; charset=utf-8")

    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        q = {k: v[0] for k, v in urllib.parse.parse_qs(u.query).items()}
        try:
            if u.path == "/":
                return self._static("index.html")
            if u.path.startswith("/static/"):
                return self._static(u.path[len("/static/"):])
            if u.path == "/api/meta":
                return self._json(meta())
            if u.path == "/api/accounts":
                return self._json(query_accounts(q))
            if u.path == "/api/account":
                did = q.get("did", "")
                row = account_row(did)
                hyd = hydrate(did, force=q.get("force") in ("1", "true"))
                return self._json({"snapshot": row, "live": hyd,
                                   "label": get_label(did)})
            if u.path == "/api/label":
                return self._json(get_label(q.get("did", "")))
            if u.path == "/api/labels":
                return self._json(all_labels())
            return self._send(404, b"not found", "text/plain")
        except Exception as e:
            return self._json({"error": str(e)}, 500)

    def do_POST(self):
        u = urllib.parse.urlparse(self.path)
        n = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(n) or b"{}") if n else {}
        try:
            if u.path == "/api/label":
                return self._json(set_label(
                    body.get("did", ""), body.get("label", ""), body.get("note", "")))
            return self._send(404, b"not found", "text/plain")
        except Exception as e:
            return self._json({"error": str(e)}, 500)


def main():
    port = int(os.environ.get("PORT", "8765"))
    if not os.path.exists(DB_PATH):
        raise SystemExit(
            f"missing {DB_PATH}\nrun: .venv/bin/python "
            f"analysis/account_viewer/build_accounts_db.py first"
        )
    srv = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"account viewer -> http://127.0.0.1:{port}  (db: {DB_PATH})", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nbye")


if __name__ == "__main__":
    main()
