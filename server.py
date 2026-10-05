"""X campaign analyzer — web app.

Local:   python3 server.py                       -> http://localhost:8787 (no login unless GOOGLE_CLIENT_ID is set)
Server:  see README.md (Railway: Dockerfile + volume at /data + env vars)

Pieces:
  * SQLite database (DATA_DIR/campaigns.db) — campaigns, posts, daily registrations.
  * Background worker thread — fetches X metrics so requests return instantly.
  * Google sign-in (auth.py) — only @ALLOWED_DOMAIN accounts get a session cookie.
  * Daily automatic backups in DATA_DIR/backups, plus download/restore endpoints.
"""
import csv
import io
import json
import os
import queue
import shutil
import sqlite3
import threading
import time
from datetime import datetime, timezone
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import auth
from x_client import XClient, tweet_id_from

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR", HERE)
DB_PATH = os.path.join(DATA_DIR, "campaigns.db")
BACKUP_DIR = os.path.join(DATA_DIR, "backups")
HOST = os.environ.get("HOST", "0.0.0.0" if os.environ.get("PORT") else "127.0.0.1")
PORT = int(os.environ.get("PORT", 8787))
PUBLIC_URL = os.environ.get("PUBLIC_URL", "").rstrip("/")   # e.g. https://campaigns.up.railway.app
KEEP_BACKUPS = 14

METRICS = ["views", "likes", "retweets", "quotes", "replies", "bookmarks"]

_db_lock = threading.Lock()     # serializes writes; SQLite allows one writer at a time


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    os.makedirs(DATA_DIR, exist_ok=True)
    with db() as c:
        c.execute("PRAGMA journal_mode=WAL")   # readers don't block the writer
        c.executescript("""
        CREATE TABLE IF NOT EXISTS campaigns (
            id INTEGER PRIMARY KEY, name TEXT NOT NULL, created_at TEXT);
        CREATE TABLE IF NOT EXISTS posts (
            id INTEGER PRIMARY KEY,
            campaign_id INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
            url TEXT NOT NULL, tweet_id TEXT NOT NULL,
            author TEXT, text TEXT, created_at TEXT,
            day TEXT,                       -- attribution day (YYYY-MM-DD), editable
            budget REAL DEFAULT 0,
            views INTEGER DEFAULT 0, likes INTEGER DEFAULT 0, retweets INTEGER DEFAULT 0,
            quotes INTEGER DEFAULT 0, replies INTEGER DEFAULT 0, bookmarks INTEGER DEFAULT 0,
            fetched_at TEXT, fetch_error TEXT,
            UNIQUE(campaign_id, tweet_id));
        CREATE TABLE IF NOT EXISTS days (
            campaign_id INTEGER NOT NULL REFERENCES campaigns(id) ON DELETE CASCADE,
            day TEXT NOT NULL,
            registrations INTEGER DEFAULT 0,
            api_users INTEGER DEFAULT 0,
            PRIMARY KEY (campaign_id, day));
        """)

        # Migrations: payment notes per post; non-X spend (e.g. LinkedIn deals) per campaign
        def add_col(table, col, decl):
            if col not in [r[1] for r in c.execute(f"PRAGMA table_info({table})")]:
                c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")
        add_col("posts", "note", "TEXT")
        add_col("campaigns", "other_spend", "REAL DEFAULT 0")
        add_col("campaigns", "other_note", "TEXT")
        if not c.execute("SELECT 1 FROM campaigns").fetchone():
            c.execute("INSERT INTO campaigns(name, created_at) VALUES (?, ?)", ("My first campaign", now()))


# ---------------------------------------------------------------- background X fetching

_jobs = queue.Queue()
_pending = set()                 # post ids queued or in flight, shown as "fetching…" in the UI
_pending_lock = threading.Lock()


def enqueue(post_id, tweet_id):
    with _pending_lock:
        if post_id in _pending:
            return
        _pending.add(post_id)
    _jobs.put((post_id, tweet_id))


def fetch_worker():
    client = None
    while True:
        post_id, tweet_id = _jobs.get()
        try:
            if client is None:
                client = XClient()
            m = client.fetch(tweet_id)          # network call happens outside the DB lock
            with _db_lock, db() as c:
                c.execute(
                    f"""UPDATE posts SET author=?, text=?, created_at=?, {", ".join(f"{k}=?" for k in METRICS)},
                        fetched_at=?, fetch_error=NULL, day=COALESCE(day, ?) WHERE id=?""",
                    (m["author"], m["text"], m["created_at"], *[m[k] for k in METRICS],
                     now(), (m["created_at"] or "")[:10] or None, post_id))
        except Exception as e:  # noqa: BLE001 — record any failure on the row so the UI shows it
            print(f"fetch {tweet_id} failed: {e}", flush=True)
            if "guest" in str(e).lower():
                client = None
            with _db_lock, db() as c:
                c.execute("UPDATE posts SET fetch_error=?, fetched_at=? WHERE id=?", (str(e)[:200], now(), post_id))
        finally:
            with _pending_lock:
                _pending.discard(post_id)
            time.sleep(0.4)                     # be gentle with X's rate limits


# ---------------------------------------------------------------- backups

def snapshot(path):
    """Consistent copy of the live database (safe while the app is running)."""
    src = db()
    dst = sqlite3.connect(path)
    with dst:
        src.backup(dst)
    dst.close()
    src.close()


def backup_worker():
    os.makedirs(BACKUP_DIR, exist_ok=True)
    while True:
        target = os.path.join(BACKUP_DIR, f"auto-{datetime.now(timezone.utc):%Y-%m-%d}.db")
        if not os.path.exists(target):
            snapshot(target)
            olds = sorted(f for f in os.listdir(BACKUP_DIR) if f.startswith("auto-"))
            for f in olds[:-KEEP_BACKUPS]:
                os.remove(os.path.join(BACKUP_DIR, f))
        time.sleep(3600)


def restore(data: bytes):
    if not data.startswith(b"SQLite format 3\x00"):
        raise ValueError("not a SQLite database file")
    tmp = DB_PATH + ".upload"
    with open(tmp, "wb") as f:
        f.write(data)
    chk = sqlite3.connect(tmp)
    tables = {r[0] for r in chk.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    chk.close()
    if not {"campaigns", "posts", "days"} <= tables:
        os.remove(tmp)
        raise ValueError("file is not a campaign analyzer database")
    os.makedirs(BACKUP_DIR, exist_ok=True)
    snapshot(os.path.join(BACKUP_DIR, f"pre-restore-{datetime.now(timezone.utc):%Y%m%d-%H%M%S}.db"))
    for suffix in ("-wal", "-shm"):
        if os.path.exists(DB_PATH + suffix):
            os.remove(DB_PATH + suffix)
    shutil.move(tmp, DB_PATH)
    init_db()                                   # apply migrations to the restored file


# ---------------------------------------------------------------- data views

def state(campaign_id):
    with db() as c:
        campaigns = [dict(r) for r in c.execute("SELECT * FROM campaigns ORDER BY id")]
        posts = [dict(r) for r in c.execute("SELECT * FROM posts WHERE campaign_id=? ORDER BY day, id", (campaign_id,))]
        days = [dict(r) for r in c.execute("SELECT * FROM days WHERE campaign_id=? ORDER BY day", (campaign_id,))]
    with _pending_lock:
        pending = [p["id"] for p in posts if p["id"] in _pending]
    return {"campaigns": campaigns, "posts": posts, "days": days, "pending": pending}


def export_csv(campaign_id):
    s = state(campaign_id)
    buf = io.StringIO()
    w = csv.writer(buf)
    camp = next(c for c in s["campaigns"] if c["id"] == campaign_id)
    x_spend = sum(p["budget"] or 0 for p in s["posts"])
    w.writerow(["x_posts_spend", "other_spend", "other_note", "total_budget"])
    w.writerow([x_spend, camp["other_spend"] or 0, camp["other_note"] or "", x_spend + (camp["other_spend"] or 0)])
    w.writerow([])
    w.writerow(["day", "url", "author", "note", "budget", *METRICS, "engagements", "er_pct", "cpm", "cpe"])
    for p in s["posts"]:
        eng = sum(p[k] for k in METRICS[1:])
        v, b = p["views"], p["budget"] or 0
        w.writerow([p["day"], p["url"], p["author"], p["note"] or "", b, *[p[k] for k in METRICS], eng,
                    round(eng / v * 100, 3) if v else "", round(b / v * 1000, 2) if v else "",
                    round(b / eng, 3) if eng else ""])
    w.writerow([])
    w.writerow(["day", "spend", "views", "engagements", "registrations", "api_users",
                "cost_per_registration", "cost_per_api_user", "reg_to_api_pct"])
    by_day = {}
    for p in s["posts"]:
        d = by_day.setdefault(p["day"] or "", {"spend": 0, "views": 0, "eng": 0, "reg": 0, "api": 0})
        d["spend"] += p["budget"] or 0
        d["views"] += p["views"]
        d["eng"] += sum(p[k] for k in METRICS[1:])
    for r in s["days"]:
        d = by_day.setdefault(r["day"], {"spend": 0, "views": 0, "eng": 0, "reg": 0, "api": 0})
        d["reg"], d["api"] = r["registrations"], r["api_users"]
    for day in sorted(by_day):
        d = by_day[day]
        w.writerow([day, d["spend"], d["views"], d["eng"], d["reg"], d["api"],
                    round(d["spend"] / d["reg"], 2) if d["reg"] else "",
                    round(d["spend"] / d["api"], 2) if d["api"] else "",
                    round(d["api"] / d["reg"] * 100, 1) if d["reg"] else ""])
    return buf.getvalue()


LOGIN_PAGE = """<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Sign in · X Campaign Analyzer</title><style>
:root{--bg:#f7f7f5;--s:#fff;--b:#e3e3de;--t:#1a1a18;--m:#6b6b66;--a:#1d6fe0;--bad:#b42318}
@media (prefers-color-scheme:dark){:root{--bg:#121211;--s:#1b1b1a;--b:#33332f;--t:#ececea;--m:#9a9a94;--a:#5b9bff;--bad:#ff7a6b}}
body{margin:0;min-height:100vh;display:flex;align-items:center;justify-content:center;background:var(--bg);color:var(--t);
font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Inter,sans-serif;padding:16px}
.c{background:var(--s);border:1px solid var(--b);border-radius:14px;padding:28px;max-width:360px;width:100%;text-align:center}
h1{font-size:18px;margin:0 0 6px}p{color:var(--m);margin:0 0 20px}.e{color:var(--bad);margin-bottom:16px}
a{display:inline-block;background:var(--a);color:#fff;text-decoration:none;font-weight:600;padding:10px 18px;border-radius:9px}
</style></head><body><div class="c"><h1>X Campaign Analyzer</h1><p>Sign in with your @__DOMAIN__ Google account.</p>
__ERROR__<a href="/auth/login">Sign in with Google</a></div></body></html>"""


# ---------------------------------------------------------------- HTTP

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    # --- helpers
    def _send(self, code, body, ctype="application/json", headers=()):
        data = body if isinstance(body, bytes) else (json.dumps(body) if ctype == "application/json" else body).encode()
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        for k, v in headers:
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)

    def _redirect(self, location, cookies=()):
        self.send_response(302)
        self.send_header("Location", location)
        for c in cookies:
            self.send_header("Set-Cookie", c)
        self.end_headers()

    def _body_bytes(self):
        return self.rfile.read(int(self.headers.get("Content-Length") or 0))

    def _cookie(self, name):
        jar = SimpleCookie(self.headers.get("Cookie") or "")
        return jar[name].value if name in jar else None

    def _https(self):
        return self.headers.get("X-Forwarded-Proto", "").startswith("https") or PUBLIC_URL.startswith("https")

    def _set_cookie(self, name, value, max_age):
        secure = "; Secure" if self._https() else ""
        return f"{name}={value}; Path=/; Max-Age={max_age}; HttpOnly; SameSite=Lax{secure}"

    def _base_url(self):
        if PUBLIC_URL:
            return PUBLIC_URL
        return f"{'https' if self._https() else 'http'}://{self.headers.get('Host')}"

    def _user(self):
        if not auth.ENABLED:
            return {"email": "local", "name": "Local (no login)"}
        tok = self._cookie("session")
        return auth.unsign(tok) if tok else None

    # --- auth routes (public)
    def _auth_routes(self, u):
        redirect_uri = self._base_url() + "/auth/callback"
        if u.path == "/login":
            err = parse_qs(u.query).get("error", [""])[0]
            html = LOGIN_PAGE.replace("__DOMAIN__", auth.ALLOWED_DOMAIN).replace(
                "__ERROR__", f'<div class="e">{err.replace("<", "&lt;")}</div>' if err else "")
            self._send(200, html, "text/html; charset=utf-8")
            return True
        if u.path == "/auth/login":
            state_ = auth.new_state()
            self._redirect(auth.login_url(redirect_uri, state_),
                           [self._set_cookie("oauth_state", auth.sign({"s": state_, "exp": time.time() + 600}), 600)])
            return True
        if u.path == "/auth/callback":
            q = parse_qs(u.query)
            st = auth.unsign(self._cookie("oauth_state") or "")
            if not st or st.get("s") != q.get("state", [""])[0] or "code" not in q:
                self._redirect("/login?error=Sign-in+expired.+Please+try+again.")
                return True
            try:
                user = auth.exchange_code(q["code"][0], redirect_uri)
            except PermissionError as e:
                self._redirect("/login?error=" + str(e).replace(" ", "+"), [self._set_cookie("oauth_state", "", 0)])
                return True
            print(f"login: {user['email']}", flush=True)
            self._redirect("/", [self._set_cookie("session", auth.session_token(user), auth.SESSION_DAYS * 86400),
                                 self._set_cookie("oauth_state", "", 0)])
            return True
        if u.path == "/auth/logout":
            self._redirect("/login", [self._set_cookie("session", "", 0)])
            return True
        return False

    def _require_user(self, u):
        user = self._user()
        if user:
            return user
        if u.path.startswith("/api/"):
            self._send(401, {"error": "login required"})
        else:
            self._redirect("/login")
        return None

    # --- GET
    def do_GET(self):
        u = urlparse(self.path)
        if u.path == "/healthz":
            return self._send(200, {"ok": True})
        if auth.ENABLED and self._auth_routes(u):
            return
        user = self._require_user(u)
        if not user:
            return
        q = parse_qs(u.query)
        if u.path in ("/", "/index.html"):
            with open(os.path.join(HERE, "static", "index.html"), "rb") as f:
                return self._send(200, f.read(), "text/html; charset=utf-8")
        if u.path == "/api/me":
            return self._send(200, {"email": user["email"], "name": user.get("name"), "auth": auth.ENABLED})
        if u.path == "/api/state":
            return self._send(200, state(int(q["campaign"][0])))
        if u.path == "/api/export.csv":
            cid = int(q["campaign"][0])
            return self._send(200, export_csv(cid).encode(), "text/csv",
                              [("Content-Disposition", f'attachment; filename="campaign_{cid}.csv"')])
        if u.path == "/api/backup.db":
            tmp = os.path.join(DATA_DIR, "download.tmp.db")
            snapshot(tmp)
            with open(tmp, "rb") as f:
                data = f.read()
            os.remove(tmp)
            return self._send(200, data, "application/octet-stream",
                              [("Content-Disposition", f'attachment; filename="campaigns-{datetime.now():%Y%m%d-%H%M}.db"')])
        self._send(404, {"error": "not found"})

    # --- POST
    def do_POST(self):
        u = urlparse(self.path)
        user = self._require_user(u)
        if not user:
            return
        path = u.path
        if path == "/api/restore":
            try:
                with _db_lock:
                    restore(self._body_bytes())
            except ValueError as e:
                return self._send(400, {"error": str(e)})
            print(f"restore by {user['email']}", flush=True)
            return self._send(200, {"ok": True})

        b = json.loads(self._body_bytes() or b"{}")
        to_fetch, result = [], {"ok": True}
        with _db_lock, db() as c:
            if path == "/api/campaigns":
                cur = c.execute("INSERT INTO campaigns(name, created_at) VALUES (?, ?)", (b["name"].strip(), now()))
                return self._send(200, {"id": cur.lastrowid})

            elif path == "/api/campaigns/rename":
                c.execute("UPDATE campaigns SET name=? WHERE id=?", (b["name"].strip(), b["id"]))

            elif path == "/api/campaigns/other_spend":
                c.execute("UPDATE campaigns SET other_spend=?, other_note=? WHERE id=?",
                          (float(b.get("other_spend") or 0), b.get("other_note"), b["id"]))

            elif path == "/api/campaigns/delete":
                c.execute("PRAGMA foreign_keys=ON")
                c.execute("DELETE FROM campaigns WHERE id=?", (b["id"],))

            elif path == "/api/posts":
                cid = b["campaign_id"]
                items = b.get("items") or [{"url": x, "budget": b.get("budget")} for x in b.get("urls", [])]
                added, skipped, invalid = 0, 0, []
                for it in items:
                    raw, budget, note = it["url"].strip(), float(it.get("budget") or 0), it.get("note") or None
                    if not raw:
                        continue
                    tid = tweet_id_from(raw)
                    if not tid:
                        invalid.append(raw)
                        continue
                    cur = c.execute("INSERT OR IGNORE INTO posts(campaign_id, url, tweet_id, budget, note) VALUES (?,?,?,?,?)",
                                    (cid, raw.split("?")[0], tid, budget, note))
                    if not cur.rowcount:
                        # Already tracked: update its cost/note from the new paste
                        c.execute("UPDATE posts SET budget=?, note=COALESCE(?, note) WHERE campaign_id=? AND tweet_id=?",
                                  (budget, note, cid, tid))
                        skipped += 1
                        continue
                    added += 1
                    to_fetch.append((cur.lastrowid, tid))
                result = {"added": added, "skipped": skipped, "invalid": invalid}

            elif path == "/api/posts/update":
                fields = {k: b[k] for k in ("budget", "day", "note") if k in b}
                if fields:
                    c.execute(f"UPDATE posts SET {', '.join(f'{k}=?' for k in fields)} WHERE id=?",
                              (*fields.values(), b["id"]))

            elif path == "/api/posts/delete":
                c.execute("DELETE FROM posts WHERE id=?", (b["id"],))

            elif path == "/api/refresh":
                ids = set(b.get("ids") or [])
                rows = c.execute("SELECT id, tweet_id FROM posts WHERE campaign_id=?", (b["campaign_id"],)).fetchall()
                to_fetch = [(r["id"], r["tweet_id"]) for r in rows if not ids or r["id"] in ids]
                result = {"queued": len(to_fetch)}

            elif path == "/api/days":
                c.execute("""INSERT INTO days(campaign_id, day, registrations, api_users) VALUES (?,?,?,?)
                             ON CONFLICT(campaign_id, day) DO UPDATE SET
                             registrations=excluded.registrations, api_users=excluded.api_users""",
                          (b["campaign_id"], b["day"], int(b.get("registrations") or 0), int(b.get("api_users") or 0)))

            elif path == "/api/days/delete":
                c.execute("DELETE FROM days WHERE campaign_id=? AND day=?", (b["campaign_id"], b["day"]))

            else:
                return self._send(404, {"error": "not found"})

        for post_id, tid in to_fetch:          # after commit, so the worker sees the rows
            enqueue(post_id, tid)
        self._send(200, result)


if __name__ == "__main__":
    auth.check_config()
    init_db()
    threading.Thread(target=fetch_worker, daemon=True).start()
    threading.Thread(target=backup_worker, daemon=True).start()
    print(f"X campaign analyzer → http://{HOST}:{PORT}  (data: {DB_PATH}, login: {'Google @' + auth.ALLOWED_DOMAIN if auth.ENABLED else 'OFF'})", flush=True)
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()
