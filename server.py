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
import re
import shutil
import sqlite3
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from http.cookies import SimpleCookie
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, quote, urlencode, urlparse

import auth
import mcp_server
import platforms
from x_client import NeedsLogin, XClient, tweet_id_from
import statistics

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None

HERE = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.environ.get("DATA_DIR", HERE)
DB_PATH = os.path.join(DATA_DIR, "campaigns.db")
BACKUP_DIR = os.path.join(DATA_DIR, "backups")
HOST = os.environ.get("HOST", "0.0.0.0" if os.environ.get("PORT") else "127.0.0.1")
PORT = int(os.environ.get("PORT", 8787))
PUBLIC_URL = os.environ.get("PUBLIC_URL", "").rstrip("/")   # e.g. https://campaigns.up.railway.app
KEEP_BACKUPS = 14

METRICS = ["views", "likes", "retweets", "quotes", "replies", "bookmarks"]


INCLUDED = "(excluded IS NULL OR excluded = 0)"   # SQL filter: posts that count toward totals


def cost(p):
    return p.get("budget") or 0

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
        add_col("posts", "platform", "TEXT DEFAULT 'x'")       # x | linkedin | other
        add_col("posts", "excluded", "INTEGER DEFAULT 0")      # 1 = keep the post, leave its cost out of budgets
        add_col("campaigns", "other_spend", "REAL DEFAULT 0")
        add_col("campaigns", "other_note", "TEXT")
        # Signup export rows. When present they are the source of truth for daily registrations / API users.
        c.execute("""CREATE TABLE IF NOT EXISTS users (
            email TEXT PRIMARY KEY, signed_up TEXT NOT NULL,          -- UTC ISO
            keys INTEGER DEFAULT 0, requests_all REAL DEFAULT 0, requests_7d REAL DEFAULT 0,
            credit TEXT, unconfirmed INTEGER DEFAULT 0, ref TEXT, imported_at TEXT)""")
        # Influencer profiles + their normal performance (baseline), keyed "x:<handle>" / "linkedin:<name>"
        c.execute("""CREATE TABLE IF NOT EXISTS creators (
            key TEXT PRIMARY KEY, platform TEXT, handle TEXT, name TEXT, user_id TEXT, avatar TEXT,
            followers INTEGER, following INTEGER, posts_count INTEGER, verified INTEGER,
            median_views INTEGER, median_likes INTEGER, median_eng INTEGER, median_er REAL, sample_size INTEGER,
            baseline_source TEXT, baseline_note TEXT, profile_at TEXT, baseline_at TEXT, error TEXT)""")
        if not c.execute("SELECT 1 FROM campaigns").fetchone():
            c.execute("INSERT INTO campaigns(name, created_at) VALUES (?, ?)", ("My first campaign", now()))


# ---------------------------------------------------------------- background X fetching

_jobs = queue.Queue()
_pending = set()                 # post ids queued or in flight, shown as "fetching…" in the UI
_pending_lock = threading.Lock()


def enqueue(post_id, key, platform="x", url=None):
    if platform not in ("x", "linkedin", "creator"):
        return                                  # nothing public to fetch; metrics are typed in
    with _pending_lock:
        if post_id in _pending:
            return
        _pending.add(post_id)
    _jobs.put((post_id, key, platform, url))


def enqueue_rows(rows):
    for r in rows:
        enqueue(r["id"], r["tweet_id"], r["platform"] or "x", r["url"])


def fetch_worker():
    client = None
    while True:
        post_id, key, platform, url = _jobs.get()
        try:
            if platform == "creator":           # post_id is the creator key, url the handle
                if client is None:
                    client = XClient()
                fetch_creator(client, post_id, url)
                continue
            if platform == "linkedin":
                m = platforms.fetch_linkedin(url)
            else:
                if client is None:
                    client = XClient()
                m = client.fetch(key)           # network call happens outside the DB lock
            auto = [k for k in METRICS if k in platforms.AUTO_METRICS[platform]]   # never overwrite typed-in numbers
            with _db_lock, db() as c:
                c.execute(
                    f"""UPDATE posts SET author=?, text=?, created_at=?, {"".join(f"{k}=?, " for k in auto)}
                        fetched_at=?, fetch_error=NULL, day=COALESCE(day, ?) WHERE id=?""",
                    (m["author"], m["text"], m["created_at"], *[m[k] for k in auto],
                     now(), (m["created_at"] or "")[:10] or None, post_id))
                if m.get("resolved_key") and m["resolved_key"] != key:   # short link -> canonical post id
                    try:
                        c.execute("UPDATE posts SET tweet_id=?, url=? WHERE id=?", (m["resolved_key"], m["resolved_url"], post_id))
                    except sqlite3.IntegrityError:
                        pass                        # same post already tracked under its full URL
        except Exception as e:  # noqa: BLE001 — record any failure on the row so the UI shows it
            print(f"fetch {platform} {key} failed: {e}", flush=True)
            if "guest" in str(e).lower():
                client = None
            with _db_lock, db() as c:
                if platform == "creator":
                    c.execute("INSERT INTO creators(key, platform, handle) VALUES (?, 'x', ?) ON CONFLICT(key) DO NOTHING", (post_id, url))
                    c.execute("UPDATE creators SET error=?, profile_at=? WHERE key=?", (str(e)[:200], now(), post_id))
                else:
                    c.execute("UPDATE posts SET fetch_error=?, fetched_at=? WHERE id=?", (str(e)[:200], now(), post_id))
        finally:
            with _pending_lock:
                _pending.discard(post_id)
            time.sleep(0.4)                     # be gentle with X's rate limits


# ---------------------------------------------------------------- pack deals

def apply_pack(c, campaign_id, ids, total, note=None):
    """One price for a bundle of posts (e.g. an agency pack): split `total` evenly across the posts, putting the
    rounding cents on the last one so they add up exactly. ids=None means every post in the wave."""
    rows = c.execute("SELECT id, note FROM posts WHERE campaign_id=? ORDER BY id", (campaign_id,)).fetchall()
    if ids:
        ids = {int(i) for i in ids}
        rows = [r for r in rows if r["id"] in ids]
    if not rows:
        raise ValueError("no posts to apply the pack price to")
    if total < 0:
        raise ValueError("pack price can't be negative")
    each = round(total / len(rows), 2)
    tag = note or f"pack ${total:,.0f} ÷ {len(rows)}"
    for i, r in enumerate(rows):
        price = round(total - each * (len(rows) - 1), 2) if i == len(rows) - 1 else each
        old = r["note"] or ""
        new_note = old if tag in old else (f"{old} · {tag}" if old else tag)
        c.execute("UPDATE posts SET budget=?, note=? WHERE id=?", (price, new_note, r["id"]))
    return {"posts": len(rows), "each": each, "total": total, "note": tag}


# ---------------------------------------------------------------- creators (influencers)

CREATOR_TTL_H = 24          # refresh profiles / baselines once a day


def creator_key(platform, author):
    return f"{platform or 'x'}:{(author or '').strip().lower()}"


def fetch_creator(client, key, handle):
    """Profile (public) + baseline from their recent own posts (needs a logged-in X session).
    Paid posts are excluded from the baseline, so 'vs median' compares our post with their normal."""
    try:
        prof, base, err = client.profile(handle), None, None
    except ValueError:
        # Handle no longer resolves — usually a rename. Ask X who wrote one of their posts now, and move the
        # posts to the new handle; the next creators_view() picks the new handle up and fetches it.
        with db() as c:
            row = c.execute("SELECT tweet_id FROM posts WHERE lower(author)=lower(?) AND (platform='x' OR platform IS NULL) LIMIT 1",
                            (handle,)).fetchone()
        new = client.fetch(row[0])["author"] if row else None
        if not new or new.lower() == handle.lower():
            raise
        with _db_lock, db() as c:
            c.execute("UPDATE posts SET author=? WHERE lower(author)=lower(?) AND (platform='x' OR platform IS NULL)", (new, handle))
            c.execute("DELETE FROM creators WHERE key=?", (key,))
        print(f"creator renamed: @{handle} -> @{new}", flush=True)
        enqueue(creator_key("x", new), creator_key("x", new), "creator", new)
        return
    try:
        with db() as c:
            paid = {r[0] for r in c.execute("SELECT tweet_id FROM posts WHERE platform='x' OR platform IS NULL")}
        cutoff = (datetime.now(timezone.utc) - timedelta(days=90)).isoformat()   # "their normal" = the last 3 months
        recent = [t for t in client.recent_posts(prof["user_id"], prof["handle"])
                  if t["tweet_id"] not in paid and t["views"] and t["created_at"] >= cutoff][:30]
        if len(recent) >= 3:
            eng = [t["likes"] + t["retweets"] + t["quotes"] + t["replies"] + t["bookmarks"] for t in recent]
            base = {"median_views": int(statistics.median(t["views"] for t in recent)),
                    "median_likes": int(statistics.median(t["likes"] for t in recent)),
                    "median_eng": int(statistics.median(eng)),
                    "median_er": round(statistics.median(e / t["views"] * 100 for e, t in zip(eng, recent)), 3),
                    "sample_size": len(recent),
                    "baseline_note": f"last {len(recent)} own posts {recent[-1]['created_at'][:10]} → {recent[0]['created_at'][:10]}"}
        else:
            err = f"only {len(recent)} own posts with views in the last 90 days"
    except NeedsLogin:
        err = "median views need a logged-in X session (X_AUTH_TOKEN / X_CT0)"
    with _db_lock, db() as c:
        c.execute("""INSERT INTO creators(key, platform, handle) VALUES (?, 'x', ?) ON CONFLICT(key) DO NOTHING""", (key, prof["handle"]))
        c.execute("""UPDATE creators SET handle=?, name=?, user_id=?, avatar=?, followers=?, following=?, posts_count=?,
                     verified=?, profile_at=?, error=? WHERE key=?""",
                  (prof["handle"], prof["name"], prof["user_id"], prof["avatar"], prof["followers"], prof["following"],
                   prof["posts_count"], int(prof["verified"]), now(), err, key))
        if base:   # don't overwrite numbers someone entered by hand / via Claude with nothing
            c.execute(f"""UPDATE creators SET {", ".join(f"{k}=?" for k in base)}, baseline_source='x', baseline_at=?
                          WHERE key=?""", (*base.values(), now(), key))


def creators_view(enqueue_stale=True):
    """One row per creator across all waves: profile + baseline + how their paid posts did."""
    dv = lambda a, b: a / b if b else None  # noqa: E731
    r2 = lambda x, n=2: round(x, n) if x is not None else None  # noqa: E731
    with db() as c:
        names = {r["id"]: r["name"] for r in c.execute("SELECT id, name FROM campaigns")}
        prof = {r["key"]: dict(r) for r in c.execute("SELECT * FROM creators")}
        posts = [dict(r) for r in c.execute(f"SELECT * FROM posts WHERE author IS NOT NULL AND author != '' AND {INCLUDED}")]
    rows, stale = {}, []
    for p in posts:
        k = creator_key(p["platform"], p["author"])
        r = rows.setdefault(k, {"key": k, "platform": p["platform"] or "x", "author": p["author"], "waves": [], "posts": 0,
                                "paid": 0.0, "views": 0, "eng": 0, "eng_v": 0, "spend_v": 0.0, "post_urls": []})
        if names.get(p["campaign_id"]) not in r["waves"]:
            r["waves"].append(names.get(p["campaign_id"]))
        e = sum(p[m] for m in METRICS[1:])
        r["posts"] += 1
        r["paid"] += cost(p)
        r["views"] += p["views"]
        r["eng"] += e
        if p["views"]:
            r["eng_v"] += e
            r["spend_v"] += cost(p)
        r["post_urls"].append(p["url"])
    out = []
    for k, r in rows.items():
        pr = prof.get(k, {})
        avg_views = dv(r["views"], r["posts"]) if r["views"] else None   # avg views per paid post
        f = pr.get("followers")
        out.append({
            "key": k, "platform": r["platform"], "handle": pr.get("handle") or r["author"], "name": pr.get("name") or r["author"],
            "avatar": pr.get("avatar"), "verified": bool(pr.get("verified")), "followers": f,
            "median_views": pr.get("median_views"), "median_er": pr.get("median_er"), "median_likes": pr.get("median_likes"),
            "sample_size": pr.get("sample_size"), "baseline_source": pr.get("baseline_source"), "baseline_note": pr.get("baseline_note"),
            "profile_error": pr.get("error"), "profile_at": pr.get("profile_at"),
            "waves": r["waves"], "posts": r["posts"], "paid": r["paid"], "views": r["views"], "engagements": r["eng"],
            "avg_views": r2(avg_views, 0), "er_pct": r2(dv(r["eng_v"] * 100, r["views"])),
            "cpm": r2(dv(r["spend_v"] * 1000, r["views"])), "cpe": r2(dv(r["paid"], r["eng"]) if r["paid"] else None),
            "price_per_1k_followers": r2(dv(dv(r["paid"], r["posts"]) * 1000, f) if r["paid"] and f else None),
            "reach_pct": r2(dv(avg_views * 100, f) if avg_views and f else None),
            "vs_median": r2(dv(avg_views, pr.get("median_views")) if avg_views else None),
            "post_urls": r["post_urls"],
        })
        fresh = pr.get("profile_at") and (datetime.now(timezone.utc) - datetime.fromisoformat(pr["profile_at"])).total_seconds() < CREATOR_TTL_H * 3600
        if r["platform"] == "x" and not fresh:
            stale.append((k, r["author"]))
    if enqueue_stale:
        for k, h in stale:
            enqueue(k, k, "creator", h)
    with _pending_lock:
        pend = [r["key"] for r in out if r["key"] in _pending]
    return {"creators": out, "pending": pend, "x_login": bool(os.environ.get("X_AUTH_TOKEN") and os.environ.get("X_CT0"))}


def set_creator_stats(key, fields, source="manual"):
    allowed = {"followers", "median_views", "median_likes", "median_eng", "median_er", "sample_size", "baseline_note", "name"}
    fields = {k: v for k, v in fields.items() if k in allowed and v is not None}
    if not fields:
        raise ValueError("nothing to set")
    platform, _, handle = key.partition(":")
    with _db_lock, db() as c:
        c.execute("INSERT INTO creators(key, platform, handle) VALUES (?,?,?) ON CONFLICT(key) DO NOTHING", (key, platform, handle))
        extra = ", baseline_source=?, baseline_at=?" if set(fields) & {"median_views", "median_er", "median_likes", "median_eng"} else ""
        c.execute(f"UPDATE creators SET {', '.join(f'{k}=?' for k in fields)}{extra} WHERE key=?",
                  (*fields.values(), *((source, now()) if extra else ()), key))
    return {"key": key, **fields}


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
        days = wave_days(c, campaign_id, user_days(c))
        n_users = c.execute("SELECT count(*) FROM users").fetchone()[0]
    with _pending_lock:
        pending = [p["id"] for p in posts if p["id"] in _pending]
    return {"campaigns": campaigns, "posts": posts, "days": days, "pending": pending,
            "users_total": n_users}


# ---------------------------------------------------------------- users → waves

def _num(v):
    s = str(v or "0").replace(",", "").replace("$", "").strip()
    mult = {"K": 1e3, "M": 1e6, "B": 1e9}
    try:
        return float(s[:-1]) * mult[s[-1].upper()] if s and s[-1].upper() in mult else float(s or 0)
    except ValueError:
        return 0.0


_DATE_FORMATS = ["%b %d, %Y, %I:%M %p", "%b %d, %Y %I:%M %p", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
                 "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M", "%m/%d/%Y %I:%M %p", "%Y-%m-%d", "%m/%d/%Y"]


def _parse_time(s, tz):
    s = (s or "").strip()
    if not s:
        return None
    try:
        t = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        t = None
        for f in _DATE_FORMATS:
            try:
                t = datetime.strptime(s, f)
                break
            except ValueError:
                pass
    if t is None:
        # Newer exports drop the year ("Oct 9, 3:56 AM"): take the most recent year that isn't in the future
        for f in ("%b %d, %I:%M %p", "%b %d %I:%M %p"):
            try:
                t = datetime.strptime(f"{datetime.now(tz).year} {s}", "%Y " + f)
                break
            except ValueError:
                pass
        if t is None:
            return None
        if t.replace(tzinfo=tz) > datetime.now(tz) + timedelta(days=1):
            t = t.replace(year=t.year - 1)
    if t.tzinfo is None:
        t = t.replace(tzinfo=tz)
    return t.astimezone(timezone.utc)


def import_users(csv_text, tz_name="America/Los_Angeles"):
    """Upsert users from a signup export. Columns are matched by name, so other export shapes work too."""
    tz = ZoneInfo(tz_name) if ZoneInfo else timezone.utc
    rows = list(csv.DictReader(io.StringIO(csv_text.lstrip("\ufeff"))))
    if not rows:
        raise ValueError("CSV has no rows")
    # Normalise header names so "Signed up" / "signed_up" and "Requests (all)" / "requests_all" match alike
    cols = {re.sub(r"[^a-z0-9]+", "_", k.lower()).strip("_"): k for k in rows[0].keys() if k}

    def col(*names):
        return next((cols[n] for n in names if n in cols), None)
    c_email = col("email", "e-mail", "user_email")
    c_time = col("signed_up", "signup", "signed_up_at", "created_at", "created", "registered", "signup_date", "date")
    if not c_email or not c_time:
        raise ValueError(f"Need an email and a signup-time column; got: {', '.join(cols.values())}")
    c_keys, c_req = col("keys", "api_keys"), col("requests_all", "total_requests", "requests")
    # "Requests" is the last-7-days count when an all-time column exists next to it
    c_req7 = col("requests_7d") or (cols.get("requests") if c_req != cols.get("requests") else None)
    c_credit, c_method, c_ref = col("credit"), col("sign_in_method", "sign_in"), col("ref", "referral", "utm_source", "referrer")
    ok, bad = 0, 0
    with _db_lock, db() as c:
        for r in rows:
            email, t = (r.get(c_email) or "").strip().lower(), _parse_time(r.get(c_time), tz)
            if not email or not t:
                bad += 1
                continue
            c.execute("""INSERT INTO users(email, signed_up, keys, requests_all, requests_7d, credit, unconfirmed, ref, imported_at)
                         VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(email) DO UPDATE SET signed_up=excluded.signed_up,
                         keys=excluded.keys, requests_all=excluded.requests_all, requests_7d=excluded.requests_7d,
                         credit=excluded.credit, unconfirmed=excluded.unconfirmed, ref=COALESCE(excluded.ref, users.ref),
                         imported_at=excluded.imported_at""",
                      (email, t.isoformat(), int(_num(r.get(c_keys))) if c_keys else 0,
                       _num(r.get(c_req)) if c_req else 0, _num(r.get(c_req7)) if c_req7 else 0,
                       r.get(c_credit) if c_credit else None,
                       int("unconfirmed" in (r.get(c_method) or "").lower()) if c_method else 0,
                       (r.get(c_ref) or None) if c_ref else None, now()))
            ok += 1
        total = c.execute("SELECT count(*) FROM users").fetchone()[0]
        waves = {}
        for cid, rows_ in (user_days(c) or {}).items():
            waves[cid] = sum(d["registrations"] for d in rows_)
        names = {r["id"]: r["name"] for r in c.execute("SELECT id, name FROM campaigns")}
    return {"imported": ok, "skipped_rows": bad, "users_total": total, "timezone": tz_name,
            "registrations_by_wave": {names[k]: v for k, v in waves.items()}}


def wave_starts(c):
    """[(campaign_id, first_post_utc)] for campaigns that have fetched posts, oldest first."""
    rows = c.execute("""SELECT campaign_id, min(created_at) FROM posts WHERE created_at IS NOT NULL AND created_at != ''
                        GROUP BY campaign_id ORDER BY min(created_at)""").fetchall()
    return [(r[0], datetime.fromisoformat(r[1])) for r in rows]


def assign_waves(c):
    """{email: campaign_id}. A user belongs to the latest wave whose first post came before their signup;
    users from before the first wave count toward the first wave."""
    starts = wave_starts(c)
    if not starts:
        return {}
    out = {}
    for u in c.execute("SELECT email, signed_up FROM users"):
        t = datetime.fromisoformat(u[1])
        cid = starts[0][0]
        for wid, st in starts:
            if t >= st:
                cid = wid
        out[u[0]] = cid
    return out


def user_days(c):
    """Daily registrations / API users per wave from the users table, or None if no users were imported."""
    if not c.execute("SELECT 1 FROM users LIMIT 1").fetchone():
        return None
    wave = assign_waves(c)
    agg = {}
    for u in c.execute("SELECT email, signed_up, requests_all, credit FROM users"):
        cid = wave.get(u[0])
        if cid is None:
            continue
        d = agg.setdefault(cid, {}).setdefault(u[1][:10], {"day": u[1][:10], "registrations": 0, "api_users": 0, "flagged": 0})
        d["registrations"] += 1
        d["api_users"] += u[2] > 0
        d["flagged"] += (u[3] or "") in ("Credit voided", "Credit withheld")
    return {cid: sorted(v.values(), key=lambda d: d["day"]) for cid, v in agg.items()}


def wave_days(c, campaign_id, by_users):
    """Daily registrations / API users for a wave = users from the signup CSV + numbers typed in by hand
    (the `days` table), added together. Each row says how much of it was manual so the UI can edit/remove it."""
    merged = {}
    for d in (by_users or {}).get(campaign_id, []):
        merged[d["day"]] = {**d, "csv_registrations": d["registrations"], "csv_api_users": d["api_users"],
                            "manual_registrations": 0, "manual_api_users": 0}
    for r in c.execute("SELECT day, registrations, api_users FROM days WHERE campaign_id=?", (campaign_id,)):
        d = merged.setdefault(r[0], {"day": r[0], "registrations": 0, "api_users": 0, "flagged": 0,
                                     "csv_registrations": 0, "csv_api_users": 0})
        d["registrations"] += r[1] or 0
        d["api_users"] += r[2] or 0
        d["manual_registrations"], d["manual_api_users"] = r[1] or 0, r[2] or 0
    return sorted(merged.values(), key=lambda d: d["day"])


def drop_manual_days_duplicating_csv():
    """Manual day rows typed in before a signup CSV was imported would now be counted twice (manual numbers are
    added on top of the CSV). Remove the ones that exactly match what the CSV gives for that wave and day."""
    with _db_lock, db() as c:
        by_users = user_days(c)
        if not by_users:
            return 0
        csv_rows = {(cid, d["day"]): (d["registrations"], d["api_users"]) for cid, rows in by_users.items() for d in rows}
        dupes = [(r[0], r[1]) for r in c.execute("SELECT campaign_id, day, registrations, api_users FROM days")
                 if csv_rows.get((r[0], r[1])) == (r[2], r[3])]
        c.executemany("DELETE FROM days WHERE campaign_id=? AND day=?", dupes)
    if dupes:
        print(f"removed {len(dupes)} manual day rows that duplicated the signup CSV", flush=True)
    return len(dupes)


def report(campaign_ids=None, top=5):
    """Dashboard numbers per campaign + creators ranked by average of CPM rank and CPE rank."""
    r2 = lambda x: round(x, 2) if x is not None else None  # noqa: E731
    dv = lambda a, b: a / b if b else None                 # noqa: E731
    out = []
    with db() as c:
        camps = [dict(r) for r in c.execute("SELECT * FROM campaigns ORDER BY id")]
        by_users = user_days(c)
        for camp in camps:
            if campaign_ids and camp["id"] not in campaign_ids:
                continue
            # Excluded posts stay in the wave's list but don't count toward any total
            posts = [dict(r) for r in c.execute(f"SELECT * FROM posts WHERE campaign_id=? AND {INCLUDED}", (camp["id"],))]
            n_excluded = c.execute("SELECT count(*) FROM posts WHERE campaign_id=? AND excluded=1", (camp["id"],)).fetchone()[0]
            eng = lambda p: sum(p[k] for k in METRICS[1:])  # noqa: E731
            days = wave_days(c, camp["id"], by_users)
            x_spend = sum(cost(p) for p in posts)          # all paid posts, any platform
            total = x_spend + (camp["other_spend"] or 0)
            spend_v = sum(cost(p) for p in posts if p["views"])   # CPM only over posts with views
            spend_e = sum(cost(p) for p in posts if eng(p))
            views = sum(p["views"] for p in posts)
            e_all, e_v = sum(eng(p) for p in posts), sum(eng(p) for p in posts if p["views"])
            regs = sum(d["registrations"] for d in days)
            api = sum(d["api_users"] for d in days)
            creators = {}
            for p in posts:
                a = creators.setdefault(p["author"] or p["url"], {"creator": p["author"] or p["url"], "platform": p["platform"] or "x",
                                                                    "posts": 0, "cost": 0, "views": 0, "engagements": 0})
                a["posts"] += 1
                a["cost"] += cost(p)
                a["views"] += p["views"]
                a["engagements"] += eng(p)
            paid = [a for a in creators.values() if a["cost"] > 0 and a["views"] > 0]
            for a in paid:
                a["er_pct"] = r2(a["engagements"] / a["views"] * 100)
                a["cpm"] = r2(a["cost"] / a["views"] * 1000)
                a["cpe"] = r2(dv(a["cost"], a["engagements"]))
            by_cpm = sorted(paid, key=lambda a: a["cpm"])
            by_cpe = sorted(paid, key=lambda a: a["cpe"] if a["cpe"] is not None else 1e9)
            for a in paid:
                a["rank_score"] = (by_cpm.index(a) + by_cpe.index(a)) / 2 + 1
            ranked = sorted(paid, key=lambda a: (a["rank_score"], a["cpm"]))
            out.append({
                "id": camp["id"], "name": camp["name"], "posts": len(posts), "excluded_posts": n_excluded,
                "first_post_utc": min((p["created_at"] for p in posts if p["created_at"]), default=None),
                "posts_spend": x_spend, "by_platform": {pl: sum(cost(p) for p in posts if (p["platform"] or "x") == pl)
                                                        for pl in sorted({p["platform"] or "x" for p in posts})},
                "other_spend": camp["other_spend"] or 0, "total_budget": total,
                "avg_cost_per_post": r2(dv(x_spend, len(posts))),
                "views": views, "engagements": e_all, "er_pct": r2(dv(e_v * 100, views)),
                "avg_cpm": r2(dv(spend_v * 1000, views)), "avg_cpe": r2(dv(spend_e, e_all)),
                "registrations": regs, "api_users": api,
                "cost_per_registration": r2(dv(total, regs)), "cost_per_api_user": r2(dv(total, api)),
                "reg_to_api_pct": r2(dv(api * 100, regs)), "regs_per_1k_views": r2(dv(regs * 1000, views)),
                "flagged_users": sum(d.get("flagged", 0) for d in days),
                "_sums": {"spend_v": spend_v, "spend_e": spend_e, "eng_v": e_v},   # for cross-wave totals
                "daily": days,
                "top": ranked[:top], "bottom": ranked[::-1][:top] if top else [],
                "missing_views": [a["creator"] for a in creators.values() if a["cost"] and not a["views"]],
                "unpriced": [{"creator": a["creator"], "views": a["views"]} for a in creators.values() if not a["cost"]],
                "flags": [f"{a['creator']}: ER {a['er_pct']}% on {a['views']:,} views — check view quality"
                          for a in paid if a["er_pct"] < 0.3 and a["views"] > 5000],
            })
    live = [c for c in out if c["posts"] or c["registrations"]]          # empty waves don't count toward totals
    tot = {k: sum(c[k] for c in live) for k in ("posts", "posts_spend", "other_spend", "total_budget", "views",
                                                  "engagements", "registrations", "api_users", "flagged_users")}
    sv, se, ev = (sum(c["_sums"][k] for c in live) for k in ("spend_v", "spend_e", "eng_v"))
    tot.update({"name": "All waves", "er_pct": r2(dv(ev * 100, tot["views"])), "avg_cpm": r2(dv(sv * 1000, tot["views"])),
                "avg_cpe": r2(dv(se, tot["engagements"])), "avg_cost_per_post": r2(dv(tot["posts_spend"], tot["posts"])),
                "cost_per_registration": r2(dv(tot["total_budget"], tot["registrations"])),
                "cost_per_api_user": r2(dv(tot["total_budget"], tot["api_users"])),
                "reg_to_api_pct": r2(dv(tot["api_users"] * 100, tot["registrations"])),
                "regs_per_1k_views": r2(dv(tot["registrations"] * 1000, tot["views"])),
                "by_platform": {pl: sum(c["by_platform"].get(pl, 0) for c in live) for pl in {p for c in live for p in c["by_platform"]}}})
    for c in out:
        c.pop("_sums", None)
    return {"campaigns": out, "totals": tot, "registrations_source": "users" if by_users is not None else "manual",
            "ranking": "rank_score = average of CPM rank and CPE rank within the wave (lower is better). "
                       "Per-creator registrations are not tracked unless users have a ref column."}


def export_csv(campaign_id):
    s = state(campaign_id)
    buf = io.StringIO()
    w = csv.writer(buf)
    camp = next(c for c in s["campaigns"] if c["id"] == campaign_id)
    x_spend = sum(cost(p) for p in s["posts"] if not p["excluded"])
    w.writerow(["posts_spend", "other_spend", "other_note", "total_budget"])
    w.writerow([x_spend, camp["other_spend"] or 0, camp["other_note"] or "", x_spend + (camp["other_spend"] or 0)])
    w.writerow([])
    w.writerow(["day", "platform", "url", "author", "note", "budget", "excluded_from_total", *METRICS, "engagements", "er_pct", "cpm", "cpe"])
    for p in s["posts"]:
        eng = sum(p[k] for k in METRICS[1:])
        v, b = p["views"], cost(p)
        w.writerow([p["day"], p["platform"] or "x", p["url"], p["author"], p["note"] or "", p["budget"] or 0, "yes" if p["excluded"] else "", *[p[k] for k in METRICS], eng,
                    round(eng / v * 100, 3) if v else "", round(b / v * 1000, 2) if v else "",
                    round(b / eng, 3) if eng else ""])
    w.writerow([])
    w.writerow(["day", "spend", "views", "engagements", "registrations", "api_users",
                "cost_per_registration", "cost_per_api_user", "reg_to_api_pct"])
    by_day = {}
    for p in (p for p in s["posts"] if not p["excluded"]):
        d = by_day.setdefault(p["day"] or "", {"spend": 0, "views": 0, "eng": 0, "reg": 0, "api": 0})
        d["spend"] += cost(p)
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
        payload = auth.unsign(tok) if tok else None
        return payload if payload and "t" not in payload else None   # "t" marks OAuth codes/tokens

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
            nxt = parse_qs(u.query).get("next", ["/"])[0]
            nxt = nxt if nxt.startswith("/") and not nxt.startswith("//") else "/"
            self._redirect(auth.login_url(redirect_uri, state_),
                           [self._set_cookie("oauth_state", auth.sign({"s": state_, "n": nxt, "exp": time.time() + 600}), 600)])
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
            self._redirect(st.get("n") or "/", [self._set_cookie("session", auth.session_token(user), auth.SESSION_DAYS * 86400),
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
        if u.path.startswith("/.well-known/oauth-protected-resource"):
            return self._send(200, mcp_server.resource_metadata(self._base_url()))
        if u.path.startswith("/.well-known/oauth-authorization-server"):
            return self._send(200, mcp_server.as_metadata(self._base_url()))
        if u.path == "/mcp":
            return self._send(405, {"error": "use POST"}, headers=[("Allow", "POST")])
        if u.path == "/oauth/authorize":
            q = parse_qs(u.query)
            client, err = mcp_server.check_authorize(q)
            if err:
                return self._send(400, err, "text/plain; charset=utf-8")
            user = self._user()
            if not user:
                return self._redirect("/auth/login?next=" + quote(self.path, safe=""))
            form_token = auth.sign({"t": "consent", "u": user["email"], "c": q["client_id"][0], "exp": time.time() + 600})
            return self._send(200, mcp_server.consent_page(client, q, user, form_token), "text/html; charset=utf-8")
        if auth.ENABLED and self._auth_routes(u):
            return
        user = self._require_user(u)
        if not user:
            return
        q = parse_qs(u.query)
        if u.path in ("/", "/index.html"):
            with open(os.path.join(HERE, "static", "index.html"), "rb") as f:
                return self._send(200, f.read(), "text/html; charset=utf-8")
        if u.path == "/totals":
            with open(os.path.join(HERE, "static", "totals.html"), "rb") as f:
                return self._send(200, f.read(), "text/html; charset=utf-8")
        if u.path == "/influencers":
            with open(os.path.join(HERE, "static", "influencers.html"), "rb") as f:
                return self._send(200, f.read(), "text/html; charset=utf-8")
        if u.path == "/api/creators":
            return self._send(200, creators_view())
        if u.path == "/api/report":
            return self._send(200, report(None, top=int(parse_qs(u.query).get("top", ["0"])[0])))
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
        if u.path == "/mcp":
            email = mcp_server.bearer_user(self.headers.get("Authorization") or "") if auth.ENABLED else "local"
            if not email:
                meta = self._base_url() + "/.well-known/oauth-protected-resource"
                return self._send(401, {"error": "unauthorized"},
                                  headers=[("WWW-Authenticate", f'Bearer resource_metadata="{meta}"')])
            code, payload = mcp_server.handle_rpc(sys.modules[__name__], email, self._body_bytes())
            if payload is None:
                self.send_response(code)
                self.send_header("Content-Length", "0")
                return self.end_headers()
            return self._send(code, payload)
        if u.path == "/oauth/register":
            return self._send(*mcp_server.register(self._body_bytes()))
        if u.path == "/oauth/token":
            return self._send(*mcp_server.token(self._body_bytes()))
        if u.path == "/oauth/authorize":
            f = parse_qs(self._body_bytes().decode())
            user, client, err = self._user(), *mcp_server.check_authorize(f)
            tok = auth.unsign(f.get("form_token", [""])[0])
            if err or not user or not tok or tok.get("t") != "consent" or tok["u"] != user["email"] or tok["c"] != f["client_id"][0]:
                return self._send(400, err or "Authorization expired — try connecting again.", "text/plain; charset=utf-8")
            if f.get("decision", [""])[0] != "allow":
                ru = f["redirect_uri"][0]
                return self._redirect(ru + ("&" if "?" in ru else "?") + urlencode(
                    {"error": "access_denied", **({"state": f["state"][0]} if f.get("state") else {})}))
            print(f"mcp authorized: {user['email']} -> {client['n']}", flush=True)
            return self._redirect(mcp_server.issue_code(f, user))
        user = self._require_user(u)
        if not user:
            return
        path = u.path
        if path == "/api/creators/update":
            b = json.loads(self._body_bytes() or b"{}")
            try:
                return self._send(200, set_creator_stats(b.pop("key"), b, "manual"))
            except (ValueError, KeyError) as e:
                return self._send(400, {"error": str(e)})
        if path == "/api/creators/refresh":
            v = creators_view(enqueue_stale=False)
            for r in v["creators"]:
                if r["platform"] == "x":
                    enqueue(r["key"], r["key"], "creator", r["handle"])
            return self._send(200, {"queued": sum(r["platform"] == "x" for r in v["creators"])})
        if path == "/api/users/import":
            tz = parse_qs(u.query).get("tz", ["America/Los_Angeles"])[0]
            try:
                res = import_users(self._body_bytes().decode("utf-8", "replace"), tz)
                drop_manual_days_duplicating_csv()
                return self._send(200, res)
            except ValueError as e:
                return self._send(400, {"error": str(e)})
        if path == "/api/restore":
            try:
                with _db_lock:
                    restore(self._body_bytes())
            except ValueError as e:
                return self._send(400, {"error": str(e)})
            print(f"restore by {user['email']}", flush=True)
            drop_manual_days_duplicating_csv()
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
                    found = platforms.detect(raw)
                    if not found:
                        invalid.append(raw)
                        continue
                    plat, key, url = found
                    cur = c.execute("INSERT OR IGNORE INTO posts(campaign_id, url, tweet_id, budget, note, platform) VALUES (?,?,?,?,?,?)",
                                    (cid, url, key, budget, note, plat))
                    if not cur.rowcount:
                        # Already tracked: update its cost/note from the new paste
                        c.execute("UPDATE posts SET budget=?, note=COALESCE(?, note) WHERE campaign_id=? AND tweet_id=?",
                                  (budget, note, cid, key))
                        skipped += 1
                        continue
                    added += 1
                    to_fetch.append({"id": cur.lastrowid, "tweet_id": key, "platform": plat, "url": url})
                result = {"added": added, "skipped": skipped, "invalid": invalid}

            elif path == "/api/posts/update":
                row = c.execute("SELECT platform FROM posts WHERE id=?", (b["id"],)).fetchone()
                typed = [k for k in METRICS if k not in platforms.AUTO_METRICS[(row["platform"] if row else None) or "x"]]
                if "excluded" in b:
                    b["excluded"] = 1 if b["excluded"] else 0
                fields = {k: b[k] for k in ("budget", "day", "note", "excluded", *typed) if k in b}
                if fields:
                    c.execute(f"UPDATE posts SET {', '.join(f'{k}=?' for k in fields)} WHERE id=?",
                              (*fields.values(), b["id"]))

            elif path == "/api/posts/pack":
                try:
                    result = apply_pack(c, b["campaign_id"], b.get("ids"), float(b["total"]), b.get("note"))
                except (ValueError, KeyError, TypeError) as e:
                    return self._send(400, {"error": str(e)})

            elif path == "/api/posts/delete":
                c.execute("DELETE FROM posts WHERE id=?", (b["id"],))

            elif path == "/api/refresh":
                ids = set(b.get("ids") or [])
                rows = c.execute("SELECT id, tweet_id, platform, url FROM posts WHERE campaign_id=?", (b["campaign_id"],)).fetchall()
                to_fetch = [r for r in rows if (not ids or r["id"] in ids) and (r["platform"] or "x") in ("x", "linkedin")]
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

        enqueue_rows(to_fetch)                 # after commit, so the worker sees the rows
        self._send(200, result)


if __name__ == "__main__":
    auth.check_config()
    init_db()
    drop_manual_days_duplicating_csv()
    threading.Thread(target=fetch_worker, daemon=True).start()
    threading.Thread(target=backup_worker, daemon=True).start()
    print(f"X campaign analyzer → http://{HOST}:{PORT}  (data: {DB_PATH}, login: {'Google @' + auth.ALLOWED_DOMAIN if auth.ENABLED else 'OFF'})", flush=True)
    ThreadingHTTPServer((HOST, PORT), Handler).serve_forever()
