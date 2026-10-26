"""Automated X DM outreach to creators.

A campaign = one sending X account + a first message + an optional follow-up, with a daily cap and a send window.
Adding creators to a campaign drafts their first DM from the template. Nothing is sent until a teammate approves
it in the Approval queue; approved DMs then go out on their own, spaced out, within the window and the daily cap.
The worker also drafts follow-ups for people who didn't answer (these need approval too) and polls the sending
account's inbox, so a reply stops the sequence for that person and shows up on the page.

Tables: dm_accounts, dm_campaigns, dm_targets (one creator in one campaign), dm_messages, dm_inbox.
"""
import json
import random
import re
import threading
import time
from datetime import datetime, timedelta, timezone

import x_dm

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None

POLL_EVERY = 8 * 3600         # seconds between inbox reads per account (each event read is billed)
GAP = (120, 300)              # seconds between two sends from one account, picked at random
SIM_ID = "sim"                # demo sending account: runs the whole flow, never calls X
SIM_GAP = (8, 15)             # the demo account sends fast so the flow can be watched
REPLY_WINDOW_DAYS = 30        # keep polling an account while it messaged someone this recently

_db = _lock = _now = None
_wake = threading.Event()


def setup(db, lock, now):
    """Wire in the server's connection factory, write lock and clock."""
    global _db, _lock, _now
    _db, _lock, _now = db, lock, now


def init_tables(c):
    c.executescript("""
    CREATE TABLE IF NOT EXISTS dm_accounts (
        user_id TEXT PRIMARY KEY, handle TEXT, name TEXT, access_token TEXT, refresh_token TEXT, expires_at INTEGER,
        scope TEXT, daily_cap INTEGER DEFAULT 40, connected_by TEXT, connected_at TEXT, error TEXT,
        next_send_at REAL DEFAULT 0, rate_until REAL DEFAULT 0, last_poll REAL DEFAULT 0, last_event_id TEXT);
    CREATE TABLE IF NOT EXISTS dm_campaigns (
        id INTEGER PRIMARY KEY, name TEXT NOT NULL, account_id TEXT, message TEXT NOT NULL DEFAULT '',
        followup TEXT DEFAULT '', followup_days INTEGER DEFAULT 4, max_followups INTEGER DEFAULT 1,
        daily_limit INTEGER DEFAULT 20, send_start INTEGER DEFAULT 9, send_end INTEGER DEFAULT 19,
        tz TEXT DEFAULT 'America/New_York', status TEXT DEFAULT 'paused',
        created_at TEXT, created_by TEXT, updated_at TEXT);
    CREATE TABLE IF NOT EXISTS dm_targets (
        id INTEGER PRIMARY KEY, campaign_id INTEGER NOT NULL REFERENCES dm_campaigns(id) ON DELETE CASCADE,
        creator_key TEXT NOT NULL, handle TEXT, x_user_id TEXT,
        stage TEXT DEFAULT 'drafted',      -- drafted · approved · sent · followup_drafted · replied · failed · stopped · done
        followups_sent INTEGER DEFAULT 0, last_sent_at TEXT, next_due_at TEXT, replied_at TEXT, reply_text TEXT,
        conversation_id TEXT, error TEXT, added_at TEXT, added_by TEXT, UNIQUE(campaign_id, creator_key));
    CREATE TABLE IF NOT EXISTS dm_messages (
        id INTEGER PRIMARY KEY, target_id INTEGER NOT NULL REFERENCES dm_targets(id) ON DELETE CASCADE,
        campaign_id INTEGER NOT NULL, kind TEXT DEFAULT 'first', body TEXT NOT NULL,
        status TEXT DEFAULT 'pending',     -- pending (needs approval) · approved · sent · failed · skipped
        created_at TEXT, approved_by TEXT, approved_at TEXT, sent_at TEXT, account_id TEXT, dm_event_id TEXT, error TEXT);
    CREATE INDEX IF NOT EXISTS dm_messages_status ON dm_messages(status);
    CREATE TABLE IF NOT EXISTS dm_inbox (
        event_id TEXT PRIMARY KEY, account_id TEXT, conversation_id TEXT, sender_id TEXT, text TEXT, created_at TEXT,
        target_id INTEGER, inbound INTEGER);
    """)
    for table, col, decl in (("dm_campaigns", "plan_id", "INTEGER"),   # made from this wave-builder plan (kept in sync)
                             ("dm_targets", "price", "REAL")):          # offer for {price}: plan price, else their quote
        if col not in [r[1] for r in c.execute(f"PRAGMA table_info({table})")]:
            c.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")


def _rows(c, sql, args=()):
    return [dict(r) for r in c.execute(sql, args)]


# ---------------------------------------------------------------- templates

FIELDS = ("first_name", "name", "handle", "price")

DEFAULT_MESSAGE = ("Hi {first_name}! I'm Dimash from Nace.ai. We build AI agents for finance and accounting teams, "
                   "and your audience looks like a great fit.\n\nWould you be open to a paid post about us? "
                   "Budget-wise we were thinking {price}. Happy to adapt to your format.")
DEFAULT_FOLLOWUP = "Hey {first_name}, just bumping this in case it got buried. Happy to share details."


def render(template, creator):
    name = (creator.get("name") or creator.get("handle") or "").strip().lstrip("@")
    first = re.split(r"[\s|·,(]", name, 1)[0] if name else ""
    if not first or not re.search(r"[A-Za-z]", first):
        first = "there"
    price = creator.get("price")
    vals = {"first_name": first, "name": name or "there", "handle": "@" + (creator.get("handle") or "").lstrip("@"),
            "price": f"${price:,.0f}" if price else "your usual rate"}
    return re.sub(r"\{(\w+)\}", lambda m: vals.get(m.group(1), m.group(0)), template or "").strip()


# ---------------------------------------------------------------- accounts

def save_account(tok, email):
    with _lock, _db() as c:
        c.execute("""INSERT INTO dm_accounts(user_id, handle, name, access_token, refresh_token, expires_at, scope,
                         connected_by, connected_at, error) VALUES (?,?,?,?,?,?,?,?,?,NULL)
                     ON CONFLICT(user_id) DO UPDATE SET handle=excluded.handle, name=excluded.name,
                         access_token=excluded.access_token, refresh_token=excluded.refresh_token,
                         expires_at=excluded.expires_at, scope=excluded.scope, connected_by=excluded.connected_by,
                         connected_at=excluded.connected_at, error=NULL""",
                  (tok["user_id"], tok["handle"], tok.get("name"), tok["access_token"], tok.get("refresh_token"),
                   tok["expires_at"], tok.get("scope"), email, _now()))


def add_sim_account(email):
    """A pretend sending account for trying the flow before the X API is set up. Its sends never reach X."""
    with _lock, _db() as c:
        c.execute("""INSERT OR IGNORE INTO dm_accounts(user_id, handle, name, daily_cap, connected_by, connected_at)
                     VALUES (?, 'demo_sender', 'Demo account (simulated)', 40, ?, ?)""", (SIM_ID, email, _now()))
    return {"user_id": SIM_ID}


def remove_account(user_id):
    with _lock, _db() as c:
        c.execute("UPDATE dm_campaigns SET status='paused' WHERE account_id=?", (user_id,))
        c.execute("DELETE FROM dm_accounts WHERE user_id=?", (user_id,))
    return {"removed": user_id}


def set_account_cap(user_id, cap):
    with _lock, _db() as c:
        c.execute("UPDATE dm_accounts SET daily_cap=? WHERE user_id=?", (max(1, min(int(cap), 500)), user_id))
    return {"ok": True}


def _access_token(acct):
    """Fresh access token for an account row, refreshing (and saving) it when it's about to expire."""
    if acct["expires_at"] and acct["expires_at"] > time.time():
        return acct["access_token"]
    if not acct["refresh_token"]:
        raise x_dm.DMError("X login expired: reconnect the account", "auth")
    tok = x_dm.refresh(acct["refresh_token"])
    with _lock, _db() as c:
        c.execute("UPDATE dm_accounts SET access_token=?, refresh_token=COALESCE(?, refresh_token), expires_at=? WHERE user_id=?",
                  (tok["access_token"], tok["refresh_token"], tok["expires_at"], acct["user_id"]))
    acct.update(tok)
    return tok["access_token"]


# ---------------------------------------------------------------- campaigns

CAMPAIGN_FIELDS = {"name": str, "account_id": str, "message": str, "followup": str, "followup_days": int,
                   "max_followups": int, "daily_limit": int, "send_start": int, "send_end": int, "tz": str}


def save_campaign(b, email):
    vals = {}
    for k, cast in CAMPAIGN_FIELDS.items():
        if k in b and b[k] is not None:
            vals[k] = cast(b[k]) if b[k] != "" or cast is str else None
    if "tz" in vals and ZoneInfo:
        ZoneInfo(vals["tz"])                              # raises on an unknown zone
    for k, lo, hi in (("send_start", 0, 23), ("send_end", 1, 24), ("daily_limit", 1, 500), ("max_followups", 0, 3),
                      ("followup_days", 1, 30)):
        if vals.get(k) is not None:
            vals[k] = max(lo, min(hi, vals[k]))
    with _lock, _db() as c:
        if b.get("id"):
            if vals:
                c.execute(f"UPDATE dm_campaigns SET {', '.join(k + '=?' for k in vals)}, updated_at=? WHERE id=?",
                          (*vals.values(), _now(), int(b["id"])))
            return {"id": int(b["id"])}
        if not (vals.get("name") or "").strip():
            raise ValueError("name the campaign")
        cid = c.execute(f"INSERT INTO dm_campaigns({', '.join(vals)}, created_at, created_by, updated_at) "
                        f"VALUES ({', '.join('?' * len(vals))}, ?, ?, ?)", (*vals.values(), _now(), email, _now())).lastrowid
    return {"id": cid}


def set_status(campaign_id, status):
    if status not in ("active", "paused"):
        raise ValueError("status is active or paused")
    with _lock, _db() as c:
        camp = c.execute("SELECT * FROM dm_campaigns WHERE id=?", (int(campaign_id),)).fetchone()
        if not camp:
            raise ValueError("campaign not found")
        if status == "active":
            if not camp["account_id"] or not c.execute("SELECT 1 FROM dm_accounts WHERE user_id=?", (camp["account_id"],)).fetchone():
                raise ValueError("pick a connected X account to send from first")
            if not (camp["message"] or "").strip():
                raise ValueError("write the first message first")
        c.execute("UPDATE dm_campaigns SET status=?, updated_at=? WHERE id=?", (status, _now(), int(campaign_id)))
    _wake.set()
    return {"status": status}


def stop_all():
    with _lock, _db() as c:
        n = c.execute("UPDATE dm_campaigns SET status='paused' WHERE status='active'").rowcount
    return {"paused": n}


def delete_campaign(campaign_id):
    with _lock, _db() as c:
        c.execute("PRAGMA foreign_keys=ON")
        c.execute("DELETE FROM dm_campaigns WHERE id=?", (int(campaign_id),))
    return {"deleted": int(campaign_id)}


def add_targets(campaign_id, keys, email):
    """Put creators in a campaign and draft their first DM (pending approval). Skips anyone already in it."""
    added, skipped = 0, []
    with _lock, _db() as c:
        camp = c.execute("SELECT * FROM dm_campaigns WHERE id=?", (int(campaign_id),)).fetchone()
        if not camp:
            raise ValueError("campaign not found")
        for k in keys:
            if _add_target(c, camp, k, None, email):
                added += 1
            else:
                skipped.append(k)
    return {"added": added, "skipped": skipped}


def _add_target(c, camp, key, price, email):
    """Insert one creator into a campaign with a drafted first DM. False if not an X creator or already in it."""
    cr = c.execute("SELECT key, handle, name, user_id, quote_price FROM creators WHERE key=?", (key,)).fetchone()
    if not cr or not key.startswith("x:"):
        return False
    if price is None and cr["quote_price"]:
        price = cr["quote_price"]
    try:
        tid = c.execute("""INSERT INTO dm_targets(campaign_id, creator_key, handle, x_user_id, price, added_at, added_by)
                           VALUES (?,?,?,?,?,?,?)""", (camp["id"], key, cr["handle"], cr["user_id"], price, _now(), email)).lastrowid
    except Exception:  # noqa: BLE001 — UNIQUE: already in this campaign
        return False
    c.execute("INSERT INTO dm_messages(target_id, campaign_id, kind, body, created_at) VALUES (?,?,?,?,?)",
              (tid, camp["id"], "first", render(camp["message"], {**dict(cr), "price": price}), _now()))
    return True


# ---------------------------------------------------------------- wave builder plans → campaigns

def sync_plan(plan_id, email=None):
    """Keep the outreach campaign of a wave-builder plan in step with the plan: created (paused) on first save,
    renamed with it, creators added → first DM drafted for approval, creators removed → dropped if not messaged yet,
    planned price changed → their waiting draft re-written. Sent DMs and replies are never touched."""
    with _lock, _db() as c:
        plan = c.execute("SELECT * FROM plans WHERE id=?", (int(plan_id),)).fetchone()
        if not plan:
            return None
        items = [i for i in json.loads(plan["items"] or "[]") if str(i.get("key", "")).startswith("x:")]
        camp = c.execute("SELECT * FROM dm_campaigns WHERE plan_id=?", (plan["id"],)).fetchone()
        if not camp:
            if not items:
                return None                      # nothing to DM yet
            acct = c.execute("SELECT user_id FROM dm_accounts ORDER BY user_id = ? , handle LIMIT 1", (SIM_ID,)).fetchone()
            cid = c.execute("""INSERT INTO dm_campaigns(name, plan_id, account_id, message, followup, created_at, created_by, updated_at)
                               VALUES (?,?,?,?,?,?,?,?)""", (plan["name"], plan["id"], acct[0] if acct else None, DEFAULT_MESSAGE,
                                                             DEFAULT_FOLLOWUP, _now(), email, _now())).lastrowid
            camp = c.execute("SELECT * FROM dm_campaigns WHERE id=?", (cid,)).fetchone()
        elif camp["name"] != plan["name"]:
            c.execute("UPDATE dm_campaigns SET name=?, updated_at=? WHERE id=?", (plan["name"], _now(), camp["id"]))
        want = {i["key"]: i.get("price") for i in items}
        have = {r["creator_key"]: dict(r) for r in c.execute("SELECT * FROM dm_targets WHERE campaign_id=?", (camp["id"],))}
        added = removed = repriced = 0
        for k, price in want.items():
            t = have.get(k)
            if not t:
                added += _add_target(c, camp, k, price, email)
                continue
            if price is None:                    # no plan price: fall back to their quote, as when they were added
                q = c.execute("SELECT quote_price FROM creators WHERE key=?", (k,)).fetchone()
                price = q[0] if q and q[0] else None
            if price != t["price"]:
                c.execute("UPDATE dm_targets SET price=? WHERE id=?", (price, t["id"]))
                cr = c.execute("SELECT handle, name FROM creators WHERE key=?", (k,)).fetchone()
                for m in c.execute("SELECT id, kind FROM dm_messages WHERE target_id=? AND status='pending'", (t["id"],)).fetchall():
                    tpl = camp["message"] if m["kind"] == "first" else camp["followup"]
                    c.execute("UPDATE dm_messages SET body=? WHERE id=?", (render(tpl, {**dict(cr or {}), "price": price}), m["id"]))
                repriced += 1
        for k, t in have.items():
            if k not in want and not t["last_sent_at"]:
                c.execute("DELETE FROM dm_messages WHERE target_id=?", (t["id"],))
                c.execute("DELETE FROM dm_targets WHERE id=?", (t["id"],))
                removed += 1
    return {"campaign_id": camp["id"], "added": added, "removed": removed, "repriced": repriced}


def plan_deleted(plan_id):
    """Plan removed in the builder: drop its campaign if nothing was sent, else keep the history and unlink it."""
    with _lock, _db() as c:
        camp = c.execute("SELECT id FROM dm_campaigns WHERE plan_id=?", (int(plan_id),)).fetchone()
        if not camp:
            return
        if c.execute("SELECT 1 FROM dm_targets WHERE campaign_id=? AND last_sent_at IS NOT NULL", (camp["id"],)).fetchone():
            c.execute("UPDATE dm_campaigns SET plan_id=NULL, status='paused' WHERE id=?", (camp["id"],))
        else:
            c.execute("DELETE FROM dm_messages WHERE campaign_id=?", (camp["id"],))
            c.execute("DELETE FROM dm_targets WHERE campaign_id=?", (camp["id"],))
            c.execute("DELETE FROM dm_campaigns WHERE id=?", (camp["id"],))


def plan_stats(c):
    """{plan_id: {campaign_id, status, people, pending, queued, sent, replied, failed}} for the builder."""
    out = {}
    for r in c.execute("""SELECT d.id, d.plan_id, d.status,
                              count(t.id) AS people,
                              sum(t.stage IN ('drafted','followup_drafted')) AS pending,
                              sum(t.stage='approved') AS queued,
                              sum(t.last_sent_at IS NOT NULL) AS sent,
                              sum(t.stage='replied') AS replied,
                              sum(t.stage='failed') AS failed
                          FROM dm_campaigns d LEFT JOIN dm_targets t ON t.campaign_id=d.id
                          WHERE d.plan_id IS NOT NULL GROUP BY d.id"""):
        out[r["plan_id"]] = {"campaign_id": r["id"], "status": r["status"], **{k: r[k] or 0 for k in
                             ("people", "pending", "queued", "sent", "replied", "failed")}}
    return out


def redraft(campaign_id):
    """Re-render every still-pending message of a campaign from its current templates."""
    with _lock, _db() as c:
        camp = c.execute("SELECT * FROM dm_campaigns WHERE id=?", (int(campaign_id),)).fetchone()
        rows = c.execute("""SELECT m.id, m.kind, cr.handle, cr.name, t.price FROM dm_messages m JOIN dm_targets t ON t.id=m.target_id
                            LEFT JOIN creators cr ON cr.key=t.creator_key WHERE m.campaign_id=? AND m.status='pending'""",
                         (int(campaign_id),)).fetchall()
        for r in rows:
            tpl = camp["message"] if r["kind"] == "first" else camp["followup"]
            c.execute("UPDATE dm_messages SET body=? WHERE id=?", (render(tpl, dict(r)), r["id"]))
    return {"redrafted": len(rows)}


def edit_message(msg_id, body):
    body = (body or "").strip()
    if not body:
        raise ValueError("message is empty")
    if len(body) > 10000:
        raise ValueError("X DMs are capped at 10,000 characters")
    with _lock, _db() as c:
        n = c.execute("UPDATE dm_messages SET body=? WHERE id=? AND status IN ('pending','approved')", (body, int(msg_id))).rowcount
    if not n:
        raise ValueError("that message was already sent or skipped")
    return {"ok": True}


def approve(ids, email):
    """A person's go-ahead for these DMs. Only pending ones can be approved; the worker sends them later."""
    ids = [int(i) for i in ids]
    with _lock, _db() as c:
        n = 0
        for i in ids:
            n += c.execute("UPDATE dm_messages SET status='approved', approved_by=?, approved_at=? WHERE id=? AND status='pending'",
                           (email, _now(), i)).rowcount
            c.execute("""UPDATE dm_targets SET stage='approved' WHERE id=(SELECT target_id FROM dm_messages WHERE id=?)
                         AND stage IN ('drafted','followup_drafted')""", (i,))
    _wake.set()
    return {"approved": n}


def unapprove(ids):
    with _lock, _db() as c:
        n = sum(c.execute("UPDATE dm_messages SET status='pending', approved_by=NULL, approved_at=NULL WHERE id=? AND status='approved'",
                          (int(i),)).rowcount for i in ids)
    return {"unapproved": n}


def skip(ids):
    with _lock, _db() as c:
        n = 0
        for i in ids:
            row = c.execute("SELECT target_id, kind FROM dm_messages WHERE id=? AND status IN ('pending','approved')", (int(i),)).fetchone()
            if not row:
                continue
            n += c.execute("UPDATE dm_messages SET status='skipped' WHERE id=?", (int(i),)).rowcount
            c.execute("UPDATE dm_targets SET stage=? WHERE id=?", ("stopped" if row["kind"] == "first" else "done", row["target_id"]))
    return {"skipped": n}


def set_target_stage(ids, stage):
    """By hand: they replied somewhere else, or stop messaging them."""
    if stage not in ("replied", "stopped", "done"):
        raise ValueError("stage is replied, stopped or done")
    with _lock, _db() as c:
        for i in ids:
            c.execute("UPDATE dm_targets SET stage=?, next_due_at=NULL, replied_at=CASE WHEN ?='replied' THEN COALESCE(replied_at, ?) ELSE replied_at END WHERE id=?",
                      (stage, stage, _now(), int(i)))
            c.execute("UPDATE dm_messages SET status='skipped' WHERE target_id=? AND status IN ('pending','approved')", (int(i),))
    return {"updated": len(ids)}


def mark_sent_by_hand(msg_id, email):
    """Assisted send: a teammate sent this DM themselves from x.com (prefilled link); record it like an API send."""
    with _db() as c:
        m = c.execute("""SELECT m.*, t.x_user_id, t.handle, t.creator_key, d.account_id FROM dm_messages m
                         JOIN dm_targets t ON t.id=m.target_id JOIN dm_campaigns d ON d.id=m.campaign_id
                         WHERE m.id=? AND m.status IN ('pending','approved')""", (int(msg_id),)).fetchone()
        if not m:
            raise ValueError("that message was already sent or skipped")
        camp = c.execute("SELECT * FROM dm_campaigns WHERE id=?", (m["campaign_id"],)).fetchone()
        acct = c.execute("SELECT * FROM dm_accounts WHERE user_id=?", (m["account_id"],)).fetchone()
    m = dict(m)
    m["approved_by"] = m["approved_by"] or email
    acct = dict(acct) if acct else {"user_id": "manual", "handle": "manual"}
    with _lock, _db() as c:
        c.execute("UPDATE dm_messages SET approved_by=?, approved_at=COALESCE(approved_at, ?) WHERE id=?", (m["approved_by"], _now(), m["id"]))
    _record_sent(dict(camp), acct, m, m["x_user_id"], {"dm_event_id": "manual", "dm_conversation_id": None})
    with _lock, _db() as c:
        c.execute("UPDATE dm_messages SET error='sent by hand on x.com' WHERE id=?", (m["id"],))
    return {"ok": True}


def simulate_reply(target_id, text):
    """Demo only: pretend the creator answered, to see how a reply stops the sequence."""
    with _lock, _db() as c:
        t = c.execute("SELECT t.*, d.account_id FROM dm_targets t JOIN dm_campaigns d ON d.id=t.campaign_id WHERE t.id=?",
                      (int(target_id),)).fetchone()
        if not t or t["account_id"] != SIM_ID:
            raise ValueError("replies can only be simulated on campaigns sending from the demo account")
        if not t["last_sent_at"]:
            raise ValueError("nothing was sent to them yet")
        c.execute("""INSERT INTO dm_inbox(event_id, account_id, conversation_id, sender_id, text, created_at, target_id, inbound)
                     VALUES (?,?,?,?,?,?,?,1)""", (f"sim-r{target_id}-{int(time.time())}", SIM_ID, None, t["x_user_id"], text, _now(), t["id"]))
        c.execute("UPDATE dm_targets SET stage='replied', next_due_at=NULL, replied_at=?, reply_text=? WHERE id=?", (_now(), text, t["id"]))
        c.execute("UPDATE dm_messages SET status='skipped', error='they replied' WHERE target_id=? AND status IN ('pending','approved')", (t["id"],))
    return {"ok": True}


def remove_targets(ids):
    with _lock, _db() as c:
        c.execute("PRAGMA foreign_keys=ON")
        for i in ids:
            c.execute("DELETE FROM dm_targets WHERE id=? AND last_sent_at IS NULL", (int(i),))
    return {"ok": True}


# ---------------------------------------------------------------- view

def _tz(name):
    try:
        return ZoneInfo(name or "America/New_York") if ZoneInfo else timezone.utc
    except Exception:  # noqa: BLE001
        return timezone.utc


def _sent_today(c, account_id, tz):
    start = datetime.now(tz).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)
    return c.execute("SELECT count(*) FROM dm_messages WHERE account_id=? AND status='sent' AND sent_at>=?",
                     (account_id, start.isoformat(timespec="seconds"))).fetchone()[0]


def _in_window(camp, at=None):
    t = (at or datetime.now(timezone.utc)).astimezone(_tz(camp["tz"]))
    return camp["send_start"] <= t.hour < camp["send_end"]


def view(campaign_id=None):
    with _db() as c:
        accounts = _rows(c, "SELECT user_id, handle, name, daily_cap, connected_by, connected_at, error, rate_until, last_poll FROM dm_accounts ORDER BY handle")
        for a in accounts:
            a["sent_today"] = _sent_today(c, a["user_id"], _tz("America/New_York"))
            a["rate_limited"] = (a.pop("rate_until") or 0) > time.time()
        camps = _rows(c, """SELECT d.*, (SELECT count(*) FROM dm_targets t WHERE t.campaign_id=d.id) AS targets,
                               (SELECT count(*) FROM dm_messages m WHERE m.campaign_id=d.id AND m.status='pending') AS pending,
                               (SELECT count(*) FROM dm_messages m WHERE m.campaign_id=d.id AND m.status='approved') AS approved,
                               (SELECT count(*) FROM dm_messages m WHERE m.campaign_id=d.id AND m.status='sent') AS sent,
                               (SELECT count(*) FROM dm_targets t WHERE t.campaign_id=d.id AND t.stage='replied') AS replied
                            FROM dm_campaigns d ORDER BY d.id DESC""")
        for a in accounts:
            a["simulated"] = a["user_id"] == SIM_ID
        out = {"enabled": x_dm.enabled(), "accounts": accounts, "campaigns": camps, "fields": list(FIELDS)}
        cid = int(campaign_id) if campaign_id else (camps[0]["id"] if camps else None)
        camp = next((x for x in camps if x["id"] == cid), None)
        if not camp:
            return out
        if camp.get("plan_id"):
            pl = c.execute("SELECT name, campaign_id FROM plans WHERE id=?", (camp["plan_id"],)).fetchone()
            camp["plan_name"], camp["wave_id"] = (pl[0], pl[1]) if pl else (None, None)
        out["campaign"] = camp
        out["in_window"] = _in_window(camp)
        acct = next((a for a in accounts if a["user_id"] == camp["account_id"]), None)
        out["account"] = acct
        out["targets"] = _rows(c, """SELECT t.*, cr.name, cr.avatar, cr.followers, cr.median_views, cr.outreached,
                                         cr.outreach_from
                                     FROM dm_targets t LEFT JOIN creators cr ON cr.key=t.creator_key
                                     WHERE t.campaign_id=? ORDER BY t.id""", (cid,))
        msgs = _rows(c, "SELECT * FROM dm_messages WHERE campaign_id=? ORDER BY id", (cid,))
        by_t = {}
        for m in msgs:
            by_t.setdefault(m["target_id"], []).append(m)
        for t in out["targets"]:
            t["messages"] = by_t.get(t["id"], [])
        out["inbox"] = _rows(c, """SELECT i.*, t.handle FROM dm_inbox i JOIN dm_targets t ON t.id=i.target_id
                                   WHERE t.campaign_id=? AND i.inbound=1 ORDER BY i.created_at DESC LIMIT 100""", (cid,))
        in_camp = {t["creator_key"] for t in out["targets"]}
        out["candidates"] = _rows(c, """SELECT key, handle, name, followers, median_views, manual_verdict, outreached,
                                           outreach_from, user_id FROM creators WHERE platform='x' ORDER BY followers DESC""")
        out["candidates"] = [r for r in out["candidates"] if r["key"] not in in_camp]
    return out


# ---------------------------------------------------------------- worker

def _send_next(c_camp, acct):
    """Send the oldest approved DM of this campaign from its account. Returns True when one went out."""
    with _db() as c:
        m = c.execute("""SELECT m.*, t.x_user_id, t.handle, t.creator_key, t.stage FROM dm_messages m
                         JOIN dm_targets t ON t.id=m.target_id
                         WHERE m.campaign_id=? AND m.status='approved' ORDER BY m.approved_at, m.id LIMIT 1""",
                      (c_camp["id"],)).fetchone()
    if not m:
        return False
    m = dict(m)
    with _lock, _db() as c:   # claim it so a second worker / restart can't double-send
        if not c.execute("UPDATE dm_messages SET status='sending' WHERE id=? AND status='approved'", (m["id"],)).rowcount:
            return False
    try:
        if acct["user_id"] == SIM_ID:            # demo account: nothing leaves the server
            rid, res = m["x_user_id"] or "sim-" + m["handle"], {"dm_event_id": f"sim-{m['id']}", "dm_conversation_id": None}
        else:
            token = _access_token(acct)
            rid = m["x_user_id"] or x_dm.user_id(token, m["handle"])
            res = x_dm.send(token, rid, m["body"])
    except x_dm.DMError as e:
        with _lock, _db() as c:
            if e.kind in ("rate", "auth"):       # not this person's fault: put it back and hold the account
                c.execute("UPDATE dm_messages SET status='approved' WHERE id=?", (m["id"],))
                c.execute("UPDATE dm_accounts SET rate_until=?, error=? WHERE user_id=?",
                          (e.retry_at or time.time() + (900 if e.kind == "rate" else 10 ** 9), str(e)[:300], acct["user_id"]))
            else:
                c.execute("UPDATE dm_messages SET status='failed', error=?, account_id=? WHERE id=?", (str(e)[:300], acct["user_id"], m["id"]))
                c.execute("UPDATE dm_targets SET stage='failed', error=? WHERE id=?", (str(e)[:300], m["target_id"]))
        print(f"outreach: DM to @{m['handle']} failed: {e}", flush=True)
        return e.kind not in ("rate", "auth")
    except Exception as e:  # noqa: BLE001 — network etc.: retry later
        with _lock, _db() as c:
            c.execute("UPDATE dm_messages SET status='approved' WHERE id=?", (m["id"],))
        print(f"outreach: DM to @{m['handle']} errored, will retry: {e}", flush=True)
        return False
    _record_sent(c_camp, acct, m, rid, res)
    return True


def _record_sent(c_camp, acct, m, rid, res):
    """Bookkeeping once a DM is out: message + target state, the follow-up due date and the creator's outreach flag."""
    sent = _now()
    nxt = None
    with _db() as c:
        t = c.execute("SELECT followups_sent FROM dm_targets WHERE id=?", (m["target_id"],)).fetchone()
    fu = (t["followups_sent"] if t else 0) + (1 if m["kind"] == "followup" else 0)
    if (c_camp["followup"] or "").strip() and fu < (c_camp["max_followups"] or 0):
        nxt = (datetime.now(timezone.utc) + timedelta(days=c_camp["followup_days"] or 4)).isoformat(timespec="seconds")
    with _lock, _db() as c:
        c.execute("UPDATE dm_messages SET status='sent', sent_at=?, account_id=?, dm_event_id=?, error=NULL WHERE id=?",
                  (sent, acct["user_id"], res.get("dm_event_id"), m["id"]))
        c.execute("""UPDATE dm_targets SET stage=CASE WHEN stage='replied' THEN stage ELSE 'sent' END, last_sent_at=?,
                         next_due_at=?, x_user_id=?, conversation_id=?, error=NULL,
                         followups_sent=followups_sent + ? WHERE id=?""",
                  (sent, nxt, rid, res.get("dm_conversation_id"), 1 if m["kind"] == "followup" else 0, m["target_id"]))
        if acct["user_id"] != SIM_ID:   # a demo send isn't real contact
            c.execute("""UPDATE creators SET outreached=1, outreached_by=COALESCE(outreached_by, ?), outreached_at=COALESCE(outreached_at, ?),
                             outreach_from=? WHERE key=?""", (m["approved_by"], sent, "@" + acct["handle"], m["creator_key"]))
        gap = SIM_GAP if acct["user_id"] == SIM_ID else GAP
        c.execute("UPDATE dm_accounts SET next_send_at=?, error=NULL WHERE user_id=?", (time.time() + random.uniform(*gap), acct["user_id"]))
    print(f"outreach: DM sent to @{m['handle']} from @{acct['handle']}", flush=True)
    return True


def _draft_followups():
    with _lock, _db() as c:
        due = c.execute("""SELECT t.*, d.followup, d.max_followups, cr.name, cr.handle AS h FROM dm_targets t
                           JOIN dm_campaigns d ON d.id=t.campaign_id LEFT JOIN creators cr ON cr.key=t.creator_key
                           WHERE t.stage='sent' AND t.next_due_at IS NOT NULL AND t.next_due_at<=?
                             AND t.followups_sent < d.max_followups AND trim(coalesce(d.followup,''))!=''""", (_now(),)).fetchall()
        for t in due:
            c.execute("INSERT INTO dm_messages(target_id, campaign_id, kind, body, created_at) VALUES (?,?,?,?,?)",
                      (t["id"], t["campaign_id"], "followup", render(t["followup"], {"name": t["name"], "handle": t["h"], "price": t["price"]}), _now()))
            c.execute("UPDATE dm_targets SET stage='followup_drafted', next_due_at=NULL WHERE id=?", (t["id"],))
    return len(due)


def _poll(acct):
    """Read the account's newest DMs; a message from someone we contacted marks them replied and stops the sequence."""
    with _db() as c:
        since = (datetime.now(timezone.utc) - timedelta(days=REPLY_WINDOW_DAYS)).isoformat(timespec="seconds")
        tmap = {r["x_user_id"]: r["id"] for r in c.execute(
            """SELECT t.id, t.x_user_id FROM dm_targets t JOIN dm_messages m ON m.target_id=t.id
               WHERE m.account_id=? AND m.status='sent' AND t.last_sent_at>=? AND t.x_user_id IS NOT NULL""", (acct["user_id"], since))}
    if not tmap:
        return
    try:
        events = x_dm.inbox(_access_token(acct), stop_at=acct["last_event_id"])
    except x_dm.DMError as e:
        with _lock, _db() as c:
            c.execute("UPDATE dm_accounts SET last_poll=?, error=? WHERE user_id=?", (time.time(), str(e)[:300], acct["user_id"]))
        return
    with _lock, _db() as c:
        for e in reversed(events):                 # oldest first
            tid = tmap.get(e.get("sender_id")) if e.get("sender_id") != acct["user_id"] else None
            peer_tid = tid or next((v for k, v in tmap.items() if k in (e.get("dm_conversation_id") or "")), None)
            c.execute("""INSERT OR IGNORE INTO dm_inbox(event_id, account_id, conversation_id, sender_id, text, created_at, target_id, inbound)
                         VALUES (?,?,?,?,?,?,?,?)""", (e["id"], acct["user_id"], e.get("dm_conversation_id"), e.get("sender_id"),
                                                      e.get("text"), e.get("created_at"), peer_tid, 1 if tid else 0))
            if tid:
                c.execute("UPDATE dm_targets SET stage='replied', next_due_at=NULL, replied_at=COALESCE(replied_at, ?), reply_text=? WHERE id=?",
                          (e.get("created_at") or _now(), (e.get("text") or "")[:1000], tid))
                c.execute("UPDATE dm_messages SET status='skipped', error='they replied' WHERE target_id=? AND status IN ('pending','approved')", (tid,))
        c.execute("UPDATE dm_accounts SET last_poll=?, last_event_id=COALESCE(?, last_event_id) WHERE user_id=?",
                  (time.time(), events[0]["id"] if events else None, acct["user_id"]))


def tick():
    _draft_followups()
    with _db() as c:
        accts = {r["user_id"]: dict(r) for r in c.execute("SELECT * FROM dm_accounts")
                 if r["user_id"] == SIM_ID or x_dm.enabled()}
        camps = _rows(c, "SELECT * FROM dm_campaigns WHERE status='active' ORDER BY id")
    for acct in accts.values():
        if acct["user_id"] != SIM_ID and time.time() - (acct["last_poll"] or 0) >= POLL_EVERY:
            _poll(acct)
    for camp in camps:
        acct = accts.get(camp["account_id"])
        if not acct or (acct["user_id"] != SIM_ID and not _in_window(camp)):   # the demo ignores the window
            continue
        if (acct["rate_until"] or 0) > time.time() or (acct["next_send_at"] or 0) > time.time():
            continue
        with _db() as c:
            tz = _tz(camp["tz"])
            if _sent_today(c, acct["user_id"], tz) >= (acct["daily_cap"] or 40):
                continue
            start = datetime.now(tz).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)
            camp_today = c.execute("SELECT count(*) FROM dm_messages WHERE campaign_id=? AND status='sent' AND sent_at>=?",
                                   (camp["id"], start.isoformat(timespec="seconds"))).fetchone()[0]
        if camp_today >= camp["daily_limit"]:
            continue
        if _send_next(camp, acct):
            with _db() as c:
                acct.update(dict(c.execute("SELECT * FROM dm_accounts WHERE user_id=?", (acct["user_id"],)).fetchone()))


def worker():
    with _lock, _db() as c:   # a crash mid-send leaves 'sending': X may or may not have it, so don't resend blindly
        c.execute("UPDATE dm_messages SET status='failed', error='interrupted while sending; check the chat on X before retrying' WHERE status='sending'")
    while True:
        try:
            tick()
        except Exception as e:  # noqa: BLE001 — keep the worker alive
            print(f"outreach worker: {e}", flush=True)
        _wake.wait(30)
        _wake.clear()
