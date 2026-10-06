"""MCP endpoint so Claude (claude.ai connector or Claude Code) can read and write campaign data.

Transport: MCP "streamable HTTP" — Claude POSTs JSON-RPC messages to /mcp and we answer with plain JSON
(no server-initiated streaming needed).

Auth: OAuth 2.1 as the MCP spec describes, with this app as the authorization server and Google as the
identity check (same @nace.ai rule as the web UI). Everything is stateless — client ids, auth codes and
tokens are HMAC-signed blobs (auth.sign), so there are no extra tables:
  /.well-known/oauth-protected-resource   -> tells Claude where to authorize
  /.well-known/oauth-authorization-server -> endpoints below
  /oauth/register   dynamic client registration (Claude registers itself; client_id = signed redirect URIs)
  /oauth/authorize  needs a signed-in user (Google login), shows an Allow screen, redirects back with a code
  /oauth/token      code + PKCE verifier -> 1h access token + 30d refresh token
"""
import base64
import hashlib
import json
import time
from urllib.parse import parse_qs, urlencode, urlparse

import auth

PROTOCOL_VERSIONS = ["2025-06-18", "2025-03-26", "2024-11-05"]
ACCESS_TTL = 3600
REFRESH_TTL = 30 * 86400

INSTRUCTIONS = """Campaign Analyzer: paid creator campaigns (X, LinkedIn, other platforms), one campaign per wave.
- A payment sheet row = creator, cost, notes, post link(s). Use add_posts for every post link (X, LinkedIn,
  YouTube, ...). Profile links and payment links are not posts — skip them. If one payment covers several post
  links, split the cost evenly across them. Use set_other_spend only for spend with no post at all.
- X metrics are fetched automatically. LinkedIn: reactions + comments are fetched, impressions are not public —
  if the user gives impressions (e.g. from a screenshot), set them with update_post(views=...). Other platforms:
  all metrics come from the user via update_post.
- A signup export (CSV of users) goes to import_users as raw CSV text. Users are assigned to waves by signup
  time: each user belongs to the latest wave whose first post was before their signup (earlier users -> first
  wave). Daily registrations / API users are recalculated from it. Ask for the export's timezone if unclear.
- API users = users who made at least one API request.
- get_report gives the dashboard numbers and top performers per wave; use it to answer analysis questions.
- get_creators lists every influencer with followers, their normal median views / ER (from their recent posts)
  and how our paid posts did vs that baseline. If median views are missing (X needs a login for timelines) and
  you can look them up (e.g. with a scraping tool), save them with set_creator_stats."""


# ---------------------------------------------------------------- tool definitions

def _campaign_arg(desc="Campaign id or exact name, e.g. \"2 wave\""):
    return {"type": ["integer", "string"], "description": desc}


TOOLS = [
    {"name": "list_campaigns", "description": "List all campaigns (waves) with headline numbers.",
     "inputSchema": {"type": "object", "properties": {}}},
    {"name": "create_campaign", "description": "Create a new campaign / wave.",
     "inputSchema": {"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]}},
    {"name": "rename_campaign", "description": "Rename a campaign.",
     "inputSchema": {"type": "object", "properties": {"campaign": _campaign_arg(), "name": {"type": "string"}},
                     "required": ["campaign", "name"]}},
    {"name": "add_posts",
     "description": "Add posts (X, LinkedIn, YouTube, TikTok, ...) to a campaign with what each cost. Re-adding an existing "
                    "post updates its cost/note. X and LinkedIn metrics are fetched in the background; call get_posts later.",
     "inputSchema": {"type": "object", "required": ["campaign", "posts"], "properties": {
         "campaign": _campaign_arg(),
         "posts": {"type": "array", "items": {"type": "object", "required": ["url"], "properties": {
             "url": {"type": "string", "description": "Post link, e.g. x.com/<handle>/status/<id> or a LinkedIn post / lnkd.in link"},
             "cost": {"type": "number", "description": "USD paid for this post (split shared payments evenly)"},
             "note": {"type": "string", "description": "format + payment status, e.g. 'QRT · paid with paypal'"}}}}}}},
    {"name": "update_post",
     "description": "Change a post's cost, note, attribution day (YYYY-MM-DD), or — for non-X posts — metrics that "
                    "can't be fetched (LinkedIn impressions go in views; reposts in retweets; comments in replies).",
     "inputSchema": {"type": "object", "required": ["campaign", "url"], "properties": {
         "campaign": _campaign_arg(), "url": {"type": "string"}, "cost": {"type": "number"},
         "note": {"type": "string"}, "day": {"type": "string"},
         **{k: {"type": "integer"} for k in ("views", "likes", "retweets", "quotes", "replies", "bookmarks")}}}},
    {"name": "remove_post", "description": "Remove a post from a campaign.",
     "inputSchema": {"type": "object", "required": ["campaign", "url"],
                     "properties": {"campaign": _campaign_arg(), "url": {"type": "string"}}}},
    {"name": "set_other_spend", "description": "Set spend that isn't an X post (e.g. a LinkedIn deal). Counts toward total budget and cost per registration, not CPM.",
     "inputSchema": {"type": "object", "required": ["campaign", "amount"], "properties": {
         "campaign": _campaign_arg(), "amount": {"type": "number"}, "note": {"type": "string"}}}},
    {"name": "refresh_metrics", "description": "Re-fetch current X metrics for every post in a campaign (runs in background).",
     "inputSchema": {"type": "object", "required": ["campaign"], "properties": {"campaign": _campaign_arg()}}},
    {"name": "import_users",
     "description": "Import a signup export (CSV text with a header row; needs email + signup time columns, optionally "
                    "keys, requests, credit). Replaces users with the same email, assigns every user to a wave, and "
                    "recalculates daily registrations and API users for all waves.",
     "inputSchema": {"type": "object", "required": ["csv_text"], "properties": {
         "csv_text": {"type": "string", "description": "The full CSV file content"},
         "timezone": {"type": "string", "description": "IANA timezone of the signup times, default America/Los_Angeles"}}}},
    {"name": "get_report",
     "description": "Dashboard numbers per campaign (spend by platform, views, ER, CPM, CPE, registrations, API users, "
                    "cost per registration / API user) plus creators ranked by cost efficiency. CPM/ER only count posts "
                    "that have views; missing_views lists paid posts still needing impressions.",
     "inputSchema": {"type": "object", "properties": {
         "campaign": _campaign_arg("Optional: one campaign; omit for all"),
         "top": {"type": "integer", "description": "How many top/bottom creators to list (default 5)"}}}},
    {"name": "get_creators",
     "description": "Every influencer across waves: followers, their normal median views and ER (recent own posts, paid "
                    "posts excluded), and how our paid posts did: avg views, vs_median (×), reach %, ER, CPM, CPE, "
                    "$ per 1K followers. Optional filter by wave.",
     "inputSchema": {"type": "object", "properties": {"campaign": _campaign_arg("Optional: only creators in this wave")}}},
    {"name": "set_creator_stats",
     "description": "Save an influencer's profile/baseline numbers that couldn't be fetched (LinkedIn followers, or median "
                    "views/ER when X needs a login). Identify the creator by X handle or, for LinkedIn, their name.",
     "inputSchema": {"type": "object", "required": ["creator"], "properties": {
         "creator": {"type": "string", "description": "X handle (e.g. dravenip) or LinkedIn name (e.g. Eduardo Ordax)"},
         "platform": {"type": "string", "enum": ["x", "linkedin", "other"], "description": "default x"},
         "followers": {"type": "integer"}, "median_views": {"type": "integer"}, "median_likes": {"type": "integer"},
         "median_er": {"type": "number", "description": "percent, e.g. 1.8"},
         "sample_size": {"type": "integer", "description": "how many recent posts the medians are based on"},
         "note": {"type": "string", "description": "where the numbers came from, e.g. 'last 20 posts via scraper'"}}}},
    {"name": "get_posts", "description": "Every post in a campaign with cost, note and metrics.",
     "inputSchema": {"type": "object", "required": ["campaign"], "properties": {"campaign": _campaign_arg()}}},
]


# ---------------------------------------------------------------- tool implementations

class ToolError(Exception):
    pass


def _campaign_id(app, c, ref):
    if isinstance(ref, int) or (isinstance(ref, str) and ref.isdigit()):
        row = c.execute("SELECT id FROM campaigns WHERE id=?", (int(ref),)).fetchone()
    else:
        row = c.execute("SELECT id FROM campaigns WHERE lower(name)=lower(?)", (str(ref).strip(),)).fetchone()
    if not row:
        names = ", ".join(f'{r["id"]}: "{r["name"]}"' for r in c.execute("SELECT id, name FROM campaigns"))
        raise ToolError(f"No campaign {ref!r}. Existing: {names}")
    return row["id"]


def _post(app, c, cid, url):
    found = app.platforms.detect(url)
    row = c.execute("SELECT * FROM posts WHERE campaign_id=? AND tweet_id=?", (cid, found[1])).fetchone() if found else None
    if not row and found:   # short links are rewritten to the canonical URL once fetched
        row = c.execute("SELECT * FROM posts WHERE campaign_id=? AND url=?", (cid, found[2])).fetchone()
    if not row:
        raise ToolError(f"Post {url} is not in this campaign")
    return row


def call_tool(app, user, name, args):
    if name == "get_report":
        with app.db() as c:
            cids = [_campaign_id(app, c, args["campaign"])] if args.get("campaign") is not None else None
        return app.report(cids, top=int(args.get("top") or 5))
    if name == "list_campaigns":
        return [{k: v for k, v in r.items() if k != "top" and k != "bottom" and k != "flags"}
                for r in app.report(None, top=0)["campaigns"]]
    if name == "get_creators":
        v = app.creators_view()
        if args.get("campaign") is not None:
            with app.db() as c:
                cname = c.execute("SELECT name FROM campaigns WHERE id=?", (_campaign_id(app, c, args["campaign"]),)).fetchone()["name"]
            v["creators"] = [r for r in v["creators"] if cname in r["waves"]]
        for r in v["creators"]:
            r.pop("avatar", None)
        return {"creators": v["creators"], "x_login_configured": v["x_login"],
                "note": "vs_median = avg views of our paid post / their median views. Missing medians can be filled with set_creator_stats."}
    if name == "set_creator_stats":
        key = app.creator_key(args.get("platform") or "x", str(args["creator"]).lstrip("@"))
        fields = {k: args.get(k) for k in ("followers", "median_views", "median_likes", "median_er", "sample_size")}
        fields["baseline_note"] = args.get("note")
        return app.set_creator_stats(key, fields, source="claude")
    if name == "import_users":
        return app.import_users(args["csv_text"], args.get("timezone") or "America/Los_Angeles")

    to_fetch, result = [], None
    with app._db_lock, app.db() as c:
        if name == "create_campaign":
            cur = c.execute("INSERT INTO campaigns(name, created_at) VALUES (?, ?)", (args["name"].strip(), app.now()))
            result = {"id": cur.lastrowid, "name": args["name"].strip()}
        elif name == "rename_campaign":
            cid = _campaign_id(app, c, args["campaign"])
            c.execute("UPDATE campaigns SET name=? WHERE id=?", (args["name"].strip(), cid))
            result = {"id": cid, "name": args["name"].strip()}
        elif name == "add_posts":
            cid = _campaign_id(app, c, args["campaign"])
            added, updated, invalid = [], [], []
            for p in args["posts"]:
                found = app.platforms.detect(p["url"])
                if not found:
                    invalid.append(p["url"])
                    continue
                plat, tid, url = found
                cost, note = float(p.get("cost") or 0), p.get("note") or None
                cur = c.execute("INSERT OR IGNORE INTO posts(campaign_id, url, tweet_id, budget, note, platform) VALUES (?,?,?,?,?,?)",
                                (cid, url, tid, cost, note, plat))
                if cur.rowcount:
                    added.append(url)
                    to_fetch.append({"id": cur.lastrowid, "tweet_id": tid, "platform": plat, "url": url})
                else:
                    c.execute("UPDATE posts SET budget=?, note=COALESCE(?, note) WHERE campaign_id=? AND tweet_id=?",
                              (cost, note, cid, tid))
                    updated.append(url)
            result = {"added": len(added), "updated_existing": len(updated), "invalid_links": invalid,
                      "total_cost_in_request": sum(float(p.get("cost") or 0) for p in args["posts"]),
                      "note": "Metrics are being fetched in the background (~1s per post)."}
        elif name == "update_post":
            cid = _campaign_id(app, c, args["campaign"])
            row = _post(app, c, cid, args["url"])
            fields = {"budget": args.get("cost"), "note": args.get("note"), "day": args.get("day")}
            auto = app.platforms.AUTO_METRICS[row["platform"] or "x"]
            for k in app.METRICS:
                if args.get(k) is not None:
                    if k in auto:
                        raise ToolError(f"{k} is fetched automatically for {row['platform'] or 'x'} posts and can't be set")
                    fields[k] = int(args[k])
            fields = {k: v for k, v in fields.items() if v is not None}
            if fields:
                c.execute(f"UPDATE posts SET {', '.join(f'{k}=?' for k in fields)} WHERE id=?", (*fields.values(), row["id"]))
            result = {"updated": row["url"], **fields}
        elif name == "remove_post":
            cid = _campaign_id(app, c, args["campaign"])
            row = _post(app, c, cid, args["url"])
            c.execute("DELETE FROM posts WHERE id=?", (row["id"],))
            result = {"removed": row["url"]}
        elif name == "set_other_spend":
            cid = _campaign_id(app, c, args["campaign"])
            c.execute("UPDATE campaigns SET other_spend=?, other_note=? WHERE id=?",
                      (float(args["amount"]), args.get("note"), cid))
            result = {"campaign": cid, "other_spend": float(args["amount"])}
        elif name == "refresh_metrics":
            cid = _campaign_id(app, c, args["campaign"])
            to_fetch = [r for r in c.execute("SELECT id, tweet_id, platform, url FROM posts WHERE campaign_id=?", (cid,))
                        if (r["platform"] or "x") in ("x", "linkedin")]
            result = {"queued": len(to_fetch)}
        elif name == "get_posts":
            cid = _campaign_id(app, c, args["campaign"])
            result = [{k: r[k] for k in ("platform", "url", "author", "day", "budget", "note", *app.METRICS, "fetch_error")}
                      for r in c.execute("SELECT * FROM posts WHERE campaign_id=? ORDER BY day, id", (cid,))]
        else:
            raise ToolError(f"Unknown tool {name}")
    app.enqueue_rows(to_fetch)
    print(f"mcp {name} by {user}", flush=True)
    return result


# ---------------------------------------------------------------- JSON-RPC over HTTP

def handle_rpc(app, user, body: bytes):
    """Returns (status, payload_or_None)."""
    try:
        msg = json.loads(body or b"null")
    except json.JSONDecodeError:
        return 400, {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"}}
    batch = isinstance(msg, list)
    out = [r for r in (_one(app, user, m) for m in (msg if batch else [msg])) if r is not None]
    if not out:
        return 202, None                       # only notifications
    return 200, out if batch else out[0]


def _one(app, user, m):
    if not isinstance(m, dict) or "method" not in m:
        return None if not isinstance(m, dict) or "id" not in m else \
            {"jsonrpc": "2.0", "id": m["id"], "error": {"code": -32600, "message": "Invalid request"}}
    if "id" not in m:
        return None                            # notification (e.g. notifications/initialized)
    rid, method, params = m["id"], m["method"], m.get("params") or {}
    ok = lambda result: {"jsonrpc": "2.0", "id": rid, "result": result}  # noqa: E731
    if method == "initialize":
        v = params.get("protocolVersion")
        return ok({"protocolVersion": v if v in PROTOCOL_VERSIONS else PROTOCOL_VERSIONS[0],
                   "capabilities": {"tools": {}},
                   "serverInfo": {"name": "x-campaign-analyzer", "version": "1.0.0"},
                   "instructions": INSTRUCTIONS})
    if method == "ping":
        return ok({})
    if method == "tools/list":
        return ok({"tools": TOOLS})
    if method == "tools/call":
        try:
            res = call_tool(app, user, params.get("name"), params.get("arguments") or {})
            return ok({"content": [{"type": "text", "text": json.dumps(res, indent=1, default=str)}], "isError": False})
        except (ToolError, KeyError, ValueError, TypeError) as e:
            msg = f"missing argument {e}" if isinstance(e, KeyError) else str(e)
            return ok({"content": [{"type": "text", "text": f"Error: {msg}"}], "isError": True})
    return {"jsonrpc": "2.0", "id": rid, "error": {"code": -32601, "message": f"Method not found: {method}"}}


# ---------------------------------------------------------------- OAuth for MCP clients

def _b64(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode()


def resource_metadata(base):
    return {"resource": base + "/mcp", "authorization_servers": [base], "bearer_methods_supported": ["header"]}


def as_metadata(base):
    return {"issuer": base,
            "authorization_endpoint": base + "/oauth/authorize",
            "token_endpoint": base + "/oauth/token",
            "registration_endpoint": base + "/oauth/register",
            "response_types_supported": ["code"],
            "grant_types_supported": ["authorization_code", "refresh_token"],
            "code_challenge_methods_supported": ["S256"],
            "token_endpoint_auth_methods_supported": ["none"],
            "scopes_supported": ["campaigns"]}


def _redirect_ok(uri: str) -> bool:
    u = urlparse(uri)
    return u.scheme == "https" or (u.scheme == "http" and u.hostname in ("localhost", "127.0.0.1"))


def register(body: bytes):
    meta = json.loads(body or b"{}")
    uris = meta.get("redirect_uris") or []
    if not uris or not all(isinstance(u, str) and _redirect_ok(u) for u in uris):
        return 400, {"error": "invalid_redirect_uri"}
    name = str(meta.get("client_name") or "MCP client")[:80]
    client_id = auth.sign({"t": "client", "r": uris, "n": name, "exp": time.time() + 10 * 365 * 86400})
    return 201, {"client_id": client_id, "client_name": name, "redirect_uris": uris,
                 "grant_types": ["authorization_code", "refresh_token"], "response_types": ["code"],
                 "token_endpoint_auth_method": "none"}


def _client(client_id):
    c = auth.unsign(client_id or "")
    return c if c and c.get("t") == "client" else None


def check_authorize(q):
    """Validate an /oauth/authorize request. Returns (client, error)."""
    g = lambda k: q.get(k, [""])[0]  # noqa: E731
    client = _client(g("client_id"))
    if not client:
        return None, "Unknown client — remove and re-add the connector."
    if g("redirect_uri") not in client["r"]:
        return None, "redirect_uri does not match the registered client."
    if g("response_type") != "code" or g("code_challenge_method") != "S256" or not g("code_challenge"):
        return None, "Only the authorization code flow with PKCE (S256) is supported."
    return client, None


def consent_page(client, q, user, form_token):
    host = urlparse(q["redirect_uri"][0]).hostname
    hidden = "".join(f'<input type="hidden" name="{auth_esc(k)}" value="{auth_esc(v[0])}">' for k, v in q.items())
    return f"""<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Allow access · X Campaign Analyzer</title><style>
:root{{--bg:#f7f7f5;--s:#fff;--b:#e3e3de;--t:#1a1a18;--m:#6b6b66;--a:#1d6fe0}}
@media (prefers-color-scheme:dark){{:root{{--bg:#121211;--s:#1b1b1a;--b:#33332f;--t:#ececea;--m:#9a9a94;--a:#5b9bff}}}}
body{{margin:0;min-height:100vh;display:flex;align-items:center;justify-content:center;background:var(--bg);color:var(--t);
font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Inter,sans-serif;padding:16px}}
.c{{background:var(--s);border:1px solid var(--b);border-radius:14px;padding:28px;max-width:400px;width:100%}}
h1{{font-size:18px;margin:0 0 10px}}p{{color:var(--m);margin:0 0 14px}}b{{color:var(--t)}}
button{{font:inherit;border-radius:9px;padding:10px 18px;border:1px solid var(--b);background:var(--s);color:var(--t);cursor:pointer}}
.p{{background:var(--a);border-color:var(--a);color:#fff;font-weight:600}}.r{{display:flex;gap:8px;justify-content:flex-end;margin-top:8px}}
</style></head><body><form class="c" method="post" action="/oauth/authorize">
<h1>Allow {auth_esc(client['n'])}?</h1>
<p><b>{auth_esc(client['n'])}</b> ({auth_esc(host)}) wants to read and change campaign data as <b>{auth_esc(user['email'])}</b>.</p>
<p>Only allow this if you just added the X Campaign Analyzer connector yourself.</p>
{hidden}<input type="hidden" name="form_token" value="{form_token}">
<div class="r"><button name="decision" value="deny">Deny</button><button class="p" name="decision" value="allow">Allow</button></div>
</form></body></html>"""


def auth_esc(s):
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def issue_code(q, user):
    g = lambda k: q.get(k, [""])[0]  # noqa: E731
    code = auth.sign({"t": "code", "u": user["email"], "c": g("client_id"), "r": g("redirect_uri"),
                      "ch": g("code_challenge"), "exp": time.time() + 300})
    params = {"code": code}
    if g("state"):
        params["state"] = g("state")
    sep = "&" if "?" in g("redirect_uri") else "?"
    return g("redirect_uri") + sep + urlencode(params)


def _tokens(email, client_id):
    now = time.time()
    return {"access_token": auth.sign({"t": "access", "u": email, "exp": now + ACCESS_TTL}),
            "refresh_token": auth.sign({"t": "refresh", "u": email, "c": client_id, "exp": now + REFRESH_TTL}),
            "token_type": "Bearer", "expires_in": ACCESS_TTL, "scope": "campaigns"}


def token(body: bytes):
    f = {k: v[0] for k, v in parse_qs(body.decode()).items()}
    if f.get("grant_type") == "authorization_code":
        code = auth.unsign(f.get("code", ""))
        if not code or code.get("t") != "code" or code["c"] != f.get("client_id") or code["r"] != f.get("redirect_uri"):
            return 400, {"error": "invalid_grant"}
        if _b64(hashlib.sha256(f.get("code_verifier", "").encode()).digest()) != code["ch"]:
            return 400, {"error": "invalid_grant", "error_description": "PKCE verification failed"}
        return 200, _tokens(code["u"], code["c"])
    if f.get("grant_type") == "refresh_token":
        rt = auth.unsign(f.get("refresh_token", ""))
        if not rt or rt.get("t") != "refresh" or (f.get("client_id") and f["client_id"] != rt["c"]):
            return 400, {"error": "invalid_grant"}
        return 200, _tokens(rt["u"], rt["c"])
    return 400, {"error": "unsupported_grant_type"}


def bearer_user(header: str):
    if not header.lower().startswith("bearer "):
        return None
    tok = auth.unsign(header[7:].strip())
    return tok["u"] if tok and tok.get("t") == "access" else None
