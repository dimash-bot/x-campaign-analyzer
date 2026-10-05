# X Campaign Analyzer

Track paid creator campaigns by wave — X, LinkedIn and other platforms: paste post links (or rows straight
from the payment sheet) → the server pulls public engagement metrics (X: everything; LinkedIn: reactions +
comments, impressions typed in; others: typed in) → enter daily registrations and API-key users →
see ER, CPM, CPE, cost per registration, cost per API user and reg → API activation.

## How it works

```
Browser (static/index.html)                Server (server.py, Python stdlib)              Outside
──────────────────────────                 ─────────────────────────────────              ───────
paste sheet → parsePaste() ──POST /api/posts──▶ insert rows → queue job ──▶ fetch worker ──▶ X GraphQL
renders tables, computes   ◀─GET /api/state──  campaigns/posts/days + "pending"      (x_client.py)
ER/CPM/$ per reg in JS      (polls every 2s while fetching, 30s otherwise)
                                               SQLite at $DATA_DIR/campaigns.db (+ daily backups)
not signed in → /login ──▶ /auth/login ──▶ Google ──▶ /auth/callback (auth.py: @nace.ai only)
                                               → signed `session` cookie (30 days)
```

- **Backend** keeps raw numbers only (costs, metrics, registrations). All ratios are calculated in the
  browser, so the formulas live in one place (`render()` in `index.html`).
- **Fetching** runs in a background thread, one post at a time with a short pause, so a 30-post paste
  returns instantly and doesn't block other users.
- **Login**: Google OAuth. The server checks the ID token's `hd`/email domain, so only `@nace.ai`
  accounts get a session. The session is an HMAC-signed cookie — no user table.
- **Data**: one SQLite file on a persistent volume. Daily snapshot to `backups/` (14 kept); the
  Backup button downloads the whole DB, Restore uploads one (current data is snapshotted first).

## Run locally

```bash
pip install -r requirements.txt
python3 server.py            # http://localhost:8787 — login is off unless GOOGLE_CLIENT_ID is set
```

## Deploy (Railway, from GitHub)

1. **Google OAuth client** — Google Cloud Console → APIs & Services:
   - OAuth consent screen → User type **Internal** (only the nace.ai Workspace can sign in).
   - Credentials → Create credentials → OAuth client ID → **Web application**.
   - Authorized redirect URI: `https://<your-railway-domain>/auth/callback`.
2. **Railway** → New Project → *Deploy from GitHub repo* → pick this repo (Dockerfile is detected).
3. Service → **Settings → Networking → Generate Domain** (use it in step 1's redirect URI).
4. Service → right-click → **Attach volume**, mount path **`/data`**.
5. Service → **Variables**:

   | Variable | Value |
   |---|---|
   | `GOOGLE_CLIENT_ID` | from step 1 |
   | `GOOGLE_CLIENT_SECRET` | from step 1 |
   | `SESSION_SECRET` | random 64 chars — `openssl rand -hex 32` |
   | `PUBLIC_URL` | `https://<your-railway-domain>` |
   | `ALLOWED_DOMAIN` | optional, default `nace.ai` |
   | `X_AUTH_TOKEN`, `X_CT0` | optional — only if X blocks guest access from Railway (see below) |

6. Bring existing data over: in the **local** app click **Backup** (downloads a consistent snapshot — don't
   copy `campaigns.db` directly, recent edits may still sit in `campaigns.db-wal`), then open the Railway URL,
   sign in, click **Restore** and upload that file.

Every push to `main` redeploys automatically; the volume keeps the data.

## Use it from Claude (MCP connector)

The app exposes an MCP server at `https://<your-railway-domain>/mcp`, so Claude can add and analyse data:
"add this payment sheet to wave 3", "import this users export", "who were the top performers?".

- **claude.ai** (team or personal): Settings → Connectors → **Add custom connector** → URL
  `https://<your-railway-domain>/mcp`. Claude opens the app's sign-in (Google, @nace.ai) and an Allow screen.
- **Claude Code**: `claude mcp add --transport http campaigns https://<your-railway-domain>/mcp`, then `/mcp` to sign in.

Tools: `list_campaigns`, `create_campaign`, `rename_campaign`, `add_posts`, `update_post`, `remove_post`,
`set_other_spend`, `refresh_metrics`, `import_users`, `get_report`, `get_posts`.

How auth works: the app is its own OAuth 2.1 authorization server (dynamic client registration + PKCE), and
uses the Google login to decide who is allowed. Codes and tokens are HMAC-signed with `SESSION_SECRET`
(1-hour access tokens, 30-day refresh tokens); rotating `SESSION_SECRET` signs everyone out.

**Waves are assigned by time.** Each user belongs to the latest wave whose first post came before their
signup, so a new wave's posts must start after the previous wave's — don't put old posts into a new wave.

### If posts show "⚠ HTTP 403/404" after deploying

X sometimes blocks anonymous requests from cloud IPs. Log into a **spare** X account in a browser,
copy the `auth_token` and `ct0` cookies (DevTools → Application → Cookies → x.com) into the
`X_AUTH_TOKEN` / `X_CT0` variables. The client switches to them automatically when guest access fails.
If errors say "Query not found", X rotated its query IDs — update `QIDS` in `x_client.py`.

Note: these are X's internal web endpoints (unofficial, against X's ToS) — fine for tracking your own
campaigns at small scale.
