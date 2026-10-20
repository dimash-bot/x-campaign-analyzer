"""X (Twitter) DMs through the official API v2: connect a sending account (OAuth 2.0 with PKCE), send DMs and
read the account's inbox for replies.

Needs an X developer app (pay-per-use; a DM send is about $0.015, a DM event read about $0.01) with OAuth 2.0
turned on and the callback URL <PUBLIC_URL>/auth/x/callback. Env: X_CLIENT_ID, plus X_CLIENT_SECRET when the app
is a confidential client ("Web App"). Stdlib + requests only.
"""
import base64
import hashlib
import os
import secrets
import time
from urllib.parse import urlencode

import requests

CLIENT_ID = os.environ.get("X_CLIENT_ID", "")
CLIENT_SECRET = os.environ.get("X_CLIENT_SECRET", "")
SCOPES = "dm.read dm.write tweet.read users.read offline.access"
API = "https://api.x.com/2"


class DMError(Exception):
    """A send or read failed. kind: closed (can't message this person), rate (slow down), auth (reconnect), other."""
    def __init__(self, msg, kind="other", retry_at=None):
        super().__init__(msg)
        self.kind, self.retry_at = kind, retry_at


def enabled():
    return bool(CLIENT_ID)


def new_pkce():
    verifier = secrets.token_urlsafe(64)[:96]
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    return verifier, challenge


def connect_url(redirect_uri, state, challenge):
    return "https://x.com/i/oauth2/authorize?" + urlencode({
        "response_type": "code", "client_id": CLIENT_ID, "redirect_uri": redirect_uri, "scope": SCOPES,
        "state": state, "code_challenge": challenge, "code_challenge_method": "S256"})


def _token(data):
    auth = (CLIENT_ID, CLIENT_SECRET) if CLIENT_SECRET else None
    if not auth:
        data = {**data, "client_id": CLIENT_ID}
    r = requests.post(API + "/oauth2/token", data=data, auth=auth, timeout=20)
    if r.status_code != 200:
        raise DMError(f"X token HTTP {r.status_code}: {r.text[:200]}", "auth")
    t = r.json()
    return {"access_token": t["access_token"], "refresh_token": t.get("refresh_token"),
            "expires_at": int(time.time()) + int(t.get("expires_in") or 7200) - 120, "scope": t.get("scope", "")}


def exchange(code, redirect_uri, verifier):
    """→ tokens + the connected account's {user_id, handle, name}."""
    tok = _token({"grant_type": "authorization_code", "code": code, "redirect_uri": redirect_uri,
                  "code_verifier": verifier})
    me = _call("GET", "/users/me", tok["access_token"])["data"]
    return {**tok, "user_id": me["id"], "handle": me["username"], "name": me.get("name")}


def refresh(refresh_token):
    return _token({"grant_type": "refresh_token", "refresh_token": refresh_token})


def _call(method, path, token, **kw):
    r = requests.request(method, API + path, headers={"Authorization": "Bearer " + token}, timeout=20, **kw)
    if r.status_code in (200, 201):
        return r.json()
    body = r.text[:300]
    if r.status_code == 429:
        reset = r.headers.get("x-rate-limit-reset")
        raise DMError("X rate limit hit", "rate", int(reset) if reset and reset.isdigit() else int(time.time()) + 900)
    if r.status_code == 401:
        raise DMError("X login expired: reconnect the account", "auth")
    if r.status_code == 403 and "dm" in path:
        raise DMError("Can't DM them: their DMs are closed to us or they blocked us. " + body, "closed")
    raise DMError(f"X HTTP {r.status_code}: {body}")


def send(token, recipient_id, text):
    """→ {dm_event_id, dm_conversation_id}."""
    return _call("POST", f"/dm_conversations/with/{recipient_id}/messages", token, json={"text": text})["data"]


def user_id(token, handle):
    return _call("GET", f"/users/by/username/{handle.lstrip('@')}", token)["data"]["id"]


def inbox(token, stop_at=None, max_pages=3):
    """Newest-first MessageCreate events across the account's conversations, stopping at event id `stop_at`
    (already seen). Each page read is billed, so callers poll sparingly."""
    out, page = [], None
    for _ in range(max_pages):
        q = {"event_types": "MessageCreate", "max_results": 50,
             "dm_event.fields": "id,text,sender_id,created_at,dm_conversation_id"}
        if page:
            q["pagination_token"] = page
        d = _call("GET", "/dm_events?" + urlencode(q), token)
        for e in d.get("data") or []:
            if e["id"] == stop_at:
                return out
            out.append(e)
        page = (d.get("meta") or {}).get("next_token")
        if not page or not stop_at:
            break
    return out
