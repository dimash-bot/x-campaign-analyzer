"""Google sign-in restricted to one Workspace domain, plus signed session cookies.

Flow:
  1. /auth/login     -> redirect to Google with a random `state` (stored in a short-lived signed cookie)
  2. Google          -> user picks their account, Google redirects back to /auth/callback?code=...&state=...
  3. /auth/callback  -> check state, exchange `code` for an ID token (server-to-server, with the client secret),
                        check the token's email is verified and belongs to ALLOWED_DOMAIN,
                        then set a signed `session` cookie. No user table needed.
Every other request checks that cookie's HMAC signature and expiry.
"""
import base64
import hashlib
import hmac
import json
import os
import secrets
import time
from urllib.parse import urlencode

import requests

GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
ALLOWED_DOMAIN = os.environ.get("ALLOWED_DOMAIN", "nace.ai").lower()
SESSION_SECRET = os.environ.get("SESSION_SECRET", "")
SESSION_DAYS = 30

ENABLED = bool(GOOGLE_CLIENT_ID)


def check_config():
    """Fail closed: on a real deployment, refuse to start without auth configured."""
    on_server = bool(os.environ.get("RAILWAY_ENVIRONMENT") or os.environ.get("REQUIRE_AUTH"))
    if on_server and not ENABLED:
        raise SystemExit("GOOGLE_CLIENT_ID is not set — refusing to run without login on a server.")
    if ENABLED and (not GOOGLE_CLIENT_SECRET or len(SESSION_SECRET) < 32):
        raise SystemExit("GOOGLE_CLIENT_SECRET and SESSION_SECRET (32+ chars) must be set when login is enabled.")


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _unb64(s: str) -> bytes:
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


def sign(payload: dict) -> str:
    body = _b64(json.dumps(payload, separators=(",", ":")).encode())
    sig = _b64(hmac.new(SESSION_SECRET.encode(), body.encode(), hashlib.sha256).digest())
    return f"{body}.{sig}"


def unsign(token: str):
    try:
        body, sig = token.split(".", 1)
        good = _b64(hmac.new(SESSION_SECRET.encode(), body.encode(), hashlib.sha256).digest())
        if not hmac.compare_digest(sig, good):
            return None
        payload = json.loads(_unb64(body))
        return payload if payload.get("exp", 0) > time.time() else None
    except (ValueError, json.JSONDecodeError):
        return None


def new_state() -> str:
    return secrets.token_urlsafe(24)


def login_url(redirect_uri: str, state: str) -> str:
    return "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode({
        "client_id": GOOGLE_CLIENT_ID,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": "openid email profile",
        "state": state,
        "hd": ALLOWED_DOMAIN,          # hint: show only accounts of this domain (still verified below)
        "prompt": "select_account",
    })


def exchange_code(code: str, redirect_uri: str) -> dict:
    """Swap the one-time code for the user's identity. Returns {email, name} or raises PermissionError."""
    r = requests.post("https://oauth2.googleapis.com/token", data={
        "code": code,
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "redirect_uri": redirect_uri,
        "grant_type": "authorization_code",
    }, timeout=15)
    if r.status_code != 200:
        raise PermissionError("Google rejected the sign-in. Try again.")
    # The ID token came straight from Google over TLS, so its claims can be read without
    # re-verifying the signature (per Google's docs); we still check every claim that matters.
    claims = json.loads(_unb64(r.json()["id_token"].split(".")[1]))
    email = (claims.get("email") or "").lower()
    if claims.get("aud") != GOOGLE_CLIENT_ID or claims.get("iss") not in ("https://accounts.google.com", "accounts.google.com"):
        raise PermissionError("Invalid sign-in token.")
    if not claims.get("email_verified") or claims.get("hd", "").lower() != ALLOWED_DOMAIN or not email.endswith("@" + ALLOWED_DOMAIN):
        raise PermissionError(f"Only @{ALLOWED_DOMAIN} accounts can use this app.")
    return {"email": email, "name": claims.get("name") or email}


def session_token(user: dict) -> str:
    return sign({**user, "exp": int(time.time()) + SESSION_DAYS * 86400})
