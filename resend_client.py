"""Resend email analytics: pull sent emails through the API (RESEND_API_KEY) and verify webhook events
(RESEND_WEBHOOK_SECRET, Svix-signed). Stdlib only."""
import base64
import hashlib
import hmac
import json
import os
import time
import urllib.error
import urllib.request

API = "https://api.resend.com"


def api_enabled():
    return bool(os.environ.get("RESEND_API_KEY"))


def webhook_enabled():
    return bool(os.environ.get("RESEND_WEBHOOK_SECRET"))


def _get(path):
    req = urllib.request.Request(API + path, headers={"Authorization": "Bearer " + os.environ["RESEND_API_KEY"],
                                                      "User-Agent": "x-campaign-analyzer"})
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < 3:      # Resend allows ~2 requests/second
                time.sleep(1.5 * (attempt + 1))
                continue
            body = e.read()[:200].decode("utf-8", "replace")
            raise RuntimeError(f"Resend HTTP {e.code}: {body}") from None


def list_emails(max_pages=100, stop_after_known=None):
    """Newest first, 100 per page. `stop_after_known(page)` → True stops paging (we already have the rest)."""
    out, after = [], None
    for _ in range(max_pages):
        page = _get("/emails?limit=100" + (f"&after={after}" if after else ""))
        data = page.get("data") or []
        out.extend(data)
        if not page.get("has_more") or not data or (stop_after_known and stop_after_known(data)):
            break
        after = data[-1]["id"]
        time.sleep(0.6)
    return out


def verify(headers, body: bytes, tolerance=300):
    """Svix signature check: HMAC-SHA256 over "<svix-id>.<svix-timestamp>.<raw body>" with the whsec_ secret."""
    secret = os.environ.get("RESEND_WEBHOOK_SECRET", "")
    msg_id, ts, sigs = headers.get("svix-id"), headers.get("svix-timestamp"), headers.get("svix-signature") or ""
    if not (secret and msg_id and ts and sigs):
        return False
    try:
        if abs(time.time() - int(ts)) > tolerance:
            return False
        key = base64.b64decode(secret.split("_", 1)[1] if secret.startswith("whsec_") else secret)
    except (ValueError, IndexError):
        return False
    expected = base64.b64encode(hmac.new(key, f"{msg_id}.{ts}.".encode() + body, hashlib.sha256).digest()).decode()
    return any(hmac.compare_digest(expected, s.split(",", 1)[1]) for s in sigs.split() if "," in s)
