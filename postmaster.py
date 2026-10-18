"""Gmail Postmaster Tools: Gmail's own spam rate, domain reputation and SPF / DKIM / DMARC pass rates per
sending domain. Uses the app's Google OAuth client with the read-only Postmaster scope and an offline
refresh token (one teammate connects once; their account must have the domains in Postmaster Tools)."""
import json
from datetime import date, timedelta
from urllib.parse import urlencode

import requests

import auth

SCOPE = "https://www.googleapis.com/auth/postmaster.readonly"
API = "https://gmailpostmastertools.googleapis.com/v1"


def connect_url(redirect_uri: str, state: str) -> str:
    return "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode({
        "client_id": auth.GOOGLE_CLIENT_ID, "redirect_uri": redirect_uri, "response_type": "code",
        "scope": "openid email " + SCOPE, "state": state, "hd": auth.ALLOWED_DOMAIN,
        "access_type": "offline", "prompt": "consent", "include_granted_scopes": "true",
    })


def exchange(code: str, redirect_uri: str) -> dict:
    """→ {refresh_token, email}. Raises PermissionError with a readable reason."""
    r = requests.post("https://oauth2.googleapis.com/token", data={
        "code": code, "client_id": auth.GOOGLE_CLIENT_ID, "client_secret": auth.GOOGLE_CLIENT_SECRET,
        "redirect_uri": redirect_uri, "grant_type": "authorization_code"}, timeout=15)
    if r.status_code != 200:
        raise PermissionError("Google rejected the connection. Try again.")
    tok = r.json()
    if SCOPE not in (tok.get("scope") or ""):
        raise PermissionError("Postmaster access wasn't granted — tick the Postmaster Tools box on Google's screen.")
    if not tok.get("refresh_token"):
        raise PermissionError("Google didn't return a long-lived token. Try connecting again.")
    claims = json.loads(auth._unb64(tok["id_token"].split(".")[1])) if tok.get("id_token") else {}
    return {"refresh_token": tok["refresh_token"], "email": (claims.get("email") or "").lower()}


def access_token(refresh_token: str) -> str:
    r = requests.post("https://oauth2.googleapis.com/token", data={
        "client_id": auth.GOOGLE_CLIENT_ID, "client_secret": auth.GOOGLE_CLIENT_SECRET,
        "refresh_token": refresh_token, "grant_type": "refresh_token"}, timeout=15)
    if r.status_code != 200:
        raise RuntimeError("Google access was revoked or expired — reconnect Postmaster Tools.")
    return r.json()["access_token"]


def _get(token, path, params=None):
    r = requests.get(API + path, params=params or {}, headers={"Authorization": "Bearer " + token}, timeout=30)
    if r.status_code == 403 and ("has not been used" in r.text or "SERVICE_DISABLED" in r.text):
        try:
            msg = r.json()["error"]["message"].split(". ")[0]
        except (ValueError, KeyError):
            msg = r.text[:200]
        raise RuntimeError(f"The Gmail Postmaster Tools API is turned off in the Google Cloud project ({msg}). If you just enabled it, wait a few minutes.")
    if r.status_code != 200:
        raise RuntimeError(f"Postmaster HTTP {r.status_code}: {r.text[:200]}")
    return r.json()


def domains(token) -> list:
    """Domains this Google account can see in Postmaster Tools (verified or shared with it)."""
    return [d["name"].split("/", 1)[1] for d in _get(token, "/domains").get("domains", [])]


def traffic_stats(token, domain, days=120) -> list:
    start, end = date.today() - timedelta(days=days), date.today() + timedelta(days=1)
    params = {"startDate.year": start.year, "startDate.month": start.month, "startDate.day": start.day,
              "endDate.year": end.year, "endDate.month": end.month, "endDate.day": end.day, "pageSize": 200}
    out = []
    while True:
        page = _get(token, f"/domains/{domain}/trafficStats", params)
        out.extend(page.get("trafficStats", []))
        if not page.get("nextPageToken"):
            return out
        params["pageToken"] = page["nextPageToken"]
