"""Gmail Postmaster Tools (API v2): Gmail's own daily spam rate, SPF / DKIM / DMARC pass rates and its compliance
verdict for bulk senders, per sending domain. Uses the app's Google OAuth client with read-only Postmaster scopes
and an offline refresh token (one teammate connects once; their account must have the domain in Postmaster Tools)."""
import json
from datetime import date, timedelta
from urllib.parse import urlencode

import requests

import auth

SCOPES = ["https://www.googleapis.com/auth/postmaster.domain", "https://www.googleapis.com/auth/postmaster.traffic.readonly"]
API = "https://gmailpostmastertools.googleapis.com/v2"


def connect_url(redirect_uri: str, state: str) -> str:
    return "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode({
        "client_id": auth.GOOGLE_CLIENT_ID, "redirect_uri": redirect_uri, "response_type": "code",
        "scope": "openid email " + " ".join(SCOPES), "state": state, "hd": auth.ALLOWED_DOMAIN,
        "access_type": "offline", "prompt": "consent", "include_granted_scopes": "true",
    })


def has_scopes(granted: str) -> bool:
    return all(s in (granted or "") for s in SCOPES)


def exchange(code: str, redirect_uri: str) -> dict:
    """→ {refresh_token, email}. Raises PermissionError with a readable reason."""
    r = requests.post("https://oauth2.googleapis.com/token", data={
        "code": code, "client_id": auth.GOOGLE_CLIENT_ID, "client_secret": auth.GOOGLE_CLIENT_SECRET,
        "redirect_uri": redirect_uri, "grant_type": "authorization_code"}, timeout=15)
    if r.status_code != 200:
        raise PermissionError("Google rejected the connection. Try again.")
    tok = r.json()
    if not has_scopes(tok.get("scope")):
        raise PermissionError("Postmaster access wasn't granted — tick both Postmaster Tools boxes on Google's screen.")
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
    tok = r.json()
    if tok.get("scope") and not has_scopes(tok["scope"]):
        raise RuntimeError("This Postmaster connection predates Google's v2 API — click Disconnect, then Connect Google Postmaster again.")
    return tok["access_token"]


def _call(token, method, path, body=None, params=None):
    r = requests.request(method, API + path, json=body, params=params or {},
                         headers={"Authorization": "Bearer " + token}, timeout=30)
    if r.status_code == 403 and ("has not been used" in r.text or "SERVICE_DISABLED" in r.text):
        try:
            msg = r.json()["error"]["message"].split(". ")[0]
        except (ValueError, KeyError):
            msg = r.text[:200]
        raise RuntimeError(f"The Gmail Postmaster Tools API is turned off in the Google Cloud project ({msg}). If you just enabled it, wait a few minutes.")
    if r.status_code != 200:
        try:
            msg = r.json()["error"]["message"][:240]
        except (ValueError, KeyError):
            msg = r.text[:240]
        raise RuntimeError(f"Postmaster HTTP {r.status_code}: {msg}")
    return r.json()


def domains(token) -> list:
    """[{domain, verified}] — domains this Google account has in Postmaster Tools."""
    out, params = [], {"pageSize": 200}
    while True:
        page = _call(token, "GET", "/domains", params=params)
        for d in page.get("domains", []):
            out.append({"domain": d["name"].split("/", 1)[1], "verified": d.get("verificationState") == "VERIFIED"})
        if not page.get("nextPageToken"):
            return out
        params["pageToken"] = page["nextPageToken"]


def _date(d):
    return {"year": d.year, "month": d.month, "day": d.day}


def _query(token, domain, metrics, days):
    """DAILY domainStats for {name: (standardMetric, filter)} → {iso_day: {name: number}}."""
    body = {"metricDefinitions": [{"name": n, "baseMetric": {"standardMetric": m}, **({"filter": f} if f else {})}
                                  for n, (m, f) in metrics.items()],
            "timeQuery": {"dateRanges": {"dateRanges": [{"start": _date(date.today() - timedelta(days=days)), "end": _date(date.today())}]}},
            "aggregationGranularity": "DAILY", "pageSize": 200}
    out = {}
    while True:
        page = _call(token, "POST", f"/domains/{domain}/domainStats:query", body)
        for s in page.get("domainStats", []):
            d, v = s.get("date") or {}, s.get("value") or {}
            num = v.get("doubleValue", v.get("floatValue", v.get("intValue")))
            if d and num is not None:
                out.setdefault(f"{d['year']:04d}-{d['month']:02d}-{d['day']:02d}", {})[s.get("metric")] = float(num)
        if not page.get("nextPageToken"):
            return out
        body["pageToken"] = page["nextPageToken"]


def daily_stats(token, domain, days=90):
    """({iso_day: {spam, spf, dkim, dmarc}}, notes). Rates as fractions (0.001 = 0.1%); auth rates are best effort."""
    stats, notes = _query(token, domain, {"spam": ("SPAM_RATE", None)}, days), []
    for k in ("spf", "dkim", "dmarc"):
        try:
            for day, vals in _query(token, domain, {k: ("AUTH_SUCCESS_RATE", f'auth_type = "{k}"')}, days).items():
                stats.setdefault(day, {}).update(vals)
        except RuntimeError as e:
            notes.append(f"{k}: {e}")
    return stats, notes


def _state(o):
    """ComplianceStatus is {status: STATE}; tolerate a bare string too."""
    s = (o or {}).get("status") if isinstance(o, dict) else o
    return s.get("status") if isinstance(s, dict) else s


def compliance(token, domain) -> dict:
    """Gmail's bulk-sender verdict: {rows: {REQUIREMENT: COMPLIANT|NEEDS_WORK}, deliverability, one_click, honor}."""
    data = _call(token, "GET", f"/domains/{domain}/complianceStatus").get("complianceData") or {}
    dv = data.get("deliverabilityStatusVerdict") or {}
    oc, hu = data.get("oneClickUnsubscribeVerdict") or {}, data.get("honorUnsubscribeVerdict") or {}
    return {"rows": {r["requirement"]: _state(r.get("status")) for r in data.get("rowData", []) if r.get("requirement")},
            "deliverability": {"state": _state(dv.get("state")), "reason": dv.get("reason")},
            "one_click": {"state": _state(oc.get("status")), "reason": oc.get("reason")},
            "honor": {"state": _state(hu.get("status")), "reason": hu.get("reason")}}
