"""Which platform a post link belongs to, and how to fetch its public metrics.

  X         -> x_client (exact views, likes, reposts, quotes, replies, bookmarks)
  LinkedIn  -> public post page's JSON-LD: reactions, comments, post time. Impressions aren't public,
               so they're entered by hand (from the creator's analytics screenshot).
  other     -> recognised post links (YouTube, TikTok, Instagram, Reddit, Threads...): everything manual.

Each post gets a stable `key` (stored in posts.tweet_id for historical reasons): the tweet id for X,
"li:<activity id>" for LinkedIn, "url:<normalised url>" otherwise.
"""
import json
import re
from datetime import datetime, timezone

import requests

from x_client import tweet_id_from

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126 Safari/537.36")

LINKEDIN_POST = re.compile(r"(?:linkedin\.com/(?:posts|feed/update|pulse)/|lnkd\.in/)", re.I)
OTHER_POST = re.compile(
    r"(?:youtube\.com/(?:watch|shorts/)|youtu\.be/|tiktok\.com/@[^/]+/video/|instagram\.com/(?:p|reel)/|"
    r"reddit\.com/r/[^/]+/comments/|threads\.(?:net|com)/@[^/]+/post/|bsky\.app/profile/[^/]+/post/|"
    r"news\.ycombinator\.com/item|producthunt\.com/posts/|medium\.com/.+/.+|substack\.com/p/)", re.I)

# Which metric columns each platform fills automatically (the rest are typed in)
AUTO_METRICS = {
    "x": {"views", "likes", "retweets", "quotes", "replies", "bookmarks"},
    "linkedin": {"likes", "replies"},
    "other": set(),
}


def detect(url: str):
    """(platform, key, canonical_url) for a post link, or None if it isn't a post link."""
    url = url.strip().strip('"')
    if not url:
        return None
    full = url if re.match(r"https?://", url) else "https://" + url
    tid = tweet_id_from(full) if re.search(r"(?:x|twitter)\.com/", full, re.I) else None
    if tid:
        return "x", tid, full.split("?")[0]
    if LINKEDIN_POST.search(full):
        if re.search(r"lnkd\.in/", full, re.I):
            full = _resolve(full)               # short link -> real post URL, so the same post always gets one key
        m = re.search(r"(?:activity|ugcPost|share)[-:](\d{10,})", full)
        return "linkedin", f"li:{m.group(1)}" if m else "url:" + _norm(full), full.split("?")[0]
    if OTHER_POST.search(full):
        return "other", "url:" + _norm(full), full
    return None


def _resolve(url):
    try:
        r = requests.get(url, headers={"User-Agent": UA}, timeout=10, allow_redirects=True, stream=True)
        r.close()
        return r.url if LINKEDIN_POST.search(r.url) else url
    except requests.RequestException:
        return url


def _norm(url):
    """Lower-case host+path, keeping only query params that identify content (YouTube's ?v=)."""
    base, _, query = url.split("#")[0].partition("?")
    keep = "&".join(q for q in query.split("&") if q.split("=")[0] in ("v", "id"))
    base = re.sub(r"^https?://(www\.|m\.)?", "", base).rstrip("/").lower()
    return base + ("?" + keep if keep else "")


def fetch_linkedin(url: str) -> dict:
    r = requests.get(url, headers={"User-Agent": UA, "Accept-Language": "en"}, timeout=20, allow_redirects=True)
    if r.status_code != 200:
        raise RuntimeError(f"LinkedIn HTTP {r.status_code}")
    data = None
    for m in re.finditer(r'<script[^>]*ld\+json[^>]*>(.*?)</script>', r.text, re.S):
        try:
            d = json.loads(m.group(1))
        except json.JSONDecodeError:
            continue
        if isinstance(d, dict) and d.get("datePublished") and d.get("interactionStatistic") is not None:
            data = d
            break
    if not data:
        raise ValueError("LinkedIn page had no public post data (post may be private or deleted)")
    stats = {s.get("interactionType", "").rsplit("/", 1)[-1]: int(s.get("userInteractionCount") or 0)
             for s in data.get("interactionStatistic") or []}
    m = re.search(r'data-num-reactions="(\d+)"', r.text)
    likes = int(m.group(1)) if m else stats.get("LikeAction", 0)
    comments = int(data.get("commentCount") or stats.get("CommentAction", 0))
    author = data.get("creator") or data.get("author") or {}
    author = author.get("name") if isinstance(author, dict) else str(author or "")
    if not author:
        t = re.search(r"<title>[^|<]*\|\s*([^|<]+?)\s*\|", r.text)
        author = t.group(1) if t else ""
    created = datetime.fromisoformat(data["datePublished"].replace("Z", "+00:00")).astimezone(timezone.utc)
    m2 = re.search(r"(?:activity|ugcPost|share)[-:](\d{10,})", r.url)
    return {
        "author": author,
        "text": re.sub(r"\s+", " ", data.get("description") or data.get("articleBody") or data.get("headline") or "")[:300],
        "created_at": created.isoformat(),
        "likes": likes,
        "replies": comments,
        "resolved_key": f"li:{m2.group(1)}" if m2 else None,
        "resolved_url": r.url.split("?")[0],
    }
