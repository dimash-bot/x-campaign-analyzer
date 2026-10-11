"""Recent posts of an X account through Apify (apidojo/tweet-scraper), for when X won't list a timeline
without a logged-in session. Needs APIFY_TOKEN. Costs about $0.0004 per post returned (~$0.016 per account)."""
import os
from datetime import datetime, timezone

import requests

ACTOR = "apidojo~tweet-scraper"


def enabled():
    return bool(os.environ.get("APIFY_TOKEN"))


def _iso(s):
    try:
        return datetime.strptime(s, "%a %b %d %H:%M:%S %z %Y").astimezone(timezone.utc).isoformat()
    except (TypeError, ValueError):
        return ""


def recent_posts(handle, max_items=40):
    """Newest posts by @handle, same shape as XClient.recent_posts plus is_reply / is_retweet flags.
    Reposts carry the reposted post's numbers, so callers should drop them."""
    r = requests.post(
        f"https://api.apify.com/v2/acts/{ACTOR}/run-sync-get-dataset-items",
        params={"token": os.environ["APIFY_TOKEN"], "timeout": 180, "maxTotalChargeUsd": 0.05},
        json={"twitterHandles": [handle], "maxItems": max_items, "sort": "Latest"}, timeout=200)
    if r.status_code >= 400:
        raise RuntimeError(f"Apify HTTP {r.status_code}: {r.text[:160]}")
    out = []
    for t in r.json():
        if not isinstance(t, dict) or not t.get("id") or t.get("noResults"):
            continue
        out.append({
            "tweet_id": str(t["id"]), "author": (t.get("author") or {}).get("userName") or "",
            "url": t.get("url") or f"https://x.com/{handle}/status/{t['id']}",
            "created_at": _iso(t.get("createdAt")),
            "is_reply": bool(t.get("isReply")), "is_retweet": bool(t.get("isRetweet")),
            "views": int(t.get("viewCount") or 0), "likes": int(t.get("likeCount") or 0),
            "retweets": int(t.get("retweetCount") or 0), "quotes": int(t.get("quoteCount") or 0),
            "replies": int(t.get("replyCount") or 0), "bookmarks": int(t.get("bookmarkCount") or 0),
        })
    return out
