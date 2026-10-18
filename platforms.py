"""Which platform a post link belongs to, and how to fetch its public metrics.

  X         -> x_client (exact views, likes, reposts, quotes, replies, bookmarks)
  LinkedIn  -> public post page's JSON-LD: reactions, comments, post time. Impressions aren't public,
               so they're entered by hand (from the creator's analytics screenshot).
  other     -> a link from any other site (YouTube, TikTok, Instagram, Reddit, Telegram, a blog...): metrics typed
               in by hand; the author is guessed from the URL where it carries one, and can be edited.

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

# Links that are never a post: payments, docs, scheduling (they show up in payment sheets next to the post link)
NOT_POST_HOST = re.compile(r"(?:^|\.)(?:paypal\.(?:com|me)|wise\.com|stripe\.com|revolut\.(?:com|me)|calendly\.com|zoom\.us|"
                           r"docs\.google\.com|drive\.google\.com|sheets\.google\.com|meet\.google\.com|cal\.com)$", re.I)
URL_RE = re.compile(r"^https?://([a-z0-9-]+(?:\.[a-z0-9-]+)*\.[a-z]{2,})(?::\d+)?(/\S*)?$", re.I)

SITE_NAMES = [("youtube.com", "YouTube"), ("youtu.be", "YouTube"), ("tiktok.com", "TikTok"), ("instagram.com", "Instagram"),
              ("reddit.com", "Reddit"), ("threads.net", "Threads"), ("threads.com", "Threads"), ("bsky.app", "Bluesky"),
              ("news.ycombinator.com", "Hacker News"), ("producthunt.com", "Product Hunt"), ("medium.com", "Medium"),
              ("substack.com", "Substack"), ("t.me", "Telegram"), ("telegram.me", "Telegram"), ("facebook.com", "Facebook"),
              ("fb.watch", "Facebook"), ("discord.com", "Discord"), ("discord.gg", "Discord"), ("dev.to", "DEV"),
              ("hashnode.dev", "Hashnode"), ("github.com", "GitHub"), ("twitch.tv", "Twitch"), ("kick.com", "Kick"),
              ("quora.com", "Quora"), ("pinterest.com", "Pinterest"), ("vk.com", "VK"), ("bilibili.com", "Bilibili"),
              ("weibo.com", "Weibo"), ("zhihu.com", "Zhihu"), ("xiaohongshu.com", "Xiaohongshu"), ("habr.com", "Habr"),
              ("mastodon.social", "Mastodon"), ("warpcast.com", "Farcaster"), ("snapchat.com", "Snapchat"),
              ("spotify.com", "Spotify"), ("podcasts.apple.com", "Apple Podcasts"), ("discourse", "Forum")]


def host_of(url):
    m = URL_RE.match(url if re.match(r"https?://", url) else "https://" + url)
    return m.group(1).lower().removeprefix("www.").removeprefix("m.") if m else ""


def site_name(url):
    """'YouTube', 'Telegram'… or the bare domain for sites we don't know."""
    h = host_of(url)
    for dom, name in SITE_NAMES:
        if h == dom or h.endswith("." + dom):
            return name
    return h or "Other"


def author_from_url(url):
    """The account a post belongs to, when the link spells it out (best effort, editable later)."""
    h, path = host_of(url), re.sub(r"^https?://[^/]+", "", url.split("?")[0])
    pats = [(r"tiktok\.com|threads\.|medium\.com|youtube\.com", r"^/@([\w.\-]+)"),
            (r"bsky\.app", r"^/profile/([\w.\-]+)"), (r"^t\.me$|telegram\.me", r"^/(?:s/)?([\w]+)/\d"),
            (r"reddit\.com", r"^/(?:u|user)/([\w\-]+)"), (r"dev\.to|github\.com|habr\.com|warpcast\.com", r"^/([\w\-]+)/")]
    for host_re, path_re in pats:
        if re.search(host_re, h):
            m = re.search(path_re, path)
            if m:
                return m.group(1)
    m = re.match(r"^([\w\-]+)\.(?:substack\.com|hashnode\.dev|medium\.com)$", h)
    if m and m.group(1) not in ("www", "open"):
        return m.group(1)
    if re.search(r"reddit\.com", h):
        m = re.search(r"^/r/([\w\-]+)/", path)
        return "r/" + m.group(1) if m else ""
    return ""


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
    host = host_of(full)
    if not host or re.search(r"(?:^|\.)(?:x|twitter)\.com$|(?:^|\.)linkedin\.com$", host) or NOT_POST_HOST.search(host):
        return None   # X / LinkedIn profiles aren't posts; payment and doc links aren't either
    if not (URL_RE.match(full).group(2) or "").strip("/"):
        return None   # a bare homepage isn't a post
    return "other", "url:" + _norm_any(full), full


def _norm_any(url):
    """Like _norm, but for unknown sites keep every query param except tracking ones (forums use ?t=123 etc.)."""
    base, _, query = url.split("#")[0].partition("?")
    keep = sorted(q for q in query.split("&") if q and not re.match(r"(?:utm_\w+|ref|ref_src|fbclid|gclid|si|s|igsh|share_id)=", q, re.I))
    base = re.sub(r"^https?://(www\.|m\.)?", "", base).rstrip("/").lower()
    return base + ("?" + "&".join(keep) if keep else "")


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
