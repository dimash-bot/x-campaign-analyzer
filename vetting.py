"""Influencer vetting: the full filter list for one X account, plus the final collab score.

Data:  Apify actor xquik/x-tweet-scraper (sees X's "Paid partnership" label; apidojo does not) for their posts in the
       last VET_DAYS days, their last 20 posts, the replies to those posts, and the replies / quotes / reposts of the
       posts we paid them for.
Judgments: Drex (Nace's decision model) for bio relevance, competitor ties, topic of each post, bot vs real and
       relevant vs not for each commenter (judged from the commenter's profile, not the comment alone), and whether
       each reply is on topic.
Score: every filter becomes a 0-1 score, weighted (WEIGHTS below); filters without data are skipped and the other
       weights scale up. Hard rules (bio clearly unrelated, competitor, black list, "work again: no", no own posts)
       mean No regardless of the score.

Env:   APIFY_TOKEN (shared with apify_client.py), DREX_API_KEY, optional DREX_URL, VET_TZ (default Asia/Tokyo, the time
       zone that defines a "day"), VET_DAYS (20), VET_TTL_DAYS (14), VET_BLACK_LIST (handles), VET_COMPETITORS (names),
       VET_YES / VET_MAYBE (score cutoffs, default 65 / 58).
Cost:  roughly $0.05-0.30 of Apify per account (more for heavy posters) + a fraction of a cent of Drex.
"""
import concurrent.futures as cf
import math
import os
import re
import statistics as st
import time
from datetime import datetime, timedelta, timezone

import requests

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None

APIFY = "https://api.apify.com/v2"
ACTOR = "xquik~x-tweet-scraper"
DREX_URL = os.environ.get("DREX_URL", "https://console.nace.ai/v1/systemone")
DAYS = int(os.environ.get("VET_DAYS", 20))
TTL_DAYS = float(os.environ.get("VET_TTL_DAYS", 14))
YES_CUTOFF = float(os.environ.get("VET_YES", 65))
MAYBE_CUTOFF = float(os.environ.get("VET_MAYBE", 58))
COMPETITOR_REJECT = 0.5
BIO_REJECT = 0.95
COMMENT_PIVOT = 10          # commenter count at which the measured share and a neutral 50% count equally
AUTOMATED_PENALTY = 10      # points off when posting looks automated
COMPETITORS = os.environ.get("VET_COMPETITORS",
                             "OpenRouter, Not Diamond, Martian, Portkey, LiteLLM, Cloudflare AI Gateway, Vercel AI Gateway")
COMPETITOR_KW = re.compile(r"openrouter|not ?diamond|withmartian|portkey|litellm|berriai|ai ?gateway|llm ?router|"
                           r"model ?router|cloudflare|vercel", re.I)
BLACK_LIST = {h.strip().lstrip("@").lower() for h in os.environ.get("VET_BLACK_LIST", "").split(",") if h.strip()}

# Relative weights. Past-experience filters only count once we have paid the creator.
WEIGHTS = {
    "bio": 2, "followers": 3,
    "posts": 2, "avg_per_day": 2.5, "avg_per_active_day": 2.5, "median_per_active_day": 2.5,
    "paid_ratio": 4, "paid_per_day": 3, "burstiness": 2.5,
    "views_total": 3, "views_median": 6, "views_avg": 3, "likes_total": 1.5, "likes_median": 3, "likes_avg": 1.5,
    "topic": 25, "who_comments": 10, "real_commenters": 8, "comment_context": 4,
    "views_per_follower": 3, "likes_per_view": 4, "paid_vs_other": 4,
    "past_vs_median": 6, "past_likes_per_view": 3, "past_cpm": 6, "past_comments": 4, "past_quotes_reposts": 3,
    "work_again": 3,
}


def enabled():
    return bool(os.environ.get("APIFY_TOKEN") and os.environ.get("DREX_API_KEY"))


# ---------------------------------------------------------------- data sources

def _apify(inp, max_usd=0.5, timeout=900):
    """Run the actor, wait for it, return its items (deduplicated by id)."""
    h = {"Authorization": f"Bearer {os.environ['APIFY_TOKEN']}"}
    run = requests.post(f"{APIFY}/acts/{ACTOR}/runs", params={"maxTotalChargeUsd": max_usd}, json=inp, headers=h,
                        timeout=60)
    if run.status_code >= 400:
        raise RuntimeError(f"Apify HTTP {run.status_code}: {run.text[:160]}")
    run = run.json()["data"]
    deadline = time.time() + timeout
    while run["status"] in ("READY", "RUNNING") and time.time() < deadline:
        time.sleep(8)
        run = requests.get(f"{APIFY}/actor-runs/{run['id']}", headers=h, timeout=60).json()["data"]
    items = requests.get(f"{APIFY}/datasets/{run['defaultDatasetId']}/items", params={"clean": "true", "format": "json"},
                         headers=h, timeout=300).json()
    out = {}
    for t in items if isinstance(items, list) else []:
        if isinstance(t, dict) and t.get("id"):
            out[str(t["id"])] = _norm(t)
    return list(out.values())


def _norm(t):
    """xquik field names -> the ones used here."""
    a = t.get("author") if isinstance(t.get("author"), dict) else None
    if a is not None:
        a["userName"] = a.get("userName") or a.get("username") or ""
    else:                                   # a user row (retweeters mode)
        t["userName"] = t.get("userName") or t.get("username") or ""
    t["isQuote"] = bool(t.get("isQuoteStatus") or t.get("isQuote"))
    t["isPaid"] = bool(((t.get("contentDisclosure") or {}).get("advertising") or {}).get("isPaidPromotion"))
    q = t.get("quoted_tweet")
    if isinstance(q, dict) and isinstance(q.get("author"), dict):
        q["author"]["userName"] = q["author"].get("userName") or q["author"].get("username") or ""
    return t


def _drex(state, questions):
    body = {"model": "drex-latest", "state": state, "questions": questions}
    h = {"Authorization": f"Bearer {os.environ['DREX_API_KEY']}"}
    for attempt in range(6):
        try:
            r = requests.post(DREX_URL, json=body, headers=h, timeout=70)
            if r.status_code in (429, 500, 502, 503, 529):
                time.sleep(2 ** attempt)
                continue
            r.raise_for_status()
            return r.json()["answers"]
        except requests.RequestException:
            if attempt == 5:
                raise
            time.sleep(2 ** attempt)
    raise RuntimeError("Drex kept failing")


# ---------------------------------------------------------------- helpers

def _ts(s, tz):
    return datetime.strptime(s, "%a %b %d %H:%M:%S %z %Y").astimezone(tz)


def _med(xs):
    return st.median(xs) if xs else 0


def _pct(a, b):
    return round(a / b, 4) if b else None


def _kind(t, h):
    if t.get("isReply"):
        return "thread" if (t.get("inReplyToUsername") or "").lower() == h.lower() else "reply"
    return "quote" if t.get("isQuote") else "single"


def _text(t):
    return t.get("fullText") or t.get("text") or ""


def _profile_state(a, extra=""):
    created = a.get("createdAt")
    try:
        cd = datetime.strptime(created, "%a %b %d %H:%M:%S %z %Y")
        age = max((datetime.now(timezone.utc) - cd).days, 1)
        made = f"Account created {cd:%Y-%m-%d} ({age} days ago). "
        rate = f"Total posts: {a.get('statusesCount')} ({(a.get('statusesCount') or 0) / age:.1f} per day). "
    except (TypeError, ValueError):
        made, rate = "", f"Total posts: {a.get('statusesCount')}. "
    return (f"X account @{a.get('userName')} ({a.get('name')}). Bio: {a.get('description') or '(empty)'}. "
            f"Location: {a.get('location') or '-'}. Followers: {a.get('followers')}, following: {a.get('following')}. "
            f"{made}{rate}Blue check: {a.get('isBlueVerified')}. {extra}")


BOT_Q = {"type": "noul", "instructions": "Is this account a bot, spam, engagement-farming or otherwise inauthentic account "
         "(not a real person or real organization)? Judge from the whole profile, not only the reply text."}
REL_Q = {"type": "noul", "instructions": "Is this account run by someone in AI, software development, ML, data or "
         "cybersecurity (a developer, engineer, researcher, tech founder or tech company)?"}


def _judge_people(people, h):
    """{username: profile + their replies} -> {username: {"bot": p, "relevant": p}} (Drex, from profiles)."""
    def one(item):
        u, (a, texts) = item
        if u.lower() in BLACK_LIST:
            return u, {"bot": 1.0, "relevant": 0.0}
        extra = (f"Replies this account left on @{h}'s posts ({len(texts)}):\n" + "\n".join(f"- {x[:400]}" for x in texts[:5])) if texts else ""
        ans = _drex(_profile_state(a, extra), {"bot": BOT_Q, "relevant_person": REL_Q})
        return u, {"bot": ans["bot"]["noul"], "relevant": ans["relevant_person"]["noul"]}
    with cf.ThreadPoolExecutor(6) as ex:
        return dict(ex.map(one, people.items()))


# ---------------------------------------------------------------- the filters

def vet(handle, profile=None, paid_tweet_ids=()):
    """All filters for @handle. `profile` = what the app already knows (followers, bio, name); `paid_tweet_ids` = our
    paid posts with them, for the past-experience filters. Returns a JSON-able dict."""
    tz = ZoneInfo(os.environ.get("VET_TZ", "Asia/Tokyo")) if ZoneInfo else timezone.utc
    today = datetime.now(tz).replace(hour=0, minute=0, second=0, microsecond=0)
    lo, hi = today - timedelta(days=DAYS), today          # the last DAYS full days
    own = lambda t: (t.get("author") or {}).get("userName", "").lower() == handle.lower()  # noqa: E731
    inwin = lambda t: lo <= _ts(t["createdAt"], tz) < hi  # noqa: E731

    tl = _apify({"searchTerms": [f"from:{handle} since:{(lo - timedelta(days=1)).date()} until:{(hi + timedelta(days=1)).date()} include:nativeretweets"],
                 "queryType": "Latest + Top", "maxItems": 2000, "outputVariant": "rich"}, max_usd=1.0)
    win = [t for t in tl if own(t) and t.get("createdAt") and inwin(t)]
    posts = [t for t in win if not t.get("isRetweet") and _kind(t, handle) != "reply"]
    reposts = [t for t in win if t.get("isRetweet")]
    replies_out = [t for t in win if not t.get("isRetweet") and _kind(t, handle) == "reply"]
    counted = [t for t in tl if own(t) and t.get("createdAt") and not t.get("isRetweet") and _kind(t, handle) != "reply"
               and _ts(t["createdAt"], tz) < hi]
    if len(counted) < 20:                                  # quiet account: reach back for their last 20 posts
        older = _apify({"searchTerms": [f"from:{handle} until:{lo.date()} -filter:nativeretweets"], "queryType": "Latest",
                        "maxItems": 60, "outputVariant": "rich"}, max_usd=0.1)
        seen = {t["id"] for t in counted}
        counted += [t for t in older if own(t) and t.get("createdAt") and not t.get("isRetweet")
                    and _kind(t, handle) != "reply" and t["id"] not in seen]
    last20 = sorted(counted, key=lambda t: int(t["id"]))[-20:]
    author = next((t["author"] for t in sorted(tl, key=lambda t: -int(t["id"])) if own(t)), {}) or {}
    prof = {**{k: author.get(k) for k in ("name", "description", "location", "followers")}, **{k: v for k, v in (profile or {}).items() if v}}
    followers = prof.get("followers")

    # --- last DAYS days
    daily = [0] * DAYS
    for t in posts:
        daily[(_ts(t["createdAt"], tz) - lo).days] += 1
    active = [c for c in daily if c]
    mean = sum(daily) / DAYS
    paid = [t for t in posts if t["isPaid"]]

    # --- last 20 posts
    views = [t.get("viewCount") or 0 for t in last20]
    likes = [t.get("likeCount") or 0 for t in last20]
    pv = [t.get("viewCount") or 0 for t in last20 if t["isPaid"]]
    ov = [t.get("viewCount") or 0 for t in last20 if not t["isPaid"]]

    # --- Drex: bio + competitor
    bio_txt = f"{prof.get('name')} | {prof.get('description')}"
    bio = _drex(f"X account @{handle} ({prof.get('name')}). Bio: {prof.get('description')}. Location: {prof.get('location')}. "
                f"Followers: {followers}.", {
        "unrelated": {"type": "noul", "instructions": "Is this account clearly and completely unrelated to technology, software, "
                      "the internet industry or AI, so that it could never be a fit for promoting an AI developer tool?"},
        "competitor": {"type": "noul", "instructions": f"Does this person work for, found, or officially represent {COMPETITORS}, "
                       "or any other LLM router / AI gateway company?"}})

    # --- Drex: topic of each of the last 20 posts
    topic_q = {"type": "choice", "instructions": "What is the main topic of this X post?", "criteria": {
        "ai_dev_tools": "LLMs, AI models, APIs, coding with AI, AI agents, AI developer tools or infrastructure",
        "ai_general": "General AI news, AI use in business or daily life, AI images/video, not developer-focused",
        "ai_policy_security": "AI policy, AI safety, cybersecurity policy or AI regulation",
        "civic_politics": "Civic tech, politics, government", "other": None}}
    rel_q = {"type": "noul", "instructions": "Is this post relevant to an audience of developers or AI practitioners who might "
             "use an LLM router (a service that picks the best AI model per request)?"}

    def post_topic(t):
        q = t.get("quote") or t.get("quoted_tweet") or {}
        s = f"Post by @{handle}: {_text(t)}"
        if q:
            s += f"\nQuoted post by @{(q.get('author') or {}).get('userName')}: {_text(q)}"
        return _drex(s, {"topic": topic_q, "relevant": rel_q})

    # --- replies to the last 20 posts (direct replies; promoted ads never appear in this mode)
    reps = []
    if last20:
        reps = [r for r in _apify({"mode": "replies", "replyTweetIds": [t["id"] for t in last20], "maxItems": 800,
                                   "outputVariant": "rich"}, max_usd=0.4)
                if r.get("isReply") and (r.get("author") or {}).get("userName", "").lower() != handle.lower()]
    by_id = {t["id"]: t for t in last20}
    people = {}
    for r in reps:
        a = r["author"]
        people.setdefault(a["userName"], (a, []))[1].append(_text(r))

    def on_topic(r):
        p = by_id.get(str(r.get("inReplyToId"))) or {}
        return _drex(f"Original post by @{handle}: {_text(p)[:800]}\nReply by @{r['author']['userName']}: {_text(r)[:600]}",
                     {"on_topic": {"type": "noul", "instructions": "Does the reply engage with the topic of the original post "
                                   "(agreement, disagreement, question or added information all count)? Off-topic promotion, "
                                   "spam or unrelated content does not."}})["on_topic"]["noul"]

    with cf.ThreadPoolExecutor(6) as ex:
        topics = list(ex.map(post_topic, last20))
        ontop = list(ex.map(on_topic, reps))
    judged = _judge_people(people, handle)
    n = len(judged)
    real = [u for u, v in judged.items() if v["bot"] < 0.5]
    relevant = [u for u, v in judged.items() if v["relevant"] >= 0.5]

    # --- past experience: replies, quotes and reposts of the posts we paid them for
    past = {"past_relevant_comments": None, "past_relevant_quotes_reposts": None}
    pids = [str(i) for i in paid_tweet_ids if i][:20]
    if pids:
        pr = [r for r in _apify({"mode": "replies", "replyTweetIds": pids, "maxItems": 400, "outputVariant": "rich"}, max_usd=0.2)
              if r.get("isReply") and (r.get("author") or {}).get("userName", "").lower() != handle.lower()]
        commenters = {}
        for r in pr:
            commenters.setdefault(r["author"]["userName"], (r["author"], []))[1].append(_text(r))
        j = _judge_people(commenters, handle)
        past["past_relevant_comments"] = sum(1 for v in j.values() if v["bot"] < 0.5 and v["relevant"] >= 0.5)
        amplifiers = {}
        for pid in pids:
            for q in _apify({"quoted_tweet_id": pid, "queryType": "Latest", "maxItems": 100, "outputVariant": "rich"}, max_usd=0.05):
                a = q.get("author") or {}
                if a.get("userName") and a["userName"].lower() != handle.lower():
                    amplifiers.setdefault(a["userName"], (a, []))
        for u in _apify({"mode": "retweeters", "retweeterTweetIds": pids, "maxItems": 300, "outputVariant": "rich"}, max_usd=0.1):
            if u.get("userName") and u["userName"].lower() != handle.lower():
                amplifiers.setdefault(u["userName"], (u, []))
        j = _judge_people(amplifiers, handle)
        past["past_relevant_quotes_reposts"] = sum(1 for v in j.values() if v["bot"] < 0.5 and v["relevant"] >= 0.5)

    topic_counts = {}
    for a in topics:
        topic_counts[a["topic"]["choice"]] = topic_counts.get(a["topic"]["choice"], 0) + 1
    automated = _automated(posts, handle, tz)
    return {
        "window": f"{lo:%Y-%m-%d} – {(hi - timedelta(days=1)):%Y-%m-%d} ({tz})",
        # general
        "followers": followers,
        "bio_unrelated_p": round(bio["unrelated"]["noul"], 3),
        "bio_decision": "reject" if bio["unrelated"]["noul"] >= BIO_REJECT else "keep",
        "competitor_p": round(bio["competitor"]["noul"], 3),
        "competitor_keywords": ", ".join(sorted({m.lower() for m in COMPETITOR_KW.findall(bio_txt)})),
        "black_list": handle.lower() in BLACK_LIST,
        # last DAYS days
        "posts": len(posts), "reposts": len(reposts), "replies_to_others": len(replies_out),
        "by_type": {k: sum(1 for t in posts if _kind(t, handle) == k) for k in ("single", "quote", "thread")},
        "avg_per_day": round(mean, 2), "active_days": len(active),
        "avg_per_active_day": round(sum(active) / len(active), 2) if active else 0,
        "median_per_active_day": _med(active),
        "paid_posts": len(paid), "paid_ratio": _pct(len(paid), len(posts)) or 0, "paid_per_day": round(len(paid) / DAYS, 2),
        "burstiness": round(sum(abs(c - mean) for c in daily) / DAYS / mean, 2) if mean else None,
        "automated": automated,
        # last 20 posts
        "last20_n": len(last20), "last20_in_window": sum(1 for t in last20 if inwin(t)),
        "views_total": sum(views), "views_median": _med(views), "views_avg": round(sum(views) / len(views)) if views else 0,
        "likes_total": sum(likes), "likes_median": _med(likes), "likes_avg": round(sum(likes) / len(likes)) if likes else 0,
        "relevant_posts": sum(1 for a in topics if a["relevant"]["noul"] >= 0.5), "topics": topic_counts,
        "commenters": n, "replies_scraped": len(reps),
        "relevant_commenters_pct": _pct(len(relevant), n), "real_commenters_pct": _pct(len(real), n),
        "blacklisted_commenters": sum(1 for u in judged if u.lower() in BLACK_LIST),
        "on_topic_pct": _pct(sum(1 for p in ontop if p >= 0.5), len(reps)),
        "views_per_follower": _pct(_med(views), followers) if followers else None,
        "likes_per_view": _pct(_med(likes), _med(views)),
        "paid_vs_other": round(_med(pv) / _med(ov), 2) if pv and ov and _med(ov) else None,
        **past,
    }


def _automated(posts, handle, tz):
    """Lots of thread replies posted seconds apart, every day: an automated pipeline rather than a person."""
    th = sorted(_ts(t["createdAt"], tz) for t in posts if _kind(t, handle) == "thread")
    if len(th) < 200:
        return False
    fast = sum(1 for a, b in zip(th, th[1:]) if (b - a).total_seconds() < 10)
    return fast / len(th) > 0.3


# ---------------------------------------------------------------- the final score

def _clip(x):
    return max(0.0, min(1.0, x))


def _log(v, lo, hi):
    return 0.0 if not v or v <= 0 else _clip((math.log10(v) - math.log10(lo)) / (math.log10(hi) - math.log10(lo)))


def _sweet(x, lo, hi, zero):
    """0 at nothing, 1 from lo to hi, down to 0.3 at `zero` (spam-level volume)."""
    if not x or x <= 0:
        return 0.0
    if x < lo:
        return x / lo
    if x <= hi:
        return 1.0
    return max(0.3, 1 - 0.7 * (x - hi) / (zero - hi))


def _shrink(p, n):
    return (0.5 * COMMENT_PIVOT + (p or 0) * n) / (COMMENT_PIVOT + n)


def filter_scores(v, past):
    """Each filter -> 0-1, or None when there is no data (the filter is then skipped)."""
    n = v.get("commenters") or 0
    num = lambda x: x if isinstance(x, (int, float)) else None  # noqa: E731
    s = {
        "bio": 1 - v["bio_unrelated_p"] if num(v.get("bio_unrelated_p")) is not None else None,
        "followers": _log(v.get("followers"), 1000, 300000) if v.get("followers") else None,
        "posts": _sweet(v.get("posts"), 20, 200, 1000),
        "avg_per_day": _sweet(v.get("avg_per_day"), 1, 10, 50),
        "avg_per_active_day": _sweet(v.get("avg_per_active_day"), 1, 10, 50),
        "median_per_active_day": _sweet(v.get("median_per_active_day"), 1, 10, 50),
        "paid_ratio": 1 - _clip((v.get("paid_ratio") or 0) / 0.5) if v.get("posts") else None,
        "paid_per_day": 1 - _clip((v.get("paid_per_day") or 0) / 1),
        "burstiness": 1 - _clip(v["burstiness"]) if num(v.get("burstiness")) is not None and (v.get("avg_per_day") or 0) >= 2 else None,
        "views_total": _log(v.get("views_total"), 2000, 1_000_000),
        "views_median": _log(v.get("views_median"), 100, 50_000),
        "views_avg": _log(v.get("views_avg"), 100, 100_000),
        "likes_total": _log(v.get("likes_total"), 20, 10_000),
        "likes_median": _log(v.get("likes_median"), 1, 500),
        "likes_avg": _log(v.get("likes_avg"), 1, 1000),
        "topic": (v.get("relevant_posts") or 0) / 20,
        "who_comments": _shrink(v.get("relevant_commenters_pct"), n),
        "real_commenters": _shrink(v.get("real_commenters_pct"), n),
        "comment_context": _shrink(v.get("on_topic_pct"), n),
        "views_per_follower": _clip((v.get("views_per_follower") or 0) / 0.3),
        "likes_per_view": _clip((v.get("likes_per_view") or 0) / 0.02),
        "paid_vs_other": _clip(v["paid_vs_other"]) if num(v.get("paid_vs_other")) is not None else None,
        "past_vs_median": _clip(past["vs_median"]) if num(past.get("vs_median")) is not None else None,
        "past_likes_per_view": _clip(past["likes_per_view"] / 0.02) if num(past.get("likes_per_view")) is not None else None,
        "past_cpm": _clip((50 - past["cpm"]) / 45) if num(past.get("cpm")) is not None else None,
        "past_comments": _clip(v["past_relevant_comments"] / 10) if num(v.get("past_relevant_comments")) is not None else None,
        "past_quotes_reposts": _clip(v["past_relevant_quotes_reposts"] / 5) if num(v.get("past_relevant_quotes_reposts")) is not None else None,
        "work_again": {"yes": 1.0, "no": 0.0}.get(past.get("work_again")),
    }
    return s


def collab_score(v, past):
    """-> {"score", "decision": Yes|Maybe|No, "reason"}. `v` = vet() output, `past` = how our paid posts did
    (vs_median, likes_per_view, cpm, work_again)."""
    if not v:
        return None
    s = filter_scores(v, past)
    have = {k: x for k, x in s.items() if x is not None}
    den = sum(WEIGHTS[k] for k in have)
    score = 100 * sum(WEIGHTS[k] * x for k, x in have.items()) / den if den else 0
    if v.get("automated"):
        score -= AUTOMATED_PENALTY
    score = round(score, 1)
    if v.get("bio_decision") == "reject":
        return {"score": score, "decision": "No", "reason": "bio clearly unrelated"}
    if (v.get("competitor_p") or 0) >= COMPETITOR_REJECT:
        return {"score": score, "decision": "No", "reason": f"competitor (Drex p={v['competitor_p']:.2f})"}
    if v.get("black_list"):
        return {"score": score, "decision": "No", "reason": "on the black list"}
    if past.get("work_again") == "no":
        return {"score": score, "decision": "No", "reason": "past collab: don't work again"}
    if not v.get("last20_n"):
        return {"score": score, "decision": "No", "reason": "no own posts to evaluate"}
    d = "Yes" if score >= YES_CUTOFF else "Maybe" if score >= MAYBE_CUTOFF else "No"
    return {"score": score, "decision": d,
            "reason": f"score {score} vs cutoffs {YES_CUTOFF:g} (Yes) / {MAYBE_CUTOFF:g} (Maybe)"
                      + (f"; −{AUTOMATED_PENALTY} automated posting" if v.get("automated") else "")}
