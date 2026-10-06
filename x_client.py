"""Fetch exact engagement metrics for a single X post via x.com's internal
GraphQL API (TweetResultByRestId) using an anonymous guest token.

Unofficial endpoint: fine for campaign tracking, but X can rotate the query ID.
If fetches start failing with 404 "Query not found", update QID below.
"""
import json
import os
import re
import time
from datetime import datetime, timezone

import requests

BEARER = ("AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs"
          "%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA")
QIDS = ["Xl5pC_lBk_gcO2ItU39DQw", "zy39CwTyYhU-_0LP7dljjg", "7xflPyRiUxGVbJd4uWmbfg"]
QID_USER = "G3KGOASz96M-Qu0nwmGXNg"      # UserByScreenName — works anonymously
QID_USER_TWEETS = "V7H0Ap3_Hh2FyS75OCDO3Q"   # UserTweets — X only serves this to logged-in sessions

FEATURES_USER = {
    "hidden_profile_subscriptions_enabled": True, "rweb_tipjar_consumption_enabled": True,
    "responsive_web_graphql_exclude_directive_enabled": True, "verified_phone_label_enabled": False,
    "subscriptions_verification_info_is_identity_verified_enabled": True,
    "subscriptions_verification_info_verified_since_enabled": True, "highlights_tweets_tab_ui_enabled": True,
    "responsive_web_twitter_article_notes_tab_enabled": True, "subscriptions_feature_can_gift_premium": True,
    "creator_subscriptions_tweet_preview_api_enabled": True,
    "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
    "responsive_web_graphql_timeline_navigation_enabled": True,
}


class NeedsLogin(Exception):
    """X only shows this data to logged-in sessions (set X_AUTH_TOKEN / X_CT0)."""

FEATURES = {
    "creator_subscriptions_tweet_preview_api_enabled": True,
    "premium_content_api_read_enabled": False,
    "communities_web_enable_tweet_community_results_fetch": True,
    "c9s_tweet_anatomy_moderator_badge_enabled": True,
    "articles_preview_enabled": True,
    "responsive_web_edit_tweet_api_enabled": True,
    "graphql_is_translatable_rweb_tweet_is_translatable_enabled": True,
    "view_counts_everywhere_api_enabled": True,
    "longform_notetweets_consumption_enabled": True,
    "responsive_web_twitter_article_tweet_consumption_enabled": True,
    "tweet_awards_web_tipping_enabled": False,
    "freedom_of_speech_not_reach_fetch_enabled": True,
    "standardized_nudges_misinfo": True,
    "tweet_with_visibility_results_prefer_gql_limited_actions_policy_enabled": True,
    "rweb_video_timestamps_enabled": True,
    "longform_notetweets_rich_text_read_enabled": True,
    "longform_notetweets_inline_media_enabled": True,
    "rweb_tipjar_consumption_enabled": True,
    "responsive_web_graphql_exclude_directive_enabled": True,
    "verified_phone_label_enabled": False,
    "creator_subscriptions_quote_tweet_preview_enabled": False,
    "responsive_web_graphql_skip_user_profile_image_extensions_enabled": False,
    "tweetypie_unmention_optimization_enabled": True,
    "responsive_web_graphql_timeline_navigation_enabled": True,
    "responsive_web_enhance_cards_enabled": False,
}


def tweet_id_from(url: str):
    m = re.search(r"/status(?:es)?/(\d+)", url) or re.fullmatch(r"\s*(\d{5,})\s*", url)
    return m.group(1) if m else None


def _num(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return 0


class XClient:
    """Anonymous guest mode by default. If X blocks guests (common from cloud/datacenter IPs) and
    X_AUTH_TOKEN + X_CT0 cookies of a logged-in (spare) account are set, falls back to those."""

    def __init__(self):
        self.s = requests.Session()
        self.s.headers.update({"Authorization": f"Bearer {BEARER}", "User-Agent": "Mozilla/5.0"})
        self.cookies = (os.environ.get("X_AUTH_TOKEN"), os.environ.get("X_CT0"))
        self.mode = "guest"
        try:
            self._guest()
        except requests.RequestException:
            if not self._use_cookies():
                raise

    def _use_cookies(self) -> bool:
        auth_token, ct0 = self.cookies
        if not (auth_token and ct0) or self.mode == "cookies":
            return False
        self.s.headers.pop("x-guest-token", None)
        self.s.headers.update({"x-csrf-token": ct0, "x-twitter-auth-type": "OAuth2Session",
                               "x-twitter-active-user": "yes"})
        self.s.cookies.update({"auth_token": auth_token, "ct0": ct0})
        self.mode = "cookies"
        print("x_client: guest access failed, switched to account cookies", flush=True)
        return True

    def _guest(self):
        r = self.s.post("https://api.twitter.com/1.1/guest/activate.json", timeout=15)
        r.raise_for_status()
        self.s.headers["x-guest-token"] = r.json()["guest_token"]

    def fetch(self, tweet_id: str) -> dict:
        variables = {"tweetId": tweet_id, "withCommunity": False,
                     "includePromotedContent": False, "withVoice": False}
        params = {"variables": json.dumps(variables), "features": json.dumps(FEATURES)}
        last_err = "unknown error"
        for attempt in range(4):
            qid = QIDS[min(attempt, len(QIDS) - 1)]
            r = self.s.get(f"https://api.twitter.com/graphql/{qid}/TweetResultByRestId",
                           params=params, timeout=20)
            if r.status_code == 200:
                res = r.json().get("data", {}).get("tweetResult", {}).get("result")
                if not res:
                    raise ValueError("post not found (deleted, private, or age-gated)")
                return _parse(res)
            last_err = f"HTTP {r.status_code}: {r.text[:120]}"
            if r.status_code in (401, 403, 429):
                if self.mode == "guest":
                    try:
                        self._guest()
                    except requests.RequestException:
                        self._use_cookies()
                time.sleep(2)
        if self._use_cookies():
            return self.fetch(tweet_id)
        raise RuntimeError(last_err)


    def profile(self, handle: str) -> dict:
        """Public profile numbers for @handle (works without login)."""
        params = {"variables": json.dumps({"screen_name": handle.lstrip("@"), "withSafetyModeUserFields": True}),
                  "features": json.dumps(FEATURES_USER)}
        for _ in range(3):
            r = self.s.get(f"https://api.twitter.com/graphql/{QID_USER}/UserByScreenName", params=params, timeout=20)
            if r.status_code == 200:
                break
            if r.status_code in (401, 403, 429) and self.mode == "guest":
                self._guest()
                time.sleep(2)
        else:
            raise RuntimeError(f"profile HTTP {r.status_code}")
        u = (r.json().get("data") or {}).get("user", {}).get("result")
        if not u or u.get("__typename") != "User":
            raise ValueError(f"@{handle} not found or suspended")
        leg, core = u.get("legacy", {}), u.get("core", {})
        return {
            "user_id": u.get("rest_id"),
            "name": core.get("name") or leg.get("name") or handle,
            "handle": core.get("screen_name") or leg.get("screen_name") or handle,
            "followers": _num(leg.get("followers_count")),
            "following": _num(leg.get("friends_count")),
            "posts_count": _num(leg.get("statuses_count")),
            "verified": bool(u.get("is_blue_verified") or leg.get("verified")),
            "avatar": (u.get("avatar") or {}).get("image_url") or leg.get("profile_image_url_https") or "",
        }

    def recent_posts(self, user_id: str, handle: str, count: int = 40) -> list:
        """The account's own recent original posts (no retweets/replies). Needs a logged-in session."""
        variables = {"userId": user_id, "count": count, "includePromotedContent": False,
                     "withQuickPromoteEligibilityTweetFields": True, "withVoice": True, "withV2Timeline": True}
        r = self.s.get(f"https://api.twitter.com/graphql/{QID_USER_TWEETS}/UserTweets",
                       params={"variables": json.dumps(variables), "features": json.dumps(FEATURES)}, timeout=25)
        if r.status_code != 200:
            raise RuntimeError(f"timeline HTTP {r.status_code}")
        if "TimelineTerminateTimeline" in r.text and '"__typename":"Tweet"' not in r.text:
            raise NeedsLogin("X hides timelines from logged-out sessions")
        if "profileBestHighlights" in r.text:
            # Logged-out sessions get the account's all-time best posts instead of recent ones —
            # a median of those is wildly inflated, so treat it as "no data".
            raise NeedsLogin("X only shows logged-out sessions this account's highlights, not recent posts")
        out = {}

        def walk(o):
            if isinstance(o, dict):
                if o.get("__typename") == "Tweet" and "legacy" in o:
                    leg = o["legacy"]
                    if not (leg.get("retweeted_status_result") or leg.get("in_reply_to_status_id_str")
                            or leg.get("full_text", "").startswith("RT @")):
                        try:
                            t = _parse(o)
                            if t["author"].lower() == handle.lower() and t["tweet_id"] not in out:
                                out[t["tweet_id"]] = t
                        except (ValueError, KeyError):
                            pass
                for v in o.values():
                    walk(v)
            elif isinstance(o, list):
                for v in o:
                    walk(v)
        walk(r.json())
        return sorted(out.values(), key=lambda t: t["created_at"], reverse=True)


def _parse(res: dict) -> dict:
    if res.get("__typename") == "TweetWithVisibilityResults":
        res = res["tweet"]
    if res.get("__typename") == "TweetTombstone":
        raise ValueError("post unavailable")
    legacy = res.get("legacy", {})
    user_res = res.get("core", {}).get("user_results", {}).get("result", {})
    handle = user_res.get("legacy", {}).get("screen_name") or user_res.get("core", {}).get("screen_name", "")
    note = res.get("note_tweet", {}).get("note_tweet_results", {}).get("result", {})
    created = legacy.get("created_at", "")
    try:
        created_iso = datetime.strptime(created, "%a %b %d %H:%M:%S %z %Y").astimezone(timezone.utc).isoformat()
    except ValueError:
        created_iso = ""
    return {
        "tweet_id": res.get("rest_id") or legacy.get("id_str"),
        "author": handle,
        "text": (note.get("text") or legacy.get("full_text", "")).replace("\n", " ").strip(),
        "created_at": created_iso,
        "views": _num(res.get("views", {}).get("count")),
        "likes": _num(legacy.get("favorite_count")),
        "retweets": _num(legacy.get("retweet_count")),
        "quotes": _num(legacy.get("quote_count")),
        "replies": _num(legacy.get("reply_count")),
        "bookmarks": _num(legacy.get("bookmark_count")),
    }


if __name__ == "__main__":
    import sys
    c = XClient()
    for arg in sys.argv[1:]:
        print(json.dumps(c.fetch(tweet_id_from(arg)), indent=2))
