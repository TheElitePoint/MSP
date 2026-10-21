"""Shared page-title -> company-name heuristic.

Used by every discovery provider that only has a page title to work with
(Tavily's search result title, Apify's scraped page title) — the crawler
later replaces this with the name found on the site itself where available
(pipeline/nameres.py), so this only needs to be a plausible first guess.
"""

from __future__ import annotations

_TITLE_SEPARATORS = ["|", " - ", " – ", " — ", "::", " • "]

_TITLE_NOISE = {
    "home", "homepage", "welcome", "index", "official site", "official website",
    "managed it services", "it services", "it support", "services", "about us",
    "contact us", "cybersecurity", "top 10", "best", "reviews",
}


def clean_company_name(title: str) -> str | None:
    """Extract a plausible company name from a page title.

    Page titles are usually "Acme Technologies | Managed IT Services in Dallas".
    The company name is normally the first or last segment.
    """
    title = title.strip()
    if not title:
        return None

    parts = [title]
    for sep in _TITLE_SEPARATORS:
        if sep in title:
            parts = [p.strip() for p in title.split(sep) if p.strip()]
            break

    def score(part: str) -> int:
        low = part.lower().strip(" .-")
        if low in _TITLE_NOISE or len(part) < 3 or len(part) > 60:
            return -1
        # Prefer short, capitalised, non-descriptive segments.
        points = 10
        words = part.split()
        if len(words) > 7:
            points -= 6
        if any(noise in low for noise in ("services in", "top ", "best ", "vs ", "how to", "why ")):
            points -= 5
        if any(w[:1].isupper() for w in words):
            points += 3
        return points

    ranked = sorted(parts, key=score, reverse=True)
    best = ranked[0] if ranked else title
    if score(best) < 0:
        return None
    return best.strip(" .-|")[:120] or None
