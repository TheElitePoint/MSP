"""Deduplication keys and matching.

This is the safeguard that stops the same company being collected twice across
runs (SOP Module 39 list hygiene, Module 63 "check duplicates by company
website, LinkedIn URL, and parent company").

Four independent keys are computed for every candidate. A candidate is a
duplicate if ANY key matches an existing record:

    domain_key  registrable domain of the website  (strongest)
    name_key    legal-suffix-stripped, normalised company name + state
    phone_key   digits of the primary phone number
    fuzzy       rapidfuzz ratio on name_key within the same state

Aggregator/directory domains are never eligible to become company records.
"""

from __future__ import annotations

import re
from urllib.parse import urlparse

import tldextract
from rapidfuzz import fuzz

# Bundled PSL snapshot only — never phones home mid-run.
_extract = tldextract.TLDExtract(suffix_list_urls=())

LEGAL_SUFFIXES = {
    "inc", "incorporated", "llc", "l.l.c", "ltd", "limited", "corp", "corporation",
    "co", "company", "plc", "llp", "lp", "pllc", "pc", "group", "holdings",
    "holding", "partners", "the", "a", "an",
}

_WS = re.compile(r"\s+")
_NON_ALNUM = re.compile(r"[^a-z0-9\s]")
_PHONE_DIGITS = re.compile(r"\D")


def canonical_url(url: str | None) -> str | None:
    """Normalise a URL to https://host/path with no tracking noise."""
    if not url:
        return None
    url = url.strip()
    if not url:
        return None
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    try:
        parsed = urlparse(url)
    except ValueError:
        return None
    host = (parsed.netloc or "").lower().split(":")[0]
    if host.startswith("www."):
        host = host[4:]
    if not host or "." not in host:
        return None
    path = (parsed.path or "").rstrip("/")
    return f"https://{host}{path}" if path else f"https://{host}"


def domain_key(url: str | None) -> str | None:
    """Registrable domain, e.g. https://mail.acme.co.uk/x -> acme.co.uk"""
    if not url:
        return None
    if not url.startswith(("http://", "https://")):
        url = "https://" + url.strip()
    try:
        host = urlparse(url).netloc.lower().split(":")[0]
    except ValueError:
        return None
    if not host:
        return None
    ext = _extract(host)
    if not ext.domain or not ext.suffix:
        return None
    return f"{ext.domain}.{ext.suffix}"


def is_aggregator(url_or_domain: str | None, aggregators: set[str]) -> bool:
    """True if the domain is a directory/social/news site, not a company site."""
    dk = domain_key(url_or_domain) or (url_or_domain or "").lower().strip()
    if not dk:
        return True
    if dk in aggregators:
        return True
    # Subdomain match, e.g. "jobs.indeed.com"
    return any(dk == agg or dk.endswith("." + agg) for agg in aggregators)


def _singularise(token: str) -> str:
    """Collapse the plural/singular variants that split real company names.

    'Technologies' and 'Technology', 'Solutions' and 'Solution', 'Systems' and
    'System' are the same brand in practice, and both forms show up across
    OSM, search titles and a company's own website.
    """
    if len(token) > 4 and token.endswith("ies"):
        return token[:-3] + "y"
    if len(token) > 3 and token.endswith("s") and not token.endswith(("ss", "us", "is")):
        return token[:-1]
    return token


def name_key(name: str | None) -> str | None:
    """Normalised company name with legal suffixes and punctuation removed."""
    if not name:
        return None
    text = _NON_ALNUM.sub(" ", name.lower())
    tokens = [t for t in _WS.sub(" ", text).strip().split(" ") if t and t not in LEGAL_SUFFIXES]
    if not tokens:
        # Name was entirely legal suffixes — fall back to the raw normalised form.
        tokens = [t for t in _WS.sub(" ", text).strip().split(" ") if t]
    key = "".join(_singularise(t) for t in tokens)
    return key or None


def phone_key(phone: str | None) -> str | None:
    """Last 10 digits of a US phone number."""
    if not phone:
        return None
    digits = _PHONE_DIGITS.sub("", phone)
    if len(digits) < 10:
        return None
    return digits[-10:]


def profile_key(
    linkedin_url: str | None,
    profile_id: str | None = None,
    full_name: str | None = None,
    job_title: str | None = None,
) -> str | None:
    """Stable dedup key for one person (SOP Module 44 / Module 63).

    Cascades most-durable first. The LinkedIn slug is preferred because it
    survives a title change, a name spelling change and a re-scrape; the
    name+title fallback does not, which is why it is last and prefixed so a
    weak key can never collide with a strong one.
    """
    if linkedin_url:
        parsed = urlparse(linkedin_url.strip().lower())
        path = parsed.path if parsed.scheme or parsed.netloc else linkedin_url.strip().lower()
        match = re.search(r"/in/([^/?#]+)", path)
        if match:
            slug = match.group(1).strip("/")
            if slug:
                return f"li:{slug}"
    if profile_id:
        pid = str(profile_id).strip().lower()
        if pid:
            return f"pid:{pid}"
    name = name_key(full_name)
    if name:
        title = name_key(job_title) or ""
        return f"nk:{name}|{title}"
    return None


def name_similarity(a: str | None, b: str | None) -> float:
    if not a or not b:
        return 0.0
    return float(fuzz.ratio(a, b))


def is_near_duplicate(a: str | None, b: str | None, threshold: float = 92.0) -> bool:
    return name_similarity(a, b) >= threshold


# Names that are page furniture, error pages or bot challenges rather than a
# company. These must never enter the database as a company record.
JUNK_NAMES = {
    "just a moment", "attention required", "access denied", "checking your browser",
    "one moment please", "please wait", "security check", "are you a robot",
    "verify you are human", "human verification", "bot verification", "captcha",
    "403 forbidden", "404 not found", "page not found", "site not found",
    "not found", "error", "server error", "under construction", "coming soon",
    "domain for sale", "home", "homepage", "welcome", "index", "untitled",
    "untitled document", "default", "new page", "test page", "blocked",
    "about", "about us", "contact", "contact us", "services", "our services",
    "it services", "managed it services", "blog", "news", "careers", "jobs",
    "privacy policy", "terms of service", "sitemap", "login", "sign in",
    "top managed service providers", "best it services", "loading",
    # WAF / CDN block pages
    "sucuri website firewall", "website firewall", "forbidden", "403 forbidden",
    "401 unauthorized", "429 too many requests", "500 internal server error",
    "502 bad gateway", "503 service unavailable", "service unavailable",
    "request blocked", "not acceptable", "temporarily unavailable",
    "maintenance", "site maintenance", "ddos guard", "cloudflare",
}


def is_junk_name(name: str | None) -> bool:
    """True when a candidate 'company name' is really page furniture."""
    if not name:
        return True
    cleaned = _NON_ALNUM.sub(" ", name.lower())
    cleaned = _WS.sub(" ", cleaned).strip()
    if not cleaned or len(cleaned) < 2:
        return True
    if cleaned in JUNK_NAMES:
        return True
    # Purely numeric or a bare HTTP status.
    if cleaned.isdigit():
        return True
    return False


def build_keys(name: str | None, website: str | None, phone: str | None) -> dict[str, str | None]:
    """All dedup keys for a candidate in one call."""
    return {
        "domain_key": domain_key(website),
        "name_key": name_key(name),
        "phone_key": phone_key(phone),
        "canonical_website": canonical_url(website),
    }
