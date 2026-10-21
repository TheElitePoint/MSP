"""Company name resolution.

A page <title> is the worst place to learn a company's name and the first place
we happened to look. It produced records called "IT Managed Services",
"Transforming Government with Tech, Speed and Scale" and "www.cognizant.com".

This module tries structured sources first and falls back to the domain, then
validates the result. The rule that does most of the work: a name whose tokens
are *all* generic industry words is a description, not a name — unless it also
matches the domain, in which case it really is the brand (itsolutions-inc.com
genuinely is called "IT Solutions").
"""

from __future__ import annotations

import json
import re
from typing import Any
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from rapidfuzz import fuzz

from .dedup import domain_key, is_junk_name, name_key

# Industry words that carry no brand information on their own.
GENERIC_TOKENS = {
    "it", "managed", "service", "services", "solution", "solutions", "support",
    "technology", "technologies", "tech", "cybersecurity", "security", "cyber",
    "cloud", "network", "networks", "networking", "computer", "computers",
    "computing", "business", "consulting", "consultant", "consultants",
    "provider", "providers", "company", "companies", "group", "systems",
    "system", "partner", "partners", "and", "the", "for", "of", "in", "your",
    "our", "we", "home", "inc", "llc", "ltd", "corp", "co", "us", "usa",
    "professional", "professionals", "expert", "experts", "specialist",
    "specialists", "management", "infrastructure", "data", "digital", "online",
    "web", "software", "hardware", "help", "helpdesk", "desk", "outsourced",
    "top", "best", "leading", "trusted", "premier", "platform", "platforms",
    "&", "-", "|", "services'", "solutions'", "firm", "agency", "center",
    "centre", "team", "works", "labs", "hub", "point", "source", "care",
}

# Place names. Discovery is city-driven, so search results are full of titles
# like "Los Angeles IT Providers" and "IT Services Delaware" — a location plus
# generic words is still a description, not a company name.
PLACE_TOKENS = {
    # states / territories
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho",
    "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana", "maine",
    "maryland", "massachusetts", "michigan", "minnesota", "mississippi",
    "missouri", "montana", "nebraska", "nevada", "hampshire", "jersey",
    "mexico", "york", "carolina", "dakota", "ohio", "oklahoma", "oregon",
    "pennsylvania", "rhode", "island", "tennessee", "texas", "utah", "vermont",
    "virginia", "washington", "wisconsin", "wyoming", "columbia", "america",
    "american", "national", "nationwide", "usa", "states", "united",
    # frequent metros
    "atlanta", "austin", "baltimore", "boston", "charlotte", "chicago",
    "cleveland", "columbus", "dallas", "denver", "detroit", "houston",
    "indianapolis", "jacksonville", "kansas", "vegas", "angeles", "los",
    "louisville", "memphis", "miami", "milwaukee", "minneapolis", "nashville",
    "orleans", "newark", "oakland", "orlando", "philadelphia", "phoenix",
    "pittsburgh", "portland", "raleigh", "richmond", "sacramento", "antonio",
    "diego", "francisco", "jose", "seattle", "louis", "tampa", "tucson",
    "tulsa", "wilmington", "wichita", "omaha", "albuquerque", "fresno",
    "mesa", "sacramento", "atlanta", "greater", "metro", "area", "valley",
    "north", "south", "east", "west", "central", "greater",
}

# Tokens that never carry brand meaning, for the "all generic" test.
_NON_BRAND = GENERIC_TOKENS | PLACE_TOKENS

# A name starting with these is a sentence, not a company.
SENTENCE_STARTS = (
    "helping", "transforming", "delivering", "providing", "making", "building",
    "empowering", "enabling", "driving", "unlock", "discover", "get", "find",
    "welcome", "how", "why", "what", "when", "your", "the leader", "a leading",
    "we ", "learn", "explore", "meet ", "introducing",
)

_WS = re.compile(r"\s+")
# Deliberately NOT case-insensitive. With IGNORECASE the capture group's
# leading [A-Z] matched lowercase too, so "© 2025 All rights reserved"
# captured "All rights reserved" as the company name.
_COPYRIGHT = re.compile(
    r"(?:©|\(c\)|&copy;|[Cc]opyright)\s*(?:\d{4}\s*(?:[-–—]\s*\d{4})?\s*)?"
    r"(?:[Bb]y\s+)?([A-Z][A-Za-z0-9&.,'’\-\s]{2,60}?)"
    r"(?:\s*(?:\.|\||·|,\s*(?:[Ii]nc|LLC|[Ll]td)|[Aa]ll [Rr]ights|$))"
)

# Footer boilerplate that must never survive as a company name.
_COPYRIGHT_NOISE = (
    "all rights reserved", "rights reserved", "privacy policy", "terms of use",
    "terms and conditions", "sitemap", "cookie policy", "designed by",
    "powered by", "website by", "web design by",
)

# Image-derived artefacts: "Cyber+Lynx+-white.png", "logo-dark", "acme_logo@2x",
# "cloud cover logo no bg phone". Asset descriptors can appear anywhere in the
# string, not only at the end, so they are matched as words throughout.
_FILENAME_ARTEFACT = re.compile(
    r"\.(?:png|jpe?g|svg|gif|webp)\b|\+|@\dx\b"
    r"|\b(?:no\s?bg|nobg|transparent|retina|thumb|thumbnail|favicon|sprite|"
    r"mobile|desktop|phone|tablet|small|medium|large|wide|square|vert|horiz)\b"
    r"|\b(?:white|black|dark|light|colou?r|rev|alt|final|v\d)\s*$",
    re.IGNORECASE,
)


def is_descriptive(name: str | None, website: str | None = None) -> bool:
    """True when the string describes a service rather than naming a company."""
    if not name:
        return True
    cleaned = _WS.sub(" ", re.sub(r"[^\w\s&-]", " ", name.lower())).strip()
    if not cleaned:
        return True

    tokens = [t for t in cleaned.split() if t]
    if not tokens:
        return True

    # A URL is not a name.
    if cleaned.startswith(("www ", "http")) or (website and cleaned.replace(" ", "") in
                                                (domain_key(website) or "").replace(".", "")):
        if cleaned.startswith(("www ", "http")):
            return True

    # Sentence-shaped.
    low = name.lower().strip()
    if low.startswith(SENTENCE_STARTS):
        return True
    if len(tokens) > 7:
        return True

    # Every token is a generic industry or place word -> it is a description...
    if all(t in _NON_BRAND for t in tokens):
        # ...unless the domain says this really is the brand. Compared loosely,
        # because name normalisation singularises ("services" -> "service")
        # while the domain does not (itservicesdelaware.com).
        if website and _matches_domain(name, website):
            return False
        return True

    return False


def _matches_domain(name: str, website: str) -> bool:
    """True when the name is essentially the same string as the domain stem."""
    nk = name_key(name) or ""
    dk = (domain_key(website) or "").split(".")[0].replace("-", "")
    if not nk or not dk:
        return False
    if nk in dk or dk in nk:
        return True
    return fuzz.ratio(nk, dk) >= 85


def _from_jsonld(soup: BeautifulSoup) -> list[str]:
    """schema.org Organization / LocalBusiness names — the most reliable source."""
    found: list[str] = []
    wanted = {
        "organization", "localbusiness", "corporation", "professionalservice",
        "itservice", "onlinebusiness",
    }

    def walk(node: Any) -> None:
        if isinstance(node, list):
            for item in node:
                walk(item)
            return
        if not isinstance(node, dict):
            return
        node_type = node.get("@type")
        types = (
            [node_type] if isinstance(node_type, str)
            else node_type if isinstance(node_type, list) else []
        )
        if any(str(t).lower() in wanted for t in types):
            name = node.get("name") or node.get("legalName")
            if isinstance(name, str) and name.strip():
                found.append(name.strip())
        for value in node.values():
            if isinstance(value, (dict, list)):
                walk(value)

    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = tag.string or tag.get_text() or ""
        if not raw.strip():
            continue
        try:
            walk(json.loads(raw))
        except (json.JSONDecodeError, ValueError):
            continue
    return found


def _from_copyright(text: str) -> list[str]:
    """Footer copyright lines usually carry the legal entity name."""
    found: list[str] = []
    for match in _COPYRIGHT.finditer(text[-6000:] or ""):
        candidate = _WS.sub(" ", match.group(1)).strip(" .,|·-")
        if not (2 < len(candidate) < 60):
            continue
        if any(noise in candidate.lower() for noise in _COPYRIGHT_NOISE):
            continue
        found.append(candidate)
    return found[:3]


def _from_logo(soup: BeautifulSoup) -> list[str]:
    """Logo alt text is often exactly the company name."""
    found: list[str] = []
    for img in soup.find_all("img", alt=True)[:40]:
        alt = (img.get("alt") or "").strip()
        if not alt or len(alt) > 60:
            continue
        haystack = f"{img.get('class', '')} {img.get('id', '')} {img.get('src', '')}".lower()
        if "logo" not in haystack and "logo" not in alt.lower():
            continue
        # Partner/client logo walls carry OTHER companies' names — that is how
        # optiv.com ended up recorded as "Crowdstrike".
        if any(w in haystack for w in ("partner", "client", "vendor", "badge",
                                       "award", "certif", "sponsor", "affiliat")):
            continue
        cleaned = re.sub(r"\b(logo|home ?page|home|icon|header)\b", "", alt, flags=re.I)
        cleaned = _FILENAME_ARTEFACT.sub(" ", cleaned.replace("_", " "))
        cleaned = _WS.sub(" ", cleaned).strip(" -|·")
        if len(cleaned) > 2:
            found.append(cleaned)
    return found[:3]


def _from_domain(website: str | None) -> str | None:
    """Last resort: turn acmetech.com into 'Acmetech'."""
    dk = domain_key(website)
    if not dk:
        return None
    stem = dk.split(".")[0]
    if not stem or len(stem) < 2:
        return None
    stem = stem.replace("-", " ").replace("_", " ")
    return " ".join(word.capitalize() for word in stem.split() if word)[:60] or None


def resolve(
    soup: BeautifulSoup,
    text: str,
    website: str | None,
    fallback: str | None = None,
) -> tuple[str | None, str]:
    """Best available company name plus the source it came from.

    Returns (name, source). Source is one of: jsonld, og, application-name,
    copyright, logo, title, domain, fallback.
    """
    candidates: list[tuple[str, str]] = []

    for name in _from_jsonld(soup):
        candidates.append((name, "jsonld"))

    for attrs, label in (
        ({"property": "og:site_name"}, "og"),
        ({"name": "application-name"}, "application-name"),
    ):
        meta = soup.find("meta", attrs=attrs)
        if meta and meta.get("content"):
            candidates.append((meta["content"].strip(), label))

    for name in _from_copyright(text):
        candidates.append((name, "copyright"))

    for name in _from_logo(soup):
        candidates.append((name, "logo"))

    if soup.title and soup.title.string:
        title = soup.title.string.strip()
        parts = [title]
        for sep in ("|", " - ", " – ", " — ", ":"):
            if sep in title:
                parts = [p.strip() for p in title.split(sep) if p.strip()]
                break
        # The brand is usually the shortest non-generic segment.
        for part in sorted(parts, key=len):
            candidates.append((part, "title"))

    # Validate every candidate, then prefer one that corroborates the domain.
    # Source priority alone picked a partner logo ("Crowdstrike" on optiv.com)
    # and an asset filename ("Cyber+Lynx+-white") over the real brand.
    valid: list[tuple[str, str]] = []
    for raw, source in candidates:
        name = _clean(raw)
        if not name or is_junk_name(name) or is_descriptive(name, website):
            continue
        if _FILENAME_ARTEFACT.search(name):
            continue
        valid.append((name, source))

    if valid:
        for name, source in valid:
            if website and _matches_domain(name, website):
                return name, source
        return valid[0]

    # Nothing usable: derive from the domain rather than keep a description.
    derived = _from_domain(website)
    if derived and not is_junk_name(derived):
        return derived, "domain"

    if fallback and not is_junk_name(fallback) and not is_descriptive(fallback, website):
        return fallback, "fallback"
    return None, "unresolved"


def _clean(name: str) -> str | None:
    """Strip taglines, trailing punctuation and marketing prefixes."""
    if not name:
        return None
    name = _WS.sub(" ", name).strip(" -|·—–:,")

    # Drop a trailing tagline after a comma or dash.
    for sep in (",", " - ", " – ", " — ", ":"):
        if sep in name:
            head, _, tail = name.partition(sep)
            head, tail_low = head.strip(), tail.strip().lower()
            if len(head) >= 3 and tail_low:
                tagline_starts = ("a ", "an ", "the ", "your ", "we ", "delivering ",
                                  "providing ", "make ", "making ", "home")
                tagline_words = ("provider", "services", "solutions", "leading",
                                 "trusted", "nationwide", "managed it", "msp",
                                 "specialist", "experts", "consulting", "serving",
                                 "support", "cybersecurity", "company")
                if tail_low.startswith(tagline_starts) or any(w in tail_low for w in tagline_words):
                    name = head
                    break

    # Remove a leading "Top 10 " / "Best " style prefix.
    name = re.sub(r"^(?:top|best)\s+\d*\s*", "", name, flags=re.I).strip()
    return name[:120] or None
