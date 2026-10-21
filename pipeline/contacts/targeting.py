"""Turn a qualified company row into Sales Navigator search input.

SOP Module 42 maps company size to the titles that actually decide. This module
is the mechanical half of that: pick the band, then assemble the actor payload
from config/decision_makers.yaml.

Everything here is a pure function over a row and the config, so the whole
module is testable without a network or a database.
"""

from __future__ import annotations

import logging
import sqlite3
from typing import Any

from ..signals import employee_range_for

log = logging.getLogger("pipeline.contacts")

# Hard ceilings from the actor's own input schema. Exceeding them is not a
# soft failure — the actor rejects or silently truncates, so we truncate
# ourselves and say so.
MAX_COMPANIES_PER_SEARCH = 10
MAX_TITLE_KEYWORDS = 20
MAX_GEO_CODES = 20

# The actor's enums. Anything outside these is dropped loudly rather than sent
# and silently ignored.
VALID_SENIORITY = {
    "Owner/Partner", "CXO", "Vice President", "Director", "Experienced Manager",
    "Entry Level Manager", "Strategic", "Senior", "Entry Level", "In Training",
}
VALID_FUNCTIONS = {
    "Accounting", "Administrative", "Arts and Design", "Business Development",
    "Community and Social Services", "Consulting", "Education", "Engineering",
    "Entrepreneurship", "Finance", "Healthcare Services", "Human Resources",
    "Information Technology", "Legal", "Marketing", "Media and Communication",
    "Military and Protective Services", "Operations", "Product Management",
    "Program and Project Management", "Purchasing", "Quality Assurance",
    "Real Estate", "Research", "Sales", "Customer Success and Support",
}
VALID_HEADCOUNTS = {
    "Self-employed", "1-10", "11-50", "51-200", "201-500",
    "501-1000", "1001-5000", "5001-10000", "10001+",
}

FALLBACK_BAND = "unknown"


def band_for(row: sqlite3.Row | dict[str, Any], cfg: Any) -> str:
    """SOP Module 25 size band for a company, for Module 42 title selection.

    Two thirds of qualified companies have no stated headcount, so the
    `unknown` band is the normal path, not an error case.
    """
    bands = cfg.dm_bands if hasattr(cfg, "dm_bands") else {}

    stated = _field(row, "employee_range")
    band = str(stated) if stated else None

    if not band:
        # Reuse the existing SOP Module 25 mapping rather than restating it.
        estimate = _field(row, "employee_estimate")
        if estimate is not None:
            try:
                band = employee_range_for(int(estimate))
            except (TypeError, ValueError):
                band = None

    # A sub-11 company is below the qualify floor, but if one reaches here the
    # founder/owner titles of the smallest band are still the right target.
    if band == "1-10":
        band = "11-50"

    if band and band in bands:
        return band
    return FALLBACK_BAND


def profile_key(row: sqlite3.Row | dict[str, Any], cfg: Any) -> str:
    """Batching key. Companies only share a search if they share this.

    title_keywords/seniority_levels/functions are per-SEARCH, not per-company,
    so mixing bands inside one batch would apply the wrong filter to some of
    its members. Today that is exactly the band; it is a separate function so
    a future dimension (segment, say) is a one-line change here rather than a
    rewrite of the batching loop.
    """
    return band_for(row, cfg)


def build_search_input(
    *,
    company_names: list[str] | None = None,
    company_ids: list[int] | None = None,
    band: str,
    cfg: Any,
    limit: int,
) -> dict[str, Any]:
    """Assemble the actor payload for one batched search.

    Prefers `current_company_ids` when every member of the batch has a known
    LinkedIn org id — that is an exact filter, which removes the wrong-company
    attribution risk entirely. Falls back to names otherwise.

    Never sets `url`: the actor's schema says a Sales Navigator URL overrides
    every other filter, which would silently discard the geo and title targeting
    this function exists to build.
    """
    bands = cfg.dm_bands if hasattr(cfg, "dm_bands") else {}
    spec = bands.get(band) or bands.get(FALLBACK_BAND) or {}
    exclude = cfg.dm_exclusions if hasattr(cfg, "dm_exclusions") else {}

    payload: dict[str, Any] = {}

    if company_ids:
        payload["current_company_ids"] = _truncate(
            [int(c) for c in company_ids], MAX_COMPANIES_PER_SEARCH, "current_company_ids"
        )
    elif company_names:
        payload["current_company_names"] = _truncate(
            [str(n) for n in company_names], MAX_COMPANIES_PER_SEARCH, "current_company_names"
        )
    else:
        raise ValueError("build_search_input needs company_names or company_ids")

    titles = _truncate(list(spec.get("title_keywords") or []), MAX_TITLE_KEYWORDS, "title_keywords")
    if titles:
        payload["title_keywords"] = titles

    seniority = _valid(spec.get("seniority_levels"), VALID_SENIORITY, "seniority_levels")
    if seniority:
        payload["seniority_levels"] = seniority

    functions = _valid(spec.get("functions"), VALID_FUNCTIONS, "functions")
    if functions:
        payload["functions"] = functions

    # SOP Module 44 people-to-avoid, pushed actor-side so we do not pay $0.01
    # per result to retrieve someone roles.py would reject on arrival.
    #
    # Optional, because the actor has an undocumented payload-size ceiling:
    # with a widened title/seniority/function list, adding these three
    # exclusion lists made it return an empty dataset instantly (and still
    # bill the $0.50 search fee). Each half worked alone; the sum did not.
    # They are redundant with the positive seniority/function lists above and
    # roles.py re-applies the same rules on ingest, so dropping them costs at
    # most a few $0.01 rejects.
    if not cfg.get("contacts.actor_side_exclusions", True):
        exclude = {}
    ex_titles = _truncate(
        list(exclude.get("title_keywords") or []), MAX_TITLE_KEYWORDS, "title_keywords_exclude"
    )
    if ex_titles:
        payload["title_keywords_exclude"] = ex_titles
    ex_functions = _valid(exclude.get("functions"), VALID_FUNCTIONS, "functions_exclude")
    if ex_functions:
        payload["functions_exclude"] = ex_functions
    ex_seniority = _valid(
        exclude.get("seniority_levels"), VALID_SENIORITY, "seniority_levels_exclude"
    )
    if ex_seniority:
        payload["seniority_levels_exclude"] = ex_seniority

    # US-only, layer 1 of 2 (layer 2 is us_geo.classify_location on the way in).
    geo = [int(g) for g in (cfg.get("contacts.geo_codes") or [])]
    if geo:
        payload["geo_codes"] = _truncate(geo, MAX_GEO_CODES, "geo_codes")
    elif cfg.get("contacts.require_geo_codes", True):
        raise ValueError(
            "contacts.geo_codes is empty and contacts.require_geo_codes is true — "
            "refusing to run an unfiltered (worldwide) people search"
        )

    headcount = cfg.dm_headcount_filter if hasattr(cfg, "dm_headcount_filter") else {}
    if headcount.get("enabled"):
        mapped = (headcount.get("map") or {}).get(band) or []
        valid = _valid(mapped, VALID_HEADCOUNTS, "company_headcounts")
        if valid:
            payload["company_headcounts"] = valid

    payload["limit"] = max(1, int(limit))
    return payload


# -- helpers ----------------------------------------------------------------


def _field(row: sqlite3.Row | dict[str, Any], name: str) -> Any:
    if isinstance(row, dict):
        return row.get(name)
    try:
        return row[name]
    except (IndexError, KeyError):
        return None


def _truncate(values: list[Any], ceiling: int, label: str) -> list[Any]:
    if len(values) > ceiling:
        log.warning(
            "contacts: %s has %d entries, actor maximum is %d — truncating",
            label, len(values), ceiling,
        )
        return values[:ceiling]
    return values


def _valid(values: Any, allowed: set[str], label: str) -> list[str]:
    """Drop enum values the actor does not accept, loudly.

    Silently sending an unknown enum is the failure mode worth avoiding: the
    actor may reject the whole search (costing $0.50 for nothing) or ignore the
    filter (costing $0.01 per unwanted result).
    """
    out: list[str] = []
    for value in list(values or []):
        text = str(value)
        if text in allowed:
            out.append(text)
        else:
            log.warning("contacts: dropping invalid %s value %r", label, text)
    return out
