"""Validate that a LinkedIn `location` string is in the United States.

This is layer 2 of the US-only requirement. Layer 1 is the actor's own
`geo_codes` filter (see targeting.build_search_input) — but a filter we did not
write, applied by a third party, on a paid call, is not something to trust as
the only control. This module re-checks every person on the way in.

The structural fact everything here depends on: LinkedIn location strings are
comma-segmented and **country-last** — "Dallas, Texas, United States",
"Greater London, England, United Kingdom". So country detection anchors on the
LAST segment. A naive substring scan gets "Ontario, California, United States"
(a US city) and "Ontario, Canada" (not) exactly backwards, which is why that
pair has a dedicated test.

pipeline/classify.py:classify_geography is deliberately NOT reused: it is tuned
for website text (ccTLDs, VAT numbers, currency symbols) and would mis-read a
person's location line.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from ..enrich import US_STATES

US_COUNTRY_NAMES = {
    "united states", "united states of america", "usa", "u.s.", "u.s.a.", "us",
    "america", "puerto rico", "guam", "u.s. virgin islands",
}

# Last-segment country/region names that mean "not the United States". Kept as
# names rather than codes because that is what LinkedIn actually writes.
NON_US_COUNTRIES = {
    "united kingdom", "england", "scotland", "wales", "northern ireland", "ireland",
    "canada", "india", "australia", "new zealand", "germany", "france", "spain",
    "portugal", "italy", "netherlands", "belgium", "switzerland", "austria",
    "sweden", "norway", "denmark", "finland", "poland", "ukraine", "romania",
    "czechia", "czech republic", "hungary", "greece", "turkey", "russia",
    "philippines", "singapore", "malaysia", "indonesia", "thailand", "vietnam",
    "japan", "south korea", "china", "hong kong", "taiwan", "pakistan",
    "bangladesh", "sri lanka", "nepal", "united arab emirates", "saudi arabia",
    "qatar", "kuwait", "israel", "egypt", "nigeria", "kenya", "ghana",
    "south africa", "morocco", "brazil", "mexico", "argentina", "chile",
    "colombia", "peru", "costa rica", "panama", "guatemala", "dominican republic",
    "jamaica", "trinidad and tobago", "bulgaria", "serbia", "croatia", "slovakia",
    "slovenia", "lithuania", "latvia", "estonia", "armenia", "georgia country",
    "bosnia and herzegovina", "north macedonia", "albania", "moldova", "belarus",
    "kazakhstan", "uzbekistan", "azerbaijan", "cyprus", "malta", "luxembourg",
    "iceland", "uruguay", "paraguay", "bolivia", "ecuador", "venezuela",
}

# City names that exist in several states, or in another country entirely.
# Rule 6 (bare-city match) refuses to assign a state for these.
AMBIGUOUS_CITIES = {
    "springfield", "columbia", "portland", "charleston", "jackson", "newark",
    "washington", "kansas city", "columbus", "richmond", "arlington", "aurora",
    "birmingham", "manchester", "london", "paris", "rochester", "albany",
    "salem", "franklin", "georgetown", "lebanon", "clinton", "dover", "concord",
    "bristol", "cambridge", "athens", "peru", "canton", "hamilton", "windsor",
    "ontario", "victoria", "lancaster", "york", "greenville", "fairfield",
}

_METRO_NOISE = re.compile(
    r"\b(greater|metropolitan|metro|metroplex|area|region|county)\b", re.I)
_STATE_CODE = re.compile(r"\b([A-Z]{2})\b")


@dataclass
class UsGeoIndex:
    """Lookup tables built once per run from config/geography.yaml."""

    states_by_name: dict[str, str] = field(default_factory=dict)
    state_codes: set[str] = field(default_factory=set)
    cities: dict[str, list[str]] = field(default_factory=dict)
    ambiguous_cities: set[str] = field(default_factory=set)


def build_us_index(cfg: Any) -> UsGeoIndex:
    """Build the index from the existing geography.yaml — no new data file."""
    index = UsGeoIndex(state_codes=set(US_STATES), ambiguous_cities=set(AMBIGUOUS_CITIES))
    for state in (cfg.all_states if hasattr(cfg, "all_states") else []):
        code = str(state.get("code", "")).upper()
        name = str(state.get("name", "")).strip().lower()
        if not code:
            continue
        index.state_codes.add(code)
        if name:
            index.states_by_name[name] = code
        for city in state.get("cities", []) or []:
            key = str(city).strip().lower()
            if key:
                index.cities.setdefault(key, []).append(code)
    # A city listed under more than one state is ambiguous by construction.
    for city, codes in index.cities.items():
        if len(set(codes)) > 1:
            index.ambiguous_cities.add(city)
    return index


def classify_location(location: str | None, index: UsGeoIndex) -> dict[str, Any]:
    """Decide whether a person's location is in the US, and which state.

    Returns {"is_us": True|False|None, "state": "TX"|None, "reason": str}.
    `None` means undetermined — the caller's `us_policy` decides whether that
    is acceptable, and every undetermined string should be logged so these
    tables can grow from real data rather than guesswork.
    """
    if not location or not str(location).strip():
        return {"is_us": None, "state": None, "reason": "no location"}

    raw = str(location).strip()
    segments = [s.strip() for s in raw.split(",") if s.strip()]
    if not segments:
        return {"is_us": None, "state": None, "reason": "no location"}

    last = segments[-1].lower()

    # Rule 2 — non-US country in the LAST segment. Anchoring here is what makes
    # "Ontario, Canada" resolve differently from "Ontario, California, US".
    if last in NON_US_COUNTRIES:
        return {"is_us": False, "state": None, "reason": f"non-US country: {segments[-1]}"}

    # Rule 3 — explicit US country marker, state from the preceding segment.
    if last in US_COUNTRY_NAMES:
        state = _state_from_segments(segments[:-1], index)
        return {"is_us": True, "state": state, "reason": "US country marker"}

    # Rule 4/5 — a state name or code anywhere, after metro normalisation.
    state = _state_from_segments(segments, index)
    if state:
        return {"is_us": True, "state": state, "reason": "US state match"}

    # Rule 6 — an unambiguous US city.
    for segment in segments:
        for candidate in _city_candidates(segment):
            if candidate in index.ambiguous_cities:
                # Known US city name, but it does not pin a state — and several
                # of these exist abroad too, so this is not proof of US.
                continue
            codes = index.cities.get(candidate)
            if codes:
                return {"is_us": True, "state": codes[0], "reason": f"US city: {candidate}"}

    return {"is_us": None, "state": None, "reason": f"unresolved: {raw[:60]}"}


# -- helpers ----------------------------------------------------------------


def _state_from_segments(segments: list[str], index: UsGeoIndex) -> str | None:
    for segment in segments:
        cleaned = _METRO_NOISE.sub(" ", segment).strip()
        lowered = cleaned.lower().strip()
        if lowered in index.states_by_name:
            return index.states_by_name[lowered]
        # Two-letter code, uppercase only — lowercase "in"/"or"/"me" are words.
        for match in _STATE_CODE.finditer(cleaned):
            code = match.group(1)
            if code in index.state_codes:
                return code
    return None


def _city_candidates(segment: str) -> list[str]:
    """Metro strings expand to their component city names.

    "Dallas-Fort Worth Metroplex" -> ["dallas fort worth", "dallas", "fort worth"]
    """
    cleaned = _METRO_NOISE.sub(" ", segment)
    cleaned = re.sub(r"\s+", " ", cleaned).strip().lower()
    if not cleaned:
        return []
    candidates = [cleaned]
    for part in re.split(r"[-–—/]", cleaned):
        part = part.strip()
        if part and part != cleaned:
            candidates.append(part)
    return candidates
