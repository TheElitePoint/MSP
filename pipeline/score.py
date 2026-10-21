"""Account scoring (SOP Module 30).

    Ideal company fit      40   business model, size, geography, customer type
    Operational need       25   incidents, dependencies, change, ownership
    Timing signals         20   hiring, acquisition, expansion, transformation
    Decision-maker access  15   relevant leaders, contactability

    75-100 Tier A | 55-74 Tier B | 35-54 Tier C | <35 Exclude

Module 30 warns against awarding timing points merely because a company posted
recently, so timing credit here requires an operational-role hire or a
confirmed business-change event.
"""

from __future__ import annotations

import sqlite3
from typing import Any

from . import intent_signals as intent_lex
from .store import Store


def _cap(value: float, ceiling: float) -> float:
    return min(value, ceiling)


def score_company(row: sqlite3.Row, store: Store, cfg: Any) -> dict[str, Any]:
    weights = cfg.get("scoring.weights", {}) or {}
    w_fit = float(weights.get("ideal_company_fit", 40))
    w_need = float(weights.get("operational_need", 25))
    w_timing = float(weights.get("timing_signals", 20))
    w_access = float(weights.get("decision_maker_access", 15))

    all_signals = store.get_signals(row["id"])
    by_cat: dict[str, set[str]] = {}
    for sig in all_signals:
        by_cat.setdefault(sig["category"], set()).add(sig["signal"])

    recurring = by_cat.get("recurring", set())
    operational = by_cat.get("operational", set())
    complexity = by_cat.get("complexity", set())
    vertical = by_cat.get("vertical", set())
    access = by_cat.get("access", set())

    # -- 1. Ideal company fit (40) -----------------------------------------
    fit = 0.0
    # Business model — the single most important factor.
    fit += _cap(len(recurring) * 5.0, 18.0)
    if {"managed_services", "co_managed", "outsourced_it"} & recurring:
        fit += 4.0  # core operator language, not just a cloud/security niche

    # Size band (Module 25: 51-200 is the strongest initial range).
    band_points = {"11-50": 5.0, "51-200": 8.0, "201-500": 7.0, "501-2000": 4.0, "2000+": 1.5}
    est_band = row["employee_range"]
    if est_band in band_points:
        fit += band_points[est_band]
    else:
        # No stated headcount (the majority case). This used to be a flat 3.0
        # regardless of evidence — every unknown-headcount company got equal
        # credit whether the record was rich or nearly empty. Calibrating a
        # "maturity signals predict headcount band" formula against this
        # pipeline's own data (Module 25's licensed substitute) showed the
        # correlation is too weak to trust for a specific band — a 51-200
        # account had the single richest signal set in the sample, while a
        # confirmed 2000+ account had almost none. So this does NOT guess a
        # band; it only differentiates evidence density among unknowns,
        # which is a materially different, defensible claim.
        maturity = sum(1 for group in (recurring, operational, complexity, access) if group)
        fit += min(maturity, 4) * 1.0

    # Geography.
    if row["headquarters_state"]:
        fit += 3.0

    # Customer type.
    if {"healthcare", "financial"} & vertical:
        fit += 4.0
    elif vertical:
        fit += 2.0

    # Operational scale.
    fit += _cap(len(complexity) * 1.5, 4.0)
    fit = _cap(fit, w_fit)

    # -- 2. Operational need (25) ------------------------------------------
    need = 0.0
    need += _cap(len(operational) * 2.5, 15.0)
    # The high-intensity markers carry extra weight.
    for marker, points in (("noc", 3.0), ("incident_response", 3.0),
                           ("sla", 2.0), ("change_management", 2.0),
                           ("continuity", 2.0), ("always_on_support", 2.0)):
        if marker in operational:
            need += points
    # Ongoing operational conditions (SOP Module 25 "maturity signals"), moved
    # here from timing: these describe a standing state of the business, not
    # a dated event, so they cannot decay and should not compete for the
    # 20-point timing budget with genuine acquisitions/hires/leadership change.
    for marker, points in intent_lex.CONDITION_WEIGHTS.items():
        if marker in operational or marker in complexity:
            need += points
    # Tool fragmentation proxy: breadth of platforms mentioned.
    if "platform_breadth" in complexity:
        need += 2.0
    if "multi_client" in complexity:
        need += 2.0
    need = _cap(need, w_need)

    # -- 3. Timing / intent (20) --------------------------------------------
    # Composite intent model over dated events (see intent_signals.py). A
    # three-year-old acquisition is not a reason to call today, and an event
    # with no discoverable date is treated as lower-confidence, not as fresh.
    intent_result = intent_lex.score_intent(store.get_intent_events(row["id"]), max_points=w_timing)
    timing_score = intent_result["score"]

    # -- 4. Decision-maker access (15) -------------------------------------
    # Two kinds of evidence, weighted differently on purpose.
    #
    # The website proxies below only say "this company probably has reachable
    # leaders". The two signals underneath say "we have named, US-verified
    # decision-makers on file", which is what Module 30 actually asks this
    # category to measure. The proxy weights were cut (6/6/2/2 -> 4/4/2/2, a
    # 16-point notional max down to 12) so confirmed evidence has room to move
    # the total: previously a leadership page plus exec titles alone already
    # exceeded the 15-point cap, so real mapped people bought literally nothing.
    #
    # Measured against the live database, this costs an average of 2.5 access
    # points across qualified companies. The differential lands where it should
    # — a company with weak website signals but real named buyers now scores
    # well above one with neither, while a company already showing every
    # website proxy saturates the SOP's 15-point ceiling, which is correct: it
    # genuinely does have good buyer access either way.
    access_score = 0.0
    if "leadership_page" in access:
        access_score += 4.0
    if "exec_titles" in access:
        access_score += 4.0
    if "contact_route" in access:
        access_score += 2.0
    if row["linkedin_url"]:
        access_score += 2.0

    # Written by stage_contacts, which runs AFTER this stage — so these are
    # read on the next run, never the one that produced them. The effect is
    # monotone (contacts only ever add points), so a company can never lose the
    # tier that qualified it for contact mapping in the first place.
    if "named_decision_maker" in access:
        access_score += 4.0
    if "economic_buyer_identified" in access:
        access_score += 3.0
    access_score = _cap(access_score, w_access)

    total = round(fit + need + timing_score + access_score, 1)

    tiers = cfg.get("scoring.tiers", {}) or {}
    if total >= float(tiers.get("A", 75)):
        tier = "A"
    elif total >= float(tiers.get("B", 55)):
        tier = "B"
    elif total >= float(tiers.get("C", 35)):
        tier = "C"
    else:
        tier = "Exclude"

    return {
        "intent_score": intent_result["score"],
        "intent_summary": intent_lex.summarise(intent_result["contributions"])[:900],
        "score_fit": round(fit, 1),
        "score_need": round(need, 1),
        "score_timing": round(timing_score, 1),
        "score_access": round(access_score, 1),
        "score_total": total,
        "tier": tier,
    }


def completeness(row: sqlite3.Row) -> float:
    """Percentage of the SOP Module 63 required tracker fields that are populated."""
    required = [
        "company_name", "website", "headquarters_state", "country",
        "segment", "services", "company_description",
        "fit_reason", "operational_signals", "status",
        "score_total", "tier", "discovery_source",
    ]
    filled = 0
    for field in required:
        value = row[field]
        if value not in (None, "", 0):
            filled += 1
    return round(100.0 * filled / len(required), 1)
