"""Qualification and exclusion (SOP Modules 25, 26, 28).

Minimum acceptance rule (Module 26): a company must pass business-model fit,
operational relevance and buyer access. Anything relevant but unproven becomes
`review` rather than being forced into the approved list.

Every exclusion is written to the permanent exclusion ledger so the same
unsuitable company is never researched again (Module 28 exclusion discipline).
"""

from __future__ import annotations

import logging
import sqlite3
from typing import Any

from . import dedup, signals
from .store import Store

log = logging.getLogger("pipeline.qualify")

STATUS_APPROVED = "approved"
STATUS_REVIEW = "review"
STATUS_EXCLUDED = "excluded"


def qualify_company(
    row: sqlite3.Row,
    store: Store,
    cfg: Any,
    run_id: str,
) -> tuple[str, str, list[str]]:
    """Return (status, reason, exclusions_checked)."""
    company_id = row["id"]
    checked: list[str] = []

    name = row["company_name"] or ""
    name_k = row["name_key"] or dedup.name_key(name)
    domain_k = row["domain_key"]

    # Already ruled out in a previous run — never re-research (Module 28).
    prior = store.is_excluded(domain_k, name_k)
    if prior:
        return STATUS_EXCLUDED, f"Previously excluded: {prior}", ["prior_exclusion"]

    sig_rows = store.get_signals(company_id)
    by_cat: dict[str, set[str]] = {}
    for sig in sig_rows:
        by_cat.setdefault(sig["category"], set()).add(sig["signal"])

    recurring = by_cat.get("recurring", set())
    operational = by_cat.get("operational", set())
    access = by_cat.get("access", set())
    exclusion_hits = by_cat.get("exclusion", set())

    # -- Hard exclusions ---------------------------------------------------

    if cfg.get("qualify.require_working_website", True):
        checked.append("website_reachable")
        if row["website_reachable"] == 0:
            # Deliberately NOT a permanent exclusion. A site can be temporarily
            # down, or simply behind bot protection that blocks our crawler —
            # several genuine MSPs return 403 to any non-browser client. The
            # exclusion ledger is for business-model mismatches (Module 28),
            # not for companies we merely failed to reach.
            return (
                STATUS_REVIEW,
                "Website could not be verified automatically "
                f"({(row['review_notes'] or 'no response')[:120]}) — check manually",
                checked,
            )

    if exclusion_hits:
        checked.append("business_model_exclusions")
        reason = "Weak-fit business model: " + ", ".join(sorted(exclusion_hits))
        store.add_exclusion(domain_k, name, name_k, reason, run_id)
        return STATUS_EXCLUDED, reason, checked

    # Headcount band (Module 25): 1-10 usually excluded.
    checked.append("headcount_band")
    estimate = row["employee_estimate"]
    hmin = int(cfg.get("qualify.headcount_min", 11))
    hmax = int(cfg.get("qualify.headcount_max", 5000))
    if estimate is not None:
        if estimate < hmin:
            reason = f"Headcount ~{estimate} below minimum {hmin} (insufficient complexity)"
            store.add_exclusion(domain_k, name, name_k, reason, run_id)
            return STATUS_EXCLUDED, reason, checked
        if estimate > hmax:
            return STATUS_REVIEW, f"Headcount ~{estimate} above {hmax}; long procurement likely", checked
    elif not cfg.get("qualify.allow_unknown_headcount", True):
        return STATUS_REVIEW, "Headcount not found on public sources", checked

    checked.append("us_location")
    if cfg.get("qualify.require_us_location", True) and not row["headquarters_state"]:
        return STATUS_REVIEW, "US state not confirmed", checked

    # -- Minimum acceptance rule (Module 26) --------------------------------

    checked.append("recurring_service_model")
    min_recurring = int(cfg.get("qualify.min_recurring_signals", 1))
    if len(recurring) < min_recurring:
        # No evidence of an operating model. Not proven unsuitable, but not
        # approvable either — hold for review rather than exclude outright.
        return (
            STATUS_REVIEW,
            "No recurring managed-service evidence found on the website",
            checked,
        )

    checked.append("operational_responsibility")
    min_operational = int(cfg.get("qualify.min_operational_signals", 1))
    if len(operational) < min_operational:
        return (
            STATUS_REVIEW,
            "Recurring services present but no 24/7, SLA, NOC or incident evidence",
            checked,
        )

    checked.append("buyer_access")
    if not access:
        return (
            STATUS_REVIEW,
            "No visible leadership or contact route identified",
            checked,
        )

    reason = (
        f"Passes business-model fit ({len(recurring)} recurring signals), "
        f"operational relevance ({len(operational)} signals), and buyer access."
    )
    return STATUS_APPROVED, reason, checked


def build_fit_reason(row: sqlite3.Row, store: Store) -> str:
    """Rationale in the SOP Module 63 format.

    [Business model] + [operational complexity] + [problem relevance]
    + [timing or priority] + [buyer access]
    """
    company_id = row["id"]
    by_cat: dict[str, list[str]] = {}
    for sig in store.get_signals(company_id):
        by_cat.setdefault(sig["category"], []).append(sig["signal"])

    parts: list[str] = []

    recurring = by_cat.get("recurring", [])
    if recurring:
        model = _phrase(recurring, {
            "managed_services": "recurring managed IT services",
            "co_managed": "co-managed IT",
            "outsourced_it": "outsourced IT / vCIO services",
            "managed_cloud": "managed cloud operations",
            "managed_security": "managed security services",
            "managed_infrastructure": "managed infrastructure",
            "recurring_commercial": "contracted recurring agreements",
        })
        size = row["employee_range"] or "size not confirmed"
        parts.append(f"Operates {model} ({size}).")
    else:
        parts.append("Recurring service model not yet confirmed.")

    complexity = by_cat.get("complexity", [])
    if complexity:
        parts.append(
            "Operating complexity evidenced by "
            + _phrase(complexity, {
                "multi_client": "multiple client environments",
                "hybrid_multicloud": "hybrid/multi-cloud infrastructure",
                "multi_location": "multiple locations",
                "platform_breadth": "a broad platform estate",
                "integration": "systems integration work",
            })
            + "."
        )

    operational = by_cat.get("operational", [])
    if operational:
        parts.append(
            "Carries operational responsibility: "
            + _phrase(operational, {
                "always_on_support": "24/7 support",
                "sla": "SLA commitments",
                "noc": "a NOC",
                "service_desk": "a service desk",
                "incident_response": "incident response",
                "monitoring": "proactive monitoring",
                "change_management": "change control",
                "continuity": "continuity/DR services",
                "service_management": "formal service management",
                # Ongoing maturity conditions (Change 1) — moved out of
                # timing since they are standing states, not dated events.
                "compliance_pursuit": "an active compliance/certification programme",
                "status_page": "a public status page",
                "incident_history": "published incident history",
                "client_scale_claim": "a large managed client base",
                "vendor_partner": "vendor partner programme participation",
                "association_member": "industry association membership",
                "operational_content": "public writing on operational pain",
            })
            + " — the conditions where connected incident, dependency and "
            "ownership context is valuable."
        )

    vertical = by_cat.get("vertical", [])
    if vertical:
        parts.append(
            "Serves "
            + _phrase(vertical, {
                "healthcare": "healthcare",
                "financial": "financial services",
                "regulated_other": "compliance-driven",
                "critical_infrastructure": "critical-operations",
            })
            + " customers, raising the cost of downtime."
        )

    timing = row["timing_signal"]
    parts.append(f"Timing: {timing}." if timing else "No current timing signal identified.")

    access = by_cat.get("access", [])
    parts.append(
        "Relevant leadership is publicly visible."
        if access
        else "Decision-maker access still needs verification."
    )

    return " ".join(parts)[:1200]


def _phrase(items: list[str], labels: dict[str, str]) -> str:
    named = [labels[i] for i in items if i in labels]
    if not named:
        return "related signals"
    if len(named) == 1:
        return named[0]
    return ", ".join(named[:-1]) + " and " + named[-1]


def next_action_for(status: str, tier: str | None) -> str:
    if status == STATUS_APPROVED and tier == "A":
        return "Map 2-4 decision-makers and hand off for priority outreach"
    if status == STATUS_APPROVED and tier == "B":
        return "Map 2-3 decision-makers after Tier A is complete"
    if status == STATUS_APPROVED:
        return "Hold in nurture; revisit if a timing signal appears"
    if status == STATUS_REVIEW:
        return "Manual review: verify the missing evidence noted in status_reason"
    return "No action — excluded"
