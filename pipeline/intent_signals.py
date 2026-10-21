"""Intent model — why this account, why now.

SOP Module 30 gives timing 20 points and warns against awarding them "merely
because the company posted recently". Module 55 goes further: "record the
date and source because timing information changes." So this module makes a
hard distinction the earlier version did not:

    EVENTS      discrete, dated things that happened — scored here as Timing,
                stored in the `intent_events` table, decayed by event_date.
      acquisition, expansion, new_service, transformation, leadership_change,
      funding, growth, hiring_operational, hiring_context, hiring_volume,
      recent_award

    CONDITIONS  ongoing states with no natural "when it happened" — these are
                *maturity*, not *timing* (SOP Module 25), and are scored as
                Operational need in score.py, not here. See CONDITION_WEIGHTS.
      tool_fragmentation, compliance_pursuit, status_page, incident_history,
      client_scale_claim, vendor_partner, association_member,
      operational_content

Only EVENTS draw timing points, and only with a real event_date do they get
full credit — an event with no discoverable date still counts (it clearly
happened *at some point*), but at a permanent confidence penalty, because we
cannot actually tell if it is recent. This replaces the old behaviour where
every signal — dated or not — decayed from the moment it was *crawled*,
making the "recency" decay a no-op for the vast majority of signals.

Intent score = sum(weight x recency_multiplier), capped at the SOP's 20 points.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

# --------------------------------------------------------------------------
# Event weights — dated, discrete triggers. Timing score only.
# --------------------------------------------------------------------------

INTENT_WEIGHTS: dict[str, float] = {
    "hiring_operational": 9.0,
    "hiring_volume": 4.0,
    "recent_award": 3.0,
    "acquisition": 8.0,
    "transformation": 6.0,
    "expansion": 5.0,
    "new_service": 4.0,
    "leadership_change": 4.0,
    "funding": 4.0,
    "growth": 3.0,
    # legacy alias kept so older rows still score
    "hiring_context": 3.0,
    # legacy name from a fixed naming bug (intent.py used to write this
    # instead of "hiring_operational") — same signal, same weight.
    "operational_hiring": 9.0,
}

TIER = {
    "hiring_operational": 1, "hiring_volume": 1, "hiring_context": 1,
    "operational_hiring": 1, "recent_award": 2,
    "acquisition": 3, "transformation": 3, "expansion": 3, "new_service": 3,
    "leadership_change": 3, "funding": 3, "growth": 3,
}

# --------------------------------------------------------------------------
# Condition weights — ongoing states, no event date. Operational-need score
# only (see score.py). Not decayed: a company either currently shows the
# condition or it does not.
# --------------------------------------------------------------------------

CONDITION_WEIGHTS: dict[str, float] = {
    "tool_fragmentation": 3.0,   # breadth of tooling — also feeds Fit via `complexity`
    "compliance_pursuit": 3.0,
    "incident_history": 2.0,
    "status_page": 1.5,
    "client_scale_claim": 2.0,
    "vendor_partner": 1.5,
    "association_member": 1.0,
    "operational_content": 1.5,
}

# --------------------------------------------------------------------------
# TIER 1 detectors
# --------------------------------------------------------------------------

# Operational tooling, grouped by category. Breadth across categories is the
# fragmentation signal — the exact problem the platform addresses.
TOOL_CATEGORIES: dict[str, list[str]] = {
    "rmm": ["connectwise automate", "kaseya vsa", "datto rmm", "ninjaone", "ninja rmm",
            "n-able", "n-central", "syncro", "atera", "level.io", "action1"],
    "psa": ["connectwise manage", "connectwise psa", "autotask", "halopsa", "syncro psa",
            "kaseya bms", "accelo"],
    "monitoring": ["datadog", "new relic", "solarwinds", "prtg", "nagios", "zabbix",
                   "logicmonitor", "auvik", "domotz", "grafana", "prometheus",
                   "dynatrace", "appdynamics", "sensu", "checkmk"],
    "ticketing": ["servicenow", "jira service", "zendesk", "freshservice", "freshdesk",
                  "ivanti", "cherwell", "manageengine", "solarwinds service desk", "halo itsm"],
    "documentation": ["it glue", "hudu", "confluence", "sharepoint", "notion",
                      "passportal", "keeper", "itboost"],
    "backup": ["veeam", "acronis", "datto backup", "rubrik", "cohesity", "commvault",
               "barracuda", "axcient", "carbonite", "unitrends"],
    "security": ["sentinelone", "crowdstrike", "sophos", "bitdefender", "huntress",
                 "arctic wolf", "blackpoint", "todyl", "perch", "wazuh", "splunk",
                 "microsoft defender", "carbon black", "cylance"],
    "cloud": ["azure", "aws", "amazon web services", "google cloud", "gcp",
              "vmware", "hyper-v", "nutanix", "citrix", "proxmox"],
    "identity": ["okta", "duo security", "jumpcloud", "entra id", "active directory",
                 "ping identity", "onelogin", "cyberark"],
    "network": ["meraki", "fortinet", "palo alto", "sonicwall", "ubiquiti",
                "aruba", "juniper", "cato networks", "velocloud"],
}

COMPLIANCE_PURSUIT = [
    "soc 2 type ii", "soc 2 type 2", "soc2 type ii", "pursuing soc 2",
    "soc 2 certified", "soc 2 compliant", "hitrust certified", "hitrust csf",
    "cmmc level 2", "cmmc level 1", "cmmc compliance", "cmmc readiness",
    "iso 27001 certified", "iso 27001", "nist 800-171", "nist csf",
    "fedramp", "pci dss compliant", "hipaa compliant", "glba compliance",
    "achieving compliance", "compliance roadmap", "annual audit",
]

INCIDENT_HISTORY = [
    "status page", "system status", "service status", "incident history",
    "postmortem", "post-mortem", "root cause analysis", "rca report",
    "past incidents", "uptime history", "scheduled maintenance window",
    "degraded performance", "service disruption notice",
]

CLIENT_SCALE = re.compile(
    r"\b(\d{2,6})\s*\+?\s*(?:clients|customers|businesses|organizations|organisations|"
    r"companies|endpoints|devices|users|seats|workstations|servers)\b(?!\s*(?:can|may|might))"
)

OPERATIONAL_CONTENT = [
    "alert fatigue", "mean time to resolution", "mttr", "mean time to detect",
    "tool sprawl", "single pane of glass", "tribal knowledge", "documentation debt",
    "reduce noise", "noisy alerts", "firefighting", "reactive to proactive",
    "root cause", "blast radius", "dependency mapping", "service mapping",
    "escalation fatigue", "ticket volume", "on-call burnout", "runbook",
]

VENDOR_PARTNER = [
    "connectwise partner", "datto partner", "kaseya partner", "ninjaone partner",
    "microsoft partner", "microsoft solutions partner", "gold partner",
    "aws partner", "aws select", "google cloud partner", "vmware partner",
    "sentinelone partner", "sophos partner", "fortinet partner", "cisco partner",
    "solution provider", "csp partner", "certified partner", "premier partner",
]

ASSOCIATION_MEMBER = [
    "comptia member", "comptia", "ascii group", "the ascii group", "cmit",
    "technology assurance group", "evolve mspaa", "msp alliance", "mspalliance",
    "chamber of commerce member", "technology council", "it nation",
]

AWARD_MARKERS = [
    "msp 501", "msp501", "crn mssp", "crn msp", "pioneer 250", "fast growth 150",
    "inc. 5000", "inc 5000", "best places to work", "channel futures",
    "top workplaces", "fastest growing",
]

# Roles that mean the company is scaling operational responsibility.
# (Kept separate from signals.HIRING_ROLES so intent can weight them.)
# Leadership roles that own the operational problem. Highest-signal.
HIGH_VALUE_ROLES = [
    "service delivery manager", "service delivery director", "director of service delivery",
    "managed services director", "director of managed services", "vp of managed services",
    "noc manager", "noc supervisor", "soc manager", "incident manager",
    "problem manager", "change manager", "service desk manager",
    "it service manager", "itsm manager", "configuration manager",
    "infrastructure manager", "cloud operations manager", "director of it operations",
    "vp of operations", "technical account manager", "enterprise architect",
]

# Front-line operational roles. Measured against real MSP careers pages, the
# leadership list above almost never matches — actual openings read "Service
# Desk Specialist", "Escalations Engineer", "Support Desk Specialist". These
# still evidence operational responsibility and scaling, so they count, but
# they are scored lower than a leadership hire.
OPERATIONAL_ROLES = [
    "service desk", "support desk", "help desk", "helpdesk", "noc technician",
    "noc engineer", "noc analyst", "soc analyst", "escalation engineer",
    "escalations engineer", "escalation specialist", "systems administrator",
    "system administrator", "sysadmin", "network administrator",
    "network engineer", "systems engineer", "infrastructure engineer",
    "cloud engineer", "devops engineer", "site reliability",
    "field engineer", "field technician", "desktop support", "technical support",
    "support engineer", "support specialist", "support technician",
    "implementation engineer", "implementation specialist", "onboarding specialist",
    "project engineer", "automation engineer", "automation associate",
    "monitoring analyst", "dispatcher", "service coordinator", "triage",
    "tier 2", "tier 3", "level 2", "level 3", "msp technician",
]


def detect_tool_fragmentation(text: str) -> tuple[int, dict[str, list[str]]]:
    """Count distinct tool *categories* named on the site or in job posts.

    Breadth across categories matters more than raw tool count: a company
    running an RMM, a PSA, a monitoring stack, a ticketing system and a
    documentation tool is exactly the fragmented environment the platform
    is designed for.
    """
    low = text.lower()
    found: dict[str, list[str]] = {}
    for category, tools in TOOL_CATEGORIES.items():
        hits = [t for t in tools if t in low]
        if hits:
            found[category] = hits
    return len(found), found


def detect_compliance_pursuit(text: str) -> list[str]:
    low = text.lower()
    return [p for p in COMPLIANCE_PURSUIT if p in low]


def detect_incident_history(text: str) -> list[str]:
    low = text.lower()
    return [p for p in INCIDENT_HISTORY if p in low]


def detect_client_scale(text: str) -> int | None:
    """Largest credible 'N clients/endpoints managed' claim."""
    best: int | None = None
    for match in CLIENT_SCALE.finditer(text.lower()):
        try:
            value = int(match.group(1))
        except ValueError:
            continue
        if 10 <= value <= 500_000 and (best is None or value > best):
            best = value
    return best


def detect_operational_content(text: str) -> list[str]:
    low = text.lower()
    return [p for p in OPERATIONAL_CONTENT if p in low]


def detect_ecosystem(text: str) -> dict[str, list[str]]:
    low = text.lower()
    out: dict[str, list[str]] = {}
    for name, markers in (
        ("vendor_partner", VENDOR_PARTNER),
        ("association_member", ASSOCIATION_MEMBER),
        ("recent_award", AWARD_MARKERS),
    ):
        hits = [m for m in markers if m in low]
        if hits:
            out[name] = hits
    return out


def detect_high_value_hiring(text: str) -> list[str]:
    """Leadership roles that own the operational problem."""
    low = text.lower()
    return [r for r in HIGH_VALUE_ROLES if r in low]


def detect_operational_hiring(text: str) -> list[str]:
    """Front-line operational roles — weaker than leadership, still evidence."""
    low = text.lower()
    return [r for r in OPERATIONAL_ROLES if r in low]


def detect_any_operational_role(text: str) -> tuple[list[str], list[str]]:
    """(leadership_roles, frontline_roles) found in a job title or description."""
    return detect_high_value_hiring(text), detect_operational_hiring(text)


# --------------------------------------------------------------------------
# Recency
# --------------------------------------------------------------------------

# Applied on top of the normal age-based decay when only detected_at (crawl
# time) is known, never a real event_date. Without this, an undated event
# reads as permanently fresh forever — the exact bug this module used to
# have for nearly every signal it scored.
UNDATED_CONFIDENCE_PENALTY = 0.5


def recency_multiplier(
    event_date: str | None, detected_at: str | None = None, half_life_days: float = 180.0
) -> float:
    """Decay factor in [0.25, 1.0]. A three-year-old acquisition is not intent.

    Prefers `event_date` (when the thing actually happened). Falls back to
    `detected_at` (when the pipeline found it) at a confidence penalty, since
    an undated event could have happened any time before it was noticed.
    """
    basis = event_date or detected_at
    if not basis:
        return 0.25
    try:
        stamp = datetime.fromisoformat(basis)
    except ValueError:
        return 0.25
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    age_days = max(0.0, (datetime.now(timezone.utc) - stamp).total_seconds() / 86400.0)
    factor = 0.5 ** (age_days / half_life_days)
    if not event_date:
        factor *= UNDATED_CONFIDENCE_PENALTY
    return max(0.25, min(1.0, factor))


def score_intent(
    event_rows: list[Any],
    max_points: float = 20.0,
) -> dict[str, Any]:
    """Composite intent score from dated timing events (`intent_events` rows).

    Unlike a plain condition, the same event_type can legitimately recur
    (three acquisitions are three separate events) so rows are NOT deduped
    by name — each dated, sourced event contributes its own decayed points.
    """
    total = 0.0
    contributions: list[tuple[str, float, int]] = []

    for row in event_rows:
        name = row["event_type"] if "event_type" in row.keys() else row["signal"]
        weight = INTENT_WEIGHTS.get(name)
        if weight is None:
            continue
        keys = row.keys()
        event_date = row["event_date"] if "event_date" in keys else None
        detected_at = row["detected_at"] if "detected_at" in keys else None
        multiplier = recency_multiplier(event_date, detected_at)
        points = weight * multiplier
        total += points
        contributions.append((name, round(points, 2), TIER.get(name, 3)))

    contributions.sort(key=lambda c: -c[1])
    return {
        "score": round(min(total, max_points), 1),
        "raw": round(total, 1),
        "contributions": contributions,
        "tiers_present": sorted({c[2] for c in contributions}),
    }


def summarise(contributions: list[tuple[str, float, int]]) -> str:
    """Human-readable intent explanation for the tracker."""
    labels = {
        "hiring_operational": "hiring operational roles",
        "operational_hiring": "hiring operational roles",  # legacy name, see INTENT_WEIGHTS
        "hiring_volume": "hiring several operations roles at once",
        "hiring_context": "job copy shows 24/7, SLA and escalation duties",
        "tool_fragmentation": "fragmented tooling across categories",
        "compliance_pursuit": "active compliance/certification programme",
        "status_page": "runs a public status page",
        "incident_history": "publishes incident history",
        "client_scale_claim": "large managed client base",
        "vendor_partner": "vendor partner programme member",
        "association_member": "industry association member",
        "recent_award": "recent growth award",
        "acquisition": "acquisition or merger",
        "transformation": "cloud migration / modernisation",
        "expansion": "geographic or service expansion",
        "new_service": "new service line",
        "leadership_change": "new technology or operations leader",
        "funding": "funding or investment event",
        "growth": "rapid growth",
        "operational_content": "leaders writing about operational pain",
    }
    if not contributions:
        return ""
    parts = [f"{labels.get(name, name)} ({points})" for name, points, _ in contributions[:6]]
    return "; ".join(parts)
