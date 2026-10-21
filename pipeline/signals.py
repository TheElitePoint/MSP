"""Signal lexicons and text-based signal detection.

Every phrase here maps to a condition the SOP asks the researcher to look for:

  RECURRING     Module 20 / 26 — is this an operator or a project shop?
  OPERATIONAL   Module 18 / 27 — 24/7, SLA, NOC, incident, continuity
  COMPLEXITY    Module 18     — multiple environments, clouds, locations
  VERTICAL      Modules 21/22 — healthcare, finance, regulated pressure
  TIMING        Modules 54/55 — hiring, acquisition, migration, expansion
  ACCESS        Module 27     — visible leadership and contact routes
  EXCLUSION     Module 28     — weak-fit and false-positive patterns

Detection is deliberately phrase-based and auditable: every hit records the
phrase and the page it came from, so a human can reproduce the decision.
"""

from __future__ import annotations

import re
from typing import Iterable

# --------------------------------------------------------------------------
# Positive lexicons
# --------------------------------------------------------------------------

RECURRING: dict[str, list[str]] = {
    "managed_services": [
        "managed service provider", "managed services", "managed it services",
        "managed it", "it managed services", "managed technology services",
    ],
    "co_managed": ["co-managed it", "comanaged it", "co-managed services", "it co-management"],
    "outsourced_it": ["outsourced it", "it outsourcing", "fully outsourced", "virtual cio", "vcio"],
    "managed_infrastructure": [
        "managed infrastructure", "infrastructure management services",
        "managed network", "network management services",
    ],
    "managed_cloud": [
        "managed cloud", "cloud managed services", "cloud operations",
        "cloud management services", "managed azure", "managed aws",
    ],
    "managed_security": [
        "managed security service", "mssp", "managed cybersecurity",
        "managed detection and response", "soc as a service", "managed soc",
        "security operations center",
    ],
    "recurring_commercial": [
        "monthly subscription", "flat-rate it", "flat rate it", "per user per month",
        "recurring services", "service agreement", "managed services agreement",
        "ongoing support", "retainer",
    ],
}

OPERATIONAL: dict[str, list[str]] = {
    "always_on_support": [
        "24/7", "24x7", "24 x 7", "around the clock", "round-the-clock", "always-on support",
    ],
    "sla": ["service level agreement", "sla-backed", "slas", "guaranteed response", "uptime guarantee"],
    "noc": ["network operations center", "network operations centre", "noc services", "our noc"],
    "service_desk": ["service desk", "help desk", "helpdesk", "tier 1 support", "tier 2 support"],
    "incident_response": [
        "incident response", "incident management", "escalation process",
        "escalation path", "major incident", "triage",
    ],
    "monitoring": [
        "proactive monitoring", "remote monitoring", "rmm", "24/7 monitoring",
        "alerting", "observability",
    ],
    "change_management": ["change management", "change control", "change advisory", "release management"],
    "continuity": [
        "business continuity", "disaster recovery", "backup and recovery",
        "bcdr", "resilience", "failover", "rto", "rpo",
    ],
    "service_management": [
        "itil", "service management", "itsm", "configuration management",
        "cmdb", "asset management", "ticketing system",
    ],
}

COMPLEXITY: dict[str, list[str]] = {
    "multi_client": [
        "client environments", "customer environments", "our clients", "clients nationwide",
        "endpoints managed", "devices managed", "users supported",
    ],
    "hybrid_multicloud": [
        "hybrid cloud", "multi-cloud", "multicloud", "on-premise and cloud",
        "private cloud", "colocation", "data center",
    ],
    "multi_location": [
        "multiple locations", "offices in", "nationwide", "multi-site", "branch offices",
    ],
    "platform_breadth": [
        "microsoft 365", "azure", "aws", "vmware", "citrix", "active directory",
        "sd-wan", "virtualization", "identity management",
    ],
    "integration": ["integrations", "api integration", "systems integration", "interoperability"],
}

VERTICAL: dict[str, list[str]] = {
    "healthcare": [
        "healthcare", "hospital", "clinic", "hipaa", "phi", "ehr", "emr", "epic",
        "cerner", "meditech", "patient care", "clinical systems", "medical practice",
    ],
    "financial": [
        "credit union", "community bank", "financial institution", "banking", "fintech",
        "wealth management", "payments", "ncua", "ffiec", "glba", "pci dss", "core banking",
    ],
    "regulated_other": [
        "soc 2", "soc2", "hitrust", "cmmc", "nist 800-171", "iso 27001", "fedramp",
        "cjis", "ferpa", "compliance requirements", "regulated industries",
    ],
    "critical_infrastructure": [
        "manufacturing", "logistics", "utilities", "critical infrastructure",
        "legal", "insurance", "government",
    ],
}

ACCESS: dict[str, list[str]] = {
    "leadership_page": [
        "leadership team", "our team", "meet the team", "executive team",
        "management team", "founder", "our leadership",
    ],
    "exec_titles": [
        "chief executive officer", "chief technology officer", "chief information officer",
        "chief operating officer", "vp of service delivery", "vice president of operations",
        "director of managed services", "director of it operations", "president",
    ],
    "contact_route": ["contact us", "book a meeting", "schedule a consultation", "request a quote"],
}

# --------------------------------------------------------------------------
# Timing / intent (SOP Modules 54, 55)
# --------------------------------------------------------------------------

HIRING_ROLES: list[str] = [
    "service delivery manager", "service delivery director", "managed services director",
    "managed services manager", "noc manager", "noc engineer", "soc manager", "soc analyst",
    "incident manager", "problem manager", "change manager", "infrastructure manager",
    "cloud operations manager", "cloud operations engineer", "it service manager",
    "technical account manager", "service desk manager", "help desk manager",
    "support operations manager", "enterprise architect", "configuration manager",
    "director of operations", "vp of operations", "operations manager",
]

HIRING_CONTEXT: list[str] = [
    "on-call", "on call rotation", "24/7 shift", "multiple client environments",
    "sla", "escalation", "incident ownership", "ticket queue",
]

TIMING_EVENTS: dict[str, list[str]] = {
    "acquisition": [
        "acquires", "acquisition of", "has acquired", "merger with", "merges with",
        "joins forces", "acquired by",
    ],
    "expansion": [
        "expands into", "new office", "opens office", "expanding to",
        "new location", "entering the", "expands its",
    ],
    "new_service": [
        "launches", "launched a new", "introduces", "new service offering",
        "now offering", "unveils",
    ],
    "transformation": [
        "cloud migration", "migrating to", "digital transformation", "modernization",
        "modernisation", "platform transformation", "infrastructure overhaul",
    ],
    "leadership_change": [
        "appoints", "names new", "joins as chief", "new cto", "new cio",
        "new coo", "promoted to vice president", "hires new",
    ],
    "growth": [
        "fastest growing", "inc. 5000", "msp 501", "record growth",
        "funding round", "raises", "investment from", "private equity",
    ],
}

# --------------------------------------------------------------------------
# Exclusions (SOP Module 28)
# --------------------------------------------------------------------------

EXCLUSION_PATTERNS: dict[str, list[str]] = {
    "web_or_marketing_agency": [
        "web design", "website design", "web development agency", "seo services",
        "digital marketing agency", "social media marketing", "branding agency",
        "graphic design", "ppc management",
    ],
    "staffing_or_recruiting": [
        "staffing agency", "it staffing", "recruitment agency", "talent acquisition services",
        "contract staffing", "executive search", "temp staffing", "resume",
    ],
    "residential_or_repair": [
        "computer repair", "laptop repair", "screen repair", "residential customers",
        "home computer", "phone repair", "we buy used", "data recovery for consumers",
        "virus removal",
    ],
    "hardware_reseller_only": [
        "authorized reseller", "shop our products", "add to cart", "buy now",
        "product catalog", "wholesale distributor",
    ],
    "project_only_consultancy": [
        "project-based engagements", "one-time project", "fixed-scope projects",
        "software development company", "custom software development",
        "app development agency", "mobile app development",
    ],
    "training_only": ["training courses", "certification bootcamp", "online courses", "e-learning platform"],
    "telecom_broker_only": ["telecom expense management", "carrier broker", "phone system reseller"],
}

# Phrases that neutralise a false exclusion hit — e.g. an MSP that *also*
# happens to mention it builds custom software for clients.
EXCLUSION_OVERRIDES: list[str] = [
    "managed service provider", "managed it services", "co-managed it",
    "network operations center", "24/7 monitoring", "service level agreement",
]

_WS = re.compile(r"\s+")


def normalise(text: str | None) -> str:
    if not text:
        return ""
    return _WS.sub(" ", text.lower())


def _find(text: str, phrases: Iterable[str]) -> list[str]:
    return [p for p in phrases if p in text]


def detect_group(text: str, lexicon: dict[str, list[str]]) -> dict[str, list[str]]:
    """Return {signal_name: [matched phrases]} for every signal with a hit."""
    text = normalise(text)
    hits: dict[str, list[str]] = {}
    for signal, phrases in lexicon.items():
        matched = _find(text, phrases)
        if matched:
            hits[signal] = matched
    return hits


def detect_all(text: str) -> dict[str, dict[str, list[str]]]:
    """Run every positive lexicon over a body of text."""
    return {
        "recurring": detect_group(text, RECURRING),
        "operational": detect_group(text, OPERATIONAL),
        "complexity": detect_group(text, COMPLEXITY),
        "vertical": detect_group(text, VERTICAL),
        "access": detect_group(text, ACCESS),
    }


def detect_exclusions(text: str) -> dict[str, list[str]]:
    """Weak-fit patterns, suppressed when strong managed-services language is present."""
    text = normalise(text)
    hits = detect_group(text, EXCLUSION_PATTERNS)
    if not hits:
        return {}
    override_hits = _find(text, EXCLUSION_OVERRIDES)
    if len(override_hits) >= 2:
        # Strong operator language present — a single stray phrase is not
        # enough to exclude. Keep only patterns with multiple distinct hits.
        return {k: v for k, v in hits.items() if len(v) >= 3}
    return hits


def detect_hiring(text: str) -> tuple[list[str], list[str]]:
    """Operational roles being hired for, plus operational context phrases."""
    text = normalise(text)
    return _find(text, HIRING_ROLES), _find(text, HIRING_CONTEXT)


def detect_timing(text: str) -> dict[str, list[str]]:
    return detect_group(text, TIMING_EVENTS)


# --------------------------------------------------------------------------
# Headcount extraction
# --------------------------------------------------------------------------

_HEADCOUNT_NOUNS = (
    r"employees|people|professionals|engineers|technicians|team members|"
    r"staff|specialists|consultants|experts|it professionals|full-time employees"
)

_EMPLOYEE_PATTERNS = [
    # Only accept first-person workforce claims. A generic "150 employees"
    # frequently describes a customer in a case study or the size of the
    # businesses the MSP supports; treating that as the provider's own
    # headcount admitted several sub-30 firms into the 30+ campaign.
    re.compile(
        rf"(?:our (?:team|staff|workforce) (?:of|has)|we (?:have|employ)|"
        rf"we are a team of|our company employs)\s+(?:over\s+|more than\s+|nearly\s+)?"
        rf"(\d{{2,5}})\+?\s+(?:{_HEADCOUNT_NOUNS})"
    ),
    # "our 60-person team", "our 500-employee company"
    re.compile(r"our\s+(\d{2,5})\s*-\s*(?:person|employee)\s+(?:team|company|firm)"),
    # "our 30-strong team"
    re.compile(r"our\s+(\d{2,5})\s*-\s*strong\s+(?:team|company|firm|workforce)"),
    # "we employ 75", "we are a team of over 40" (noun may be omitted)
    re.compile(r"we (?:employ|are a team of)\s+(?:over\s+|more than\s+)?(\d{2,5})\+?"),
    # "our 95 employees"
    re.compile(rf"our\s+(\d{{2,5}})\+?\s+(?:{_HEADCOUNT_NOUNS})"),
    # "our headcount is 500", "our workforce of 220", "we have grown to 85".
    re.compile(
        rf"(?:our headcount (?:is|of)|our workforce of|we have grown to)\s+(\d{{2,5}})\+?"
        rf"(?:\s+(?:{_HEADCOUNT_NOUNS}))?"
    ),
]


def extract_employee_estimate(text: str) -> int | None:
    """Best-effort headcount from website copy. Returns None when unstated."""
    text = normalise(text)
    best: int | None = None
    for pattern in _EMPLOYEE_PATTERNS:
        for match in pattern.finditer(text):
            try:
                value = int(match.group(1))
            except (ValueError, IndexError):
                continue
            if 2 <= value <= 20000 and (best is None or value > best):
                best = value
    return best


def employee_range_for(estimate: int | None) -> str | None:
    """Map a headcount to the SOP Module 25 bands."""
    if estimate is None:
        return None
    for upper, label in ((10, "1-10"), (50, "11-50"), (200, "51-200"),
                         (500, "201-500"), (2000, "501-2000")):
        if estimate <= upper:
            return label
    return "2000+"
