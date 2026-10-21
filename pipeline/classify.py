"""Entity-type and geography classification.

The pipeline's biggest quality failure was validating *language* without asking
*what kind of organisation* published it. Splunk, Arctic Wolf and Mimecast all
talk fluently about 24/7 operations, SLAs and incident response — because they
sell to MSPs. Directories like ChannelPro rank MSPs. Associations represent
them. None of them are accounts.

SOP Module 26 asks the question directly: "Does the company genuinely operate
or manage technology, rather than only advise, recruit, resell, or complete
one-time projects?" This module answers it.

Types:
    investor         private equity, venture capital or asset management
    msp_operator     manages customer environments under recurring arrangements
    software_vendor  sells a product/platform (may serve MSPs)
    directory_media  rankings, reviews, listicles, trade press
    association      membership body
    end_user_org     hospital, credit union, university, manufacturer
    consultancy      advisory / project delivery without managed operations
    reseller         hardware or licence resale
    unknown          insufficient evidence
"""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse

from . import signals
from .dedup import domain_key

# --------------------------------------------------------------------------
# Type lexicons — phrases that indicate what the organisation *is*
# --------------------------------------------------------------------------

# First-person service ownership. This is what actually distinguishes an
# operator: it is speaking as the party responsible for someone else's estate.
OPERATOR_STRONG = [
    "our clients", "our customers", "we manage", "we monitor", "our noc",
    "our soc", "our technicians", "our engineers", "clients trust",
    "we become your", "your it department", "outsourced it department",
    "clients nationwide", "endpoints we manage", "we take care of",
    "businesses we support", "companies we serve", "managed services agreement",
    "client environments", "customer environments", "we support your",
]

OPERATOR_WEAK = [
    "we support", "service level agreement", "co-managed", "onsite support",
    "remote support", "help desk services", "vcio", "our team of technicians",
    "flat-rate", "monthly plan", "service plans", "our process", "our approach",
    "free network assessment", "schedule a consultation", "local it",
]

# Trade press and content sites. These quote MSPs constantly, so they trip
# operator markers; what gives them away is publishing furniture appearing
# dozens of times.
# Publisher-only furniture. Deliberately excludes webinar / newsletter /
# podcast / resource library / press release: vendors run those too, and so do
# MSPs with a blog, so they cannot separate a publisher from an operator.
MEDIA_MARKERS = [
    "advertise with us", "media kit", "our editorial", "editorial team",
    "write for us", "contributor", "posted on", "min read", "in this issue",
    "rss feed", "our writers", "newsroom", "featured story", "sponsored content",
    "magazine", "read the article", "continue reading", "related articles",
    "by the editors", "guest column", "subscribe to the print",
]

VENDOR_MARKERS = [
    "request a demo", "book a demo", "free trial", "start free", "try it free",
    "our platform", "the platform", "platform", "product tour", "view pricing",
    "pricing plans", "per month per user", "api documentation", "developer docs",
    "developer portal", "sdk", "release notes", "changelog",
    "integrations marketplace", "app marketplace", "download the app",
    "open source", "github.com/", "customer stories", "g2 crowd",
    "magic quadrant", "forrester wave", "analyst report", "our software",
    "software platform", "saas platform", "deploy our", "install the agent",
    "supported integrations", "product documentation", "sign up free",
    "get started free", "book a live demo", "demo", "datasheet", "whitepaper",
    "solution brief", "resource library", "our product", "product suite",
    "documentation", "trusted by",
]

DIRECTORY_MARKERS = [
    "top 10", "top 15", "top 20", "top 25", "top 50", "top 100",
    "best managed service", "best it services", "compare providers",
    "advertise with us", "sponsored listing", "get listed", "list your company",
    "submit your company", "claim your profile", "our editorial", "editorial team",
    "write for us", "subscribe to our newsletter", "press releases",
    "vendor directory", "buyer's guide", "buyers guide", "reviews and ratings",
    "leaderboard", "rankings", "read the full review", "sponsored content",
    "market research report", "research report", "industry report",
]

ASSOCIATION_MARKERS = [
    "become a member", "membership benefits", "our members", "member directory",
    "board of directors", "annual conference", "chapter meeting", "join the association",
    "membership dues", "advocacy", "our mission is to represent", "nonprofit organization",
    "501(c)", "trade association", "member login",
]

END_USER_MARKERS = [
    "patients", "patient care", "our hospital", "medical center", "health system",
    "clinic locations", "schedule an appointment", "our physicians",
    "become a member today", "open an account", "checking account", "savings account",
    "loan rates", "apply for a loan", "branch locations", "atm locations",
    "students", "faculty", "campus", "admissions", "enroll now", "our university",
    "residents", "city council", "public works", "our congregation",
    "shop now", "our products are manufactured", "our facility produces",
]

CONSULTANCY_MARKERS = [
    "our consultants", "advisory services", "strategy consulting",
    "we advise", "assessment services", "readiness assessment", "audit services",
    "staff augmentation", "project delivery", "statement of work",
    "custom software development", "application development services",
    # Accounting / professional-services firms. Wipfli, BDO and Guidehouse all
    # reached a real export as "msp_operator": they run large IT practices and
    # speak fluent operator language, but they sell advisory engagements.
    "tax services", "tax planning", "assurance services", "audit and assurance",
    "cpa", "certified public accountant", "accounting services", "bookkeeping",
    "wealth management", "actuarial", "transaction advisory", "due diligence",
    "management consulting", "our practice", "practice leader", "engagement team",
    "thought leadership", "industries we serve", "professional services firm",
    "risk advisory", "regulatory compliance consulting", "valuation services",
    "outsourced accounting", "cfo services", "internal audit", "forensic",
    "our partners and principals", "client engagements", "consulting practice",
]

# Technology vocabulary. Required before the operator reading is available,
# so that industry-neutral service language ("our clients", "we manage")
# cannot make a construction firm or a law practice look like an MSP.
# Deliberately excludes words a non-IT business uses in its own domain:
# "network", "infrastructure", "cloud", "hardware", "software", "data center"
# (a construction firm builds data centres and installs networks).
IT_DOMAIN_MARKERS = [
    "managed it", "it services", "it support", "it services", "help desk",
    "helpdesk", "service desk", "cybersecurity", "cyber security",
    "microsoft 365", "office 365", "azure", "aws", "ransomware", "endpoint",
    "firewall", "voip", "virtualization", "active directory", "vpn",
    "antivirus", "phishing", "malware", "patch management", "sql server",
    "workstation", "email security", "network security", "it infrastructure",
    "disaster recovery", "backup and recovery", "remote monitoring",
    "managed services", "technology services", "cloud migration", "sharepoint",
    "multi-factor authentication", "penetration testing", "siem", "rmm",
]

# Investment firms describe themselves in a way nothing else does. Apollo
# Global Management reached a real export as entity_type=unknown because none
# of the other lexicons fit an asset manager at all.
INVESTOR_MARKERS = [
    "assets under management", "aum", "portfolio companies", "our portfolio",
    "private equity", "venture capital", "growth equity", "buyout",
    "limited partners", "fund i", "fund ii", "fund iii", "capital raised",
    "we invest in", "our investments", "investment strategy", "asset management",
    "credit strategies", "real assets", "institutional investors",
    "hedge fund", "wealth solutions", "capital markets", "deal team",
]

RESELLER_MARKERS = [
    "authorized reseller", "shop our products", "add to cart", "product catalog",
    "wholesale", "request a quote for hardware", "we sell", "distributor",
]


def _count(text: str, markers: list[str], per_marker_cap: int = 4) -> tuple[float, list[str]]:
    """Weighted frequency, not mere presence.

    Presence alone misleads badly: trade press quotes "our clients" once inside
    an article and scores as an operator, while a product company saying
    "platform" fifty times scores the same as one saying it once. Counting
    occurrences (capped per marker so one repeated phrase cannot dominate)
    separates what a site *is* from what it merely *mentions*.
    """
    total = 0.0
    hits: list[str] = []
    for marker in markers:
        count = text.count(marker)
        if count:
            total += min(count, per_marker_cap)
            hits.append(f"{marker} x{count}" if count > 1 else marker)
    hits.sort(key=lambda h: -int(h.rsplit(" x", 1)[1]) if " x" in h else -1)
    return total, hits


def classify_entity(text: str, website: str | None = None) -> dict[str, Any]:
    """Decide what kind of organisation published this site.

    Returns {type, confidence, scores, evidence}.
    """
    low = signals.normalise(text)
    if len(low) < 200:
        return {"type": "unknown", "confidence": 0.0, "scores": {}, "evidence": []}

    # Longer pages accumulate more of everything, so normalise to a density.
    norm = max(1.0, len(low) / 20000.0)

    strong_n, strong_hits = _count(low, OPERATOR_STRONG)
    weak_n, weak_hits = _count(low, OPERATOR_WEAK)
    vendor_n, vendor_hits = _count(low, VENDOR_MARKERS)
    directory_n, directory_hits = _count(low, DIRECTORY_MARKERS)
    media_n, media_hits = _count(low, MEDIA_MARKERS)
    assoc_n, assoc_hits = _count(low, ASSOCIATION_MARKERS)
    enduser_n, enduser_hits = _count(low, END_USER_MARKERS)
    consult_n, consult_hits = _count(low, CONSULTANCY_MARKERS)
    reseller_n, reseller_hits = _count(low, RESELLER_MARKERS)
    investor_n, investor_hits = _count(low, INVESTOR_MARKERS)

    # First-person ownership language carries the operator case. Generic
    # "managed services" phrasing is deliberately weak here — vendors and
    # publishers use it constantly.
    recurring = signals.detect_group(low, signals.RECURRING)
    operator_score = (strong_n * 1.6 + weak_n * 0.5 + len(recurring) * 0.4) / norm

    vendor_score = vendor_n * 0.9 / norm
    directory_score = (directory_n * 1.6 + media_n * 0.7) / norm
    assoc_score = assoc_n * 1.6 / norm
    enduser_score = enduser_n * 1.3 / norm
    consult_score = consult_n * 1.0 / norm
    reseller_score = reseller_n * 1.2 / norm
    investor_score = investor_n * 1.7 / norm

    directory_hits = (directory_hits + media_hits)[:8]

    # Domain hints.
    dk = (domain_key(website) or "").lower()
    if dk.endswith(".org"):
        assoc_score += 1.5
    if dk.endswith(".edu") or dk.endswith(".gov"):
        enduser_score += 4.0
    if re.search(r"\b(msp|it)(review|ranked|directory|list|companies|global)\b", dk):
        directory_score += 3.0

    scores = {
        "software_vendor": round(vendor_score, 2),
        "directory_media": round(directory_score, 2),
        "association": round(assoc_score, 2),
        "end_user_org": round(enduser_score, 2),
        "consultancy": round(consult_score, 2),
        "reseller": round(reseller_score, 2),
        "investor": round(investor_score, 2),
    }

    # "Our clients", "we manage" and "we support" are industry-neutral. A
    # construction firm says all three — about buildings. An operator must also
    # be speaking about technology, so require IT vocabulary before the
    # operator reading is available at all.
    # Distinct terms, not occurrences: one word repeated is not a vocabulary.
    it_distinct = sum(1 for m in IT_DOMAIN_MARKERS if m in low)
    if it_distinct < 3:
        operator_score *= 0.15

    scores["msp_operator"] = round(operator_score, 2)

    best_type = max(scores, key=lambda k: scores[k])
    best_score = scores[best_type]

    # An operator is the working hypothesis for this ICP, so another type has
    # to win clearly — not by a rounding error — before we discard the account.
    # Without this, an MSP with an active blog loses to `directory_media` and a
    # genuine prospect is thrown away.
    OVERRIDE_MARGIN = 1.4
    if (
        best_type != "msp_operator"
        and strong_n >= 2
        and operator_score >= 2.0
        and best_score < operator_score * OVERRIDE_MARGIN
    ):
        best_type = "msp_operator"
        best_score = operator_score

    if best_score < 2.0:
        return {"type": "unknown", "confidence": 0.0, "scores": scores, "evidence": []}

    runner_up = sorted(scores.values(), reverse=True)[1] if len(scores) > 1 else 0.0
    confidence = round(min(1.0, (best_score - runner_up) / max(best_score, 1.0)), 2)

    evidence_map = {
        "msp_operator": strong_hits + weak_hits,
        "software_vendor": vendor_hits,
        "directory_media": directory_hits,
        "association": assoc_hits,
        "end_user_org": enduser_hits,
        "consultancy": consult_hits,
        "reseller": reseller_hits,
        "investor": investor_hits,
    }

    return {
        "type": best_type,
        "confidence": confidence,
        "scores": scores,
        "evidence": evidence_map.get(best_type, [])[:6],
    }


# --------------------------------------------------------------------------
# Geography
# --------------------------------------------------------------------------

NON_US_TLDS = {
    "de", "uk", "fr", "in", "es", "it", "nl", "se", "no", "dk", "fi", "pl", "ru",
    "cn", "jp", "kr", "au", "nz", "ca", "br", "mx", "za", "ie", "ch", "at", "be",
    "pt", "gr", "cz", "ro", "hu", "il", "ae", "sg", "my", "ph", "id", "th", "vn",
    "tr", "ua", "pk", "bd", "lk", "ng", "ke", "eg", "sa", "qa", "hk", "tw",
}

# Matched with word boundaries, NOT as substrings. "oy " previously matched
# inside "employ ", "deploy " and "enjoy ", labelling US companies as Finnish;
# "abn " matched inside ordinary prose the same way.
NON_US_LEGAL_FORMS = [
    r"gmbh", r"pvt\.?\s*ltd", r"private limited", r"pty\.?\s*ltd",
    r"sp\.\s*z\s*o\.o\.", r"s\.a\.s\b", r"sarl", r"s\.r\.l\b",
    r"b\.v\.", r"n\.v\.", r"\bab\s+publ\b", r"\boy\b", r"\baps\b",
    r"registered in england", r"companies house", r"company number",
    r"vat number", r"vat reg", r"\babn\b", r"\bacn\b", r"\bcin\b:",
    r"\bgstin\b", r"\bkvk\b", r"siret", r"\bnif\b",
]

_LEGAL_FORM_RE = re.compile("|".join(NON_US_LEGAL_FORMS), re.IGNORECASE)

NON_US_PHONE = re.compile(r"\+(?:44|49|33|91|61|64|31|46|47|45|48|39|34|351|353|41|43|32|420|36|380|7|86|81|82|65|60|63|66|84|90|27|55|52|971|966|92|880)\b")
NON_US_CURRENCY = re.compile(r"[£€₹¥]|\bEUR\b|\bGBP\b|\bINR\b|\bAUD\b|\bCAD\b")

US_ADDRESS = re.compile(r"\b(A[LKZR]|C[AOT]|D[CE]|FL|GA|HI|I[ADLN]|K[SY]|LA|M[ADEINOST]|N[CDEHJMVY]|O[HKR]|PA|RI|S[CD]|T[NX]|UT|V[AT]|W[AIVY])\s+\d{5}(?:-\d{4})?\b")


def classify_geography(
    text: str,
    website: str | None,
    phone: str | None = None,
    extra_urls: list[str] | None = None,
) -> dict[str, Any]:
    """Decide whether this is a US organisation.

    `extra_urls` carries the URLs a company was originally discovered at. The
    crawler prefers the root domain, so a regional path such as /nl/en is lost
    by the time geography is assessed unless it is passed in explicitly.

    Returns {is_us, confidence, reasons, state}.
    """
    reasons: list[str] = []
    low = signals.normalise(text)
    candidate_urls = [u for u in ([website] + (extra_urls or [])) if u]

    # 1. Country-code TLD is close to decisive.
    dk = (domain_key(website) or "").lower()
    tld = dk.rsplit(".", 1)[-1] if "." in dk else ""
    if tld in NON_US_TLDS:
        reasons.append(f"non-US ccTLD .{tld}")
        return {"is_us": False, "confidence": 0.9, "reasons": reasons, "state": None}

    # A country path segment such as /in-en, /de/ or /nl/en signals a regional
    # site. The code must end the path segment (a locale suffix like /de-de/
    # or /en-us/ counts too) — it must NOT just prefix an arbitrary word.
    # "it" is both Italy's ccTLD path code and the single most common word in
    # this entire industry vertical: an earlier, looser version of this regex
    # (allowing a bare hyphen to follow) matched "/it-support-delaware",
    # "/it-solutions-delaware" and similar as "Italy", sending real US MSPs to
    # the *permanent* exclusion ledger.
    for candidate in candidate_urls:
        path = (urlparse(candidate).path if candidate else "") or ""
        if re.search(
            r"/(?:in|de|fr|uk|au|jp|cn|br|es|it|nl|se|no|dk|fi|pl|ru|kr|mx|za|ie|ch|at|be|pt)"
            r"(?:/|$|-[a-z]{2}(?:/|$))",
            path.lower(),
        ):
            reasons.append(f"country-specific path {path[:24]}")
            return {"is_us": False, "confidence": 0.6, "reasons": reasons, "state": None}

    # 2. A US postal address is strong positive evidence.
    match = US_ADDRESS.search(text or "")
    if match:
        reasons.append(f"US address ({match.group(1)})")
        return {"is_us": True, "confidence": 0.9, "reasons": reasons, "state": match.group(1)}

    # 3. Negative indicators.
    negatives = 0
    # Count DISTINCT markers. A Finnish "Oy" plus a "VAT number" are two
    # independent signals; stopping at the first match under-counted and left
    # obviously non-US companies at is_us=None, which the clean stage keeps.
    legal_hits = {m.group(0).strip().lower() for m in _LEGAL_FORM_RE.finditer(text or "")}
    if legal_hits:
        reasons.append("non-US legal markers: " + ", ".join(sorted(legal_hits)[:3]))
        negatives += min(len(legal_hits), 2)
    if NON_US_PHONE.search(text or "") or (phone and NON_US_PHONE.search(phone)):
        reasons.append("non-US phone country code")
        negatives += 1
    if NON_US_CURRENCY.search(text or ""):
        reasons.append("non-USD currency")
        negatives += 1

    if negatives >= 2:
        return {"is_us": False, "confidence": 0.75, "reasons": reasons, "state": None}
    if negatives == 1:
        return {"is_us": None, "confidence": 0.4, "reasons": reasons, "state": None}

    return {"is_us": None, "confidence": 0.2, "reasons": ["no location evidence found"], "state": None}


# Which entity types may become accounts, by segment.
ACCEPTABLE_TYPES = {
    "Managed technology provider": {"msp_operator"},
    "Healthcare IT environment": {"msp_operator", "end_user_org"},
    "Financial organisation": {"msp_operator", "end_user_org"},
    "Regulated mid-market IT": {"msp_operator", "end_user_org"},
}


def is_acceptable(entity_type: str, segment: str | None) -> bool:
    allowed = ACCEPTABLE_TYPES.get(segment or "", {"msp_operator"})
    return entity_type in allowed


# --------------------------------------------------------------------------
# Large-enterprise detection (SOP Modules 25 / 28)
# --------------------------------------------------------------------------

# Scale language that only a global enterprise uses about itself.
ENTERPRISE_MARKERS = [
    "fortune 500", "fortune 100", "fortune global", "s&p 500",
    "nyse:", "nasdaq:", "listed on the new york stock exchange",
    "our global workforce", "employees worldwide", "colleagues worldwide",
    "associates worldwide", "offices in over", "operations in over",
    "present in over", "countries around the world", "across 6 continents",
    "global delivery centers", "global delivery centres", "delivery centers across",
    "world's largest", "global leader in", "multinational",
    "annual revenue of", "billion in revenue", "billion in annual",
]

# "150,000 employees", "over 300,000 associates"
_HEADCOUNT_AT_SCALE = re.compile(
    r"\b(\d{1,3}(?:,\d{3})+|\d{5,})\s*\+?\s*"
    r"(?:employees|associates|colleagues|professionals|people|staff)\b"
)

_COUNTRY_COUNT = re.compile(r"\b(?:in|across|over)\s+(\d{2,3})\s+countries\b")


def is_large_enterprise(
    text: str,
    website: str | None,
    enterprise_domains: set[str],
    employee_estimate: int | None = None,
    headcount_ceiling: int = 5000,
) -> tuple[bool, str]:
    """True when a company is too large to be a realistic first pilot.

    Returns (is_large, reason). SOP Module 28 lists "very large enterprise
    without timing or access" as an exclusion: the evaluation cycle is long and
    the practicality low, so these are removed rather than nurtured.
    """
    dk = (domain_key(website) or "").lower()
    if dk and (dk in enterprise_domains
               or any(dk == d or dk.endswith("." + d) for d in enterprise_domains)):
        return True, f"Global enterprise / big tech ({dk}) — long cycle, not a first pilot"

    if employee_estimate and employee_estimate > headcount_ceiling:
        return True, f"Headcount ~{employee_estimate:,} exceeds the {headcount_ceiling:,} ceiling"

    low = signals.normalise(text)

    match = _HEADCOUNT_AT_SCALE.search(low)
    if match:
        try:
            value = int(match.group(1).replace(",", ""))
            if value > headcount_ceiling:
                return True, f"States a workforce of ~{value:,} — enterprise scale"
        except ValueError:
            pass

    country_match = _COUNTRY_COUNT.search(low)
    if country_match:
        try:
            if int(country_match.group(1)) >= 15:
                return True, (
                    f"Operates in {country_match.group(1)} countries — "
                    "global enterprise, not a contained pilot"
                )
        except ValueError:
            pass

    hits = [m for m in ENTERPRISE_MARKERS if m in low]
    if len(hits) >= 2:
        return True, "Enterprise-scale self-description: " + ", ".join(hits[:3])

    return False, ""
