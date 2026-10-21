"""Grounded LLM tasks (SOP Modules 29, 59, 61, 63).

Every prompt here follows the SOP's safe operating method (Module 59):
supply the evidence already collected, ask the model to separate facts from
inference, and forbid invention. The model never produces companies,
headcounts, customers or tools — only judgements about text it was given.

Its verdict is stored *alongside* the deterministic one, never silently in
place of it. Where the two disagree, the record is flagged for human review,
which is exactly what Module 29 requires: "AI summaries are research
assistance, not proof."
"""

from __future__ import annotations

import logging
import random
import re
import sqlite3
from datetime import datetime
from typing import Any

from .llm import GroqClient, LLMBudgetExhausted
from .store import Store

log = logging.getLogger("pipeline.llm_tasks")

ENTITY_TYPES = [
    "msp_operator", "software_vendor", "directory_media", "association",
    "end_user_org", "consultancy", "reseller", "unknown",
]

ENTITY_SYSTEM = f"""You classify B2B company websites for a sales-research pipeline.

Decide what kind of organisation published the supplied page text. Choose ONE:
- msp_operator: continuously manages OTHER companies' IT/cloud/security/service-desk
  environments under recurring arrangements. Speaks as the party responsible.
- software_vendor: sells a product or platform (it may sell TO managed providers).
- directory_media: rankings, reviews, listicles, trade press, job boards.
- association: membership or trade body.
- end_user_org: a hospital, bank, university, manufacturer, charity or similar
  that consumes IT rather than selling it.
- consultancy: advisory, audit, accounting or project delivery WITHOUT ongoing
  managed operations.
- reseller: hardware or licence resale only.
- unknown: the text does not support a confident answer.

Rules:
- Use ONLY the supplied text. Do not use outside knowledge about the company.
- A company that writes ABOUT managed services is not necessarily an operator.
- Quote the exact phrase that decided it.
- If the text is a cookie banner, error page or navigation only, answer unknown.

Reply as JSON:
{{"entity_type": one of {ENTITY_TYPES},
  "confidence": 0.0-1.0,
  "evidence": "exact quote from the text, max 20 words",
  "reasoning": "one sentence"}}"""

RATIONALE_SYSTEM = """You write account rationales for a B2B sales-research tracker.

Write ONE paragraph of 60-90 words in this order:
business model, operational complexity, problem relevance, timing, buyer access.

Rules:
- Use ONLY the supplied evidence. Never invent customers, revenue, tools,
  headcount, or traction.
- If a component has no evidence, say so briefly ("timing not established")
  rather than inventing one.
- Plain professional English. No marketing language. No bullet points.
- Do not begin with the company name repeated from the input.

Reply as JSON: {"rationale": "...", "unsupported_claims_removed": ["..."]}"""

CHALLENGE_SYSTEM = """You stress-test a sales prospect before it is approved (a
false-positive check).

Given the evidence, give the strongest reasons this company may be a POOR target
for a platform that connects operational data (incidents, changes, dependencies,
ownership) for companies managing complex technology environments.

Rules:
- Use ONLY the supplied evidence.
- Be specific. "Not enough information" is a valid and useful concern.
- Recommend approve, hold, or exclude based only on what is supplied.

Reply as JSON:
{"concerns": ["..."], "evidence_needed": ["..."],
 "verdict": "approve" | "hold" | "exclude", "reasoning": "one sentence"}"""


def _evidence_blob(store: Store, row: sqlite3.Row, max_chars: int = 6000) -> str:
    """Assemble the crawled evidence for one company."""
    parts = [
        f"COMPANY NAME (unverified): {row['company_name']}",
        f"WEBSITE: {row['website']}",
        f"HQ STATE: {row['headquarters_state']} (confirmed={row['hq_confirmed']})",
        f"EMPLOYEE RANGE: {row['employee_range'] or 'not found'}",
        f"SERVICES DETECTED: {row['services'] or 'none'}",
        f"CUSTOMER INDUSTRIES: {row['customer_industries'] or 'none'}",
        f"OPERATIONAL SIGNALS: {row['operational_signals'] or 'none'}",
        f"TIMING SIGNALS: {row['timing_signal'] or 'none'}",
        f"TOOLS NAMED: {row['tools_found'] or 'none'}",
        f"OPEN OPERATIONAL ROLES: {row['open_roles'] or 0}",
    ]
    facts = store.get_evidence(row["id"])[:12]
    if facts:
        parts.append("SOURCE-BACKED FACTS:")
        parts += [f"  - {f['supported_fact']}" for f in facts]
    description = (row["company_description"] or "")[:2000]
    if description:
        parts.append(f"WEBSITE TEXT: {description}")
    return "\n".join(parts)[:max_chars]


def verify_entity(
    client: GroqClient, store: Store, row: sqlite3.Row, page_text: str
) -> dict[str, Any] | None:
    """Second opinion on entity type. Returns the model's verdict, or None."""
    if not client.available or not page_text:
        return None
    user = f"WEBSITE: {row['website']}\n\nPAGE TEXT:\n{page_text[:9000]}"
    try:
        result = client.complete_json(ENTITY_SYSTEM, user, role="reasoning", max_tokens=650)
    except LLMBudgetExhausted:
        raise
    if not result:
        return None
    entity = str(result.get("entity_type", "")).strip().lower()
    if entity not in ENTITY_TYPES:
        return None
    return {
        "entity_type": entity,
        "confidence": float(result.get("confidence") or 0.0),
        "evidence": str(result.get("evidence") or "")[:300],
        "reasoning": str(result.get("reasoning") or "")[:300],
    }


def write_rationale(
    client: GroqClient, store: Store, row: sqlite3.Row
) -> str | None:
    """SOP Module 63 rationale in prose, grounded in collected evidence."""
    if not client.available:
        return None
    try:
        result = client.complete_json(
            RATIONALE_SYSTEM, _evidence_blob(store, row), role="fast", max_tokens=700
        )
    except LLMBudgetExhausted:
        raise
    if not result:
        return None
    rationale = str(result.get("rationale") or "").strip()
    return rationale[:1200] or None


def challenge(
    client: GroqClient, store: Store, row: sqlite3.Row
) -> dict[str, Any] | None:
    """SOP Module 61 false-positive test, run before a Tier A approval."""
    if not client.available:
        return None
    try:
        result = client.complete_json(
            CHALLENGE_SYSTEM, _evidence_blob(store, row), role="reasoning", max_tokens=800
        )
    except LLMBudgetExhausted:
        raise
    if not result:
        return None
    verdict = str(result.get("verdict") or "").strip().lower()
    if verdict not in ("approve", "hold", "exclude"):
        verdict = "hold"
    concerns = result.get("concerns") or []
    if isinstance(concerns, str):
        concerns = [concerns]
    return {
        "verdict": verdict,
        "concerns": [str(c)[:200] for c in concerns][:5],
        "evidence_needed": [str(e)[:200] for e in (result.get("evidence_needed") or [])][:5],
        "reasoning": str(result.get("reasoning") or "")[:300],
    }


# --------------------------------------------------------------------------
# Fresh-query generation (Apify discovery stage)
#
# A different kind of task from the three above: instead of judging
# already-crawled evidence, this asks the model for search STRATEGY —
# keyword expansion, one of SOP Module 59's seven sanctioned AI uses. The
# model is never asked for and must never produce company names; it produces
# query strings, which then go through the exact same discovery guards
# (junk-name, aggregator, exclusion ledger) and the full classify/qualify/
# score pipeline as any other source before anything is exportable.
# --------------------------------------------------------------------------

QUERY_GEN_SYSTEM = """You generate web-search query strings for a B2B account-discovery
pipeline (SOP Module 17 target segments; Module 60 keyword-expansion prompt).

Target segments (research these, nothing else):
- Managed technology providers: continuously manage other companies' IT, cloud,
  infrastructure, security or service-desk environments under recurring arrangements.
- Financial organisations: credit unions, community/regional banks, fintech
  infrastructure providers.
- Healthcare IT environments: healthcare providers or technology partners managing
  clinical, identity, data, imaging, cloud or administrative systems.
- Regulated mid-market IT: operationally complex private-sector organisations with
  meaningful technology dependencies.

Each query string must be something you would literally type into a search engine —
a Boolean/keyword phrase, NOT a question, NOT a request for a list, and NEVER a
specific company name. Vary service-model language, vertical, geography, and
reverse-engineering angle (hiring, partner directories, associations, acquisitions,
expansion) across the batch.

Reply as JSON:
{"queries": [{"query": "...", "segment": "one of the four segment names above"}]}"""

# Mirrors config/queries.yaml's own group themes, so generated queries stay
# on-target without being literal duplicates of the static library.
_THEME_ROTATION = [
    "hiring signals for operational roles (service delivery, NOC, incident, change)",
    "recent acquisition, merger or leadership-change activity",
    "partner directories and vendor ecosystem listings",
    "industry association or membership directories",
    "compliance-driven demand (SOC 2, HITRUST, CMMC)",
    "cloud or security modernization / expansion",
]

# Looks like a question or a request for a list/answer, not a search string.
_BAD_QUERY_PREFIX = re.compile(
    r"^\s*(list|give me|show me|what are|who are|name (some|a few)|provide|find me)\b",
    re.IGNORECASE,
)


def _build_user_prompt(cfg: Any, count: int, recent_queries: list[str]) -> str:
    """The actual freshness mechanism: GroqClient caches on sha256(model+
    system+user), so an unvarying prompt would return the same cached query
    list forever. Vary the date, theme and state sample every call."""
    today = datetime.now().strftime("%Y-%m-%d")
    theme = random.choice(_THEME_ROTATION)
    states = cfg.all_states
    sample_size = min(5, len(states))
    states_sample = [s["name"] for s in random.sample(states, k=sample_size)] if states else []

    lines = [
        f"Date: {today}",
        f"Generate {count} distinct queries.",
        f"Emphasise this angle for at least half of them: {theme}.",
    ]
    if states_sample:
        lines.append(f"Rotate across these states for geographic spread: {', '.join(states_sample)}.")
    if recent_queries:
        lines.append("Do NOT repeat or closely paraphrase these already-used queries:")
        lines += [f"- {q}" for q in recent_queries[:40]]
    return "\n".join(lines)


def generate_fresh_queries(
    client: GroqClient,
    cfg: Any,
    count: int,
    recent_queries: list[str] | None = None,
) -> list[str]:
    """LLM-generated search query strings, deliberately varied run to run.

    Never pads or fabricates: returns fewer than `count` if that's all that
    passed validation. An empty or malformed LLM response returns [].
    """
    if not client.available:
        return []

    user = _build_user_prompt(cfg, count, recent_queries or [])
    try:
        result = client.complete_json(QUERY_GEN_SYSTEM, user, role="fast", max_tokens=1200)
    except LLMBudgetExhausted:
        raise
    if not result or not isinstance(result.get("queries"), list):
        log.warning("generate_fresh_queries: malformed or empty LLM response")
        return []

    avoid_lower = {q.lower().strip() for q in (recent_queries or [])}
    seen: set[str] = set()
    out: list[str] = []
    for entry in result["queries"]:
        query = entry.get("query") if isinstance(entry, dict) else entry
        if not isinstance(query, str):
            continue
        query = query.strip()
        if not (8 <= len(query) <= 200):
            continue
        if _BAD_QUERY_PREFIX.match(query):
            continue  # looks like a question / list-request, not a search string
        low = query.lower()
        if low in seen or low in avoid_lower:
            continue
        seen.add(low)
        out.append(query)
        if len(out) >= count:
            break

    if len(out) < count:
        log.info("generate_fresh_queries: LLM returned %d/%d usable queries", len(out), count)
    return out
