"""Intent and timing signals (SOP Modules 54, 55).

Two evidence routes, cheapest first:

  1. Careers page — already crawled during enrichment. Hiring for a service
     delivery, NOC, incident, change or cloud-ops role is a direct signal that
     the company is scaling operational responsibility (Module 54).

  2. News search — a Tavily lookup per company for acquisition, expansion,
     migration and leadership-change events (Module 55). Credit-expensive, so
     it is gated on a minimum base score and a per-run cap.

Module 54's timing rule is enforced: a generic vacancy is not enough. Only
roles from the operational-role list count.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Any

from . import signals
from .discovery.tavily import TavilyBudgetExhausted, TavilyProvider
from .store import Store

log = logging.getLogger("pipeline.intent")


def _parse_date(value: str | None) -> str | None:
    """Best-effort parse of a source-provided date into ISO 8601, or None.

    News APIs mix ISO ("2026-03-05") and RFC 2822 ("Thu, 05 Mar 2026 ...")
    formats. A date we cannot parse is treated the same as no date at all —
    score_intent applies its undated-confidence penalty either way.
    """
    if not value or not value.strip():
        return None
    value = value.strip()
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).isoformat(timespec="seconds")
    except ValueError:
        pass
    try:
        return parsedate_to_datetime(value).isoformat(timespec="seconds")
    except (TypeError, ValueError):
        return None


def careers_signals(
    store: Store, company_id: int, careers_text: str, careers_url: str | None
) -> list[str]:
    """Record hiring intent found on the company's own careers page.

    This is a coarse text scan with no per-posting date — call it only as a
    fallback when the structured ATS route (run.py:_ats_intent) finds
    nothing, since a real posting date always beats a page-text guess.
    """
    if not careers_text:
        return []

    roles, context = signals.detect_hiring(careers_text)
    found: list[str] = []

    if roles:
        # Name must match intent_signals.INTENT_WEIGHTS ("hiring_operational",
        # the 9.0-point flagship signal) or score_intent silently drops it.
        store.add_intent_event(
            company_id,
            "hiring_operational",
            detail="Hiring: " + ", ".join(sorted(set(roles))[:6]),
            source_url=careers_url,
            source_type="Careers page",
            evidence_level=1,
        )
        store.add_evidence(
            company_id,
            "Careers page",
            careers_url,
            f"Currently hiring operational roles: {', '.join(sorted(set(roles))[:6])}",
            evidence_level=1,
        )
        found.append("hiring_operational")

    if context and roles:
        store.add_intent_event(
            company_id,
            "hiring_context",
            detail="Job copy mentions: " + ", ".join(sorted(set(context))[:6]),
            source_url=careers_url,
            source_type="Careers page",
            evidence_level=1,
        )
        found.append("hiring_context")

    return found


def news_signals(
    store: Store,
    tavily: TavilyProvider,
    row: sqlite3.Row,
    lookback_days: int,
) -> list[str]:
    """Search recent news for the business-change events in Module 55."""
    company_id = row["id"]
    name = row["company_name"]
    if not name:
        return []

    try:
        results = tavily.news_for_company(name, days=lookback_days)
    except TavilyBudgetExhausted:
        raise
    except Exception as exc:  # noqa: BLE001
        log.debug("news search failed for %s: %s", name, exc)
        return []

    found: list[str] = []
    domain_token = (row["domain_key"] or "").split(".")[0].lower()
    name_token = name.lower()[:20]

    for res in results:
        title = res.get("title") or ""
        content = res.get("content") or ""
        url = res.get("url") or ""
        blob = f"{title} {content}"

        # The article must actually be about this company, not a namesake.
        low = blob.lower()
        if name_token not in low and (not domain_token or domain_token not in low):
            continue

        # Tavily's news topic usually carries a publish date; when it does,
        # this is the one route in the whole pipeline with a genuinely known
        # event_date rather than a crawl-time guess.
        event_date = _parse_date(res.get("published_date") or res.get("publishedDate"))

        for event, phrases in signals.detect_timing(blob).items():
            store.add_intent_event(
                company_id,
                event,
                detail=f"{title[:200]} (matched: {', '.join(phrases[:3])})",
                source_url=url,
                source_type="News article",
                evidence_level=2,
                event_date=event_date,
            )
            store.add_evidence(
                company_id,
                "News article",
                url,
                f"{event.replace('_', ' ').title()} signal: {title[:200]}",
                evidence_level=2,
            )
            if event not in found:
                found.append(event)

    return found


def summarise_timing(store: Store, company_id: int) -> str:
    """One-line timing summary for the tracker, newest event first."""
    rows = store.conn.execute(
        "SELECT event_type, detail, event_date, detected_at FROM intent_events"
        " WHERE company_id=? ORDER BY COALESCE(event_date, detected_at) DESC",
        (company_id,),
    ).fetchall()
    if not rows:
        return ""

    labels = {
        "hiring_operational": "Hiring operational roles",
        "operational_hiring": "Hiring operational roles",
        "hiring_context": "Job copy shows 24/7 / SLA / escalation duties",
        "hiring_volume": "Hiring several operations roles at once",
        "recent_award": "Recent industry award or ranking",
        "acquisition": "Acquisition or merger activity",
        "expansion": "Geographic or service expansion",
        "new_service": "New service line launched",
        "transformation": "Cloud migration / modernisation programme",
        "leadership_change": "New technology or operations leader",
        "growth": "Rapid growth or investment event",
    }
    parts: list[str] = []
    for row in rows:
        label = labels.get(row["event_type"], row["event_type"])
        detail = (row["detail"] or "").strip()
        parts.append(f"{label} — {detail[:160]}" if detail else label)
    return " | ".join(parts)[:900]
