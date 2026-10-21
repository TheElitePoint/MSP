"""Final cleaning and verification pass.

Runs after scoring and before export. Everything here is a *last line of
defence*: by this point each record has already been through discovery guards,
enrichment and qualification, so anything caught here is reported as a defect
class in the run report rather than silently dropped.

Checks, in order:

  1. entity type      only operators (and end-user orgs for the end-user
                      segments) may reach the tracker
  2. geography        non-US organisations are demoted
  3. name quality     descriptive or unresolved names are repaired or flagged
  4. self-reference   our own aggregator/vendor blocklist, re-applied
  5. field hygiene    whitespace, casing, URL form, placeholder text
  6. verification     a per-record verdict with the reason recorded

Demotion, not deletion: a rejected record moves to `excluded` with a reason so
it is never re-researched, and it stays in the database as evidence of the
decision (SOP Module 28).
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timezone
from typing import Any

from . import classify, dedup, nameres
from .store import Store

log = logging.getLogger("pipeline.clean")

_WS = re.compile(r"\s+")

# SOP Module 23 defines "Regulated mid-market IT" as operationally complex
# PRIVATE-SECTOR organisations. classify.py's own domain hints give .gov/.edu
# a strong boost toward `end_user_org` (correctly, for classification), which
# means government agencies and universities are disproportionately the
# end_user_org records seen here — exactly what must NOT be re-segmented into
# mid-market. Government procurement also directly contradicts Module 10's
# 30-to-60-day pilot requirement.
_PUBLIC_SECTOR_MARKERS = re.compile(
    r"\b(city of|county of|state of|township of|village of|borough of|"
    r"department of|division of|office of|commission on|bureau of|"
    r"university|college|school district|public schools|"
    r"municipal|municipality)\b",
    re.IGNORECASE,
)


def _is_public_sector(name: str | None, domain_key: str | None) -> bool:
    if domain_key and any(domain_key.endswith(sfx) for sfx in (".gov", ".mil", ".edu")):
        return True
    return bool(_PUBLIC_SECTOR_MARKERS.search(name or ""))

# Defect classes reported in the run summary.
DEFECTS = [
    "wrong_entity_type",
    "large_enterprise",
    "unverified_entity",
    "llm_entity_conflict",
    "non_us",
    "unresolved_name",
    "descriptive_name",
    "aggregator_domain",
    "no_evidence",
]


def clean_and_verify(
    store: Store,
    cfg: Any,
    run_id: str,
    aggregators: set[str],
    enterprise_domains: set[str] | None = None,
) -> dict[str, Any]:
    rows = store.companies_where("status IN ('approved','review')")
    report: dict[str, Any] = {
        "checked": len(rows),
        "demoted": 0,
        "repaired": 0,
        "resegmented": 0,
        "defects": {d: 0 for d in DEFECTS},
        "examples": {d: [] for d in DEFECTS},
        "verified": 0,
    }

    enterprise_domains = enterprise_domains or set()
    drop_enterprise = bool(cfg.get("clean.exclude_large_enterprise", True))
    headcount_ceiling = int(cfg.get("qualify.headcount_max", 5000))
    strict_entity = bool(cfg.get("clean.enforce_entity_type", True))
    strict_geo = bool(cfg.get("clean.enforce_us_only", True))
    allow_unknown_entity = bool(cfg.get("clean.allow_unknown_entity", True))

    for row in rows:
        cid = row["id"]
        name = row["company_name"] or ""
        website = row["website"]
        demote_reason: str | None = None
        defect: str | None = None
        unverified_entity = False
        updates: dict[str, Any] = {}

        # -- 1. entity type ------------------------------------------------
        entity_type = row["entity_type"] or "unknown"
        segment = row["segment"]

        # SOP Module 23: an operationally complex end-user org is not a
        # defect — it is exactly the "Regulated mid-market IT" segment's
        # designated home. `ACCEPTABLE_TYPES` already allows end_user_org
        # for that segment (and for Healthcare/Financial); the only segment
        # that rejects it is "Managed technology provider", which is also
        # the segment most discovery queries carry (217 of 292 in a
        # representative run, vs 17 for mid-market). Without this, a
        # genuine mid-market account discovered via an MSP-labeled query is
        # thrown away as wrong_entity_type instead of re-homed to the
        # segment built for it. Genuine defects (software_vendor,
        # directory_media, association) still demote exactly as before.
        if (
            strict_entity
            and entity_type == "end_user_org"
            and segment == "Managed technology provider"
            and not _is_public_sector(name, row["domain_key"])
        ):
            sig_cats = {s["category"] for s in store.get_signals(cid)}
            if "complexity" in sig_cats or "operational" in sig_cats:
                segment = "Regulated mid-market IT"
                updates["segment"] = segment
                report["resegmented"] += 1

        if strict_entity and entity_type != "unknown":
            if not classify.is_acceptable(entity_type, segment):
                demote_reason = (
                    f"Not an account: classified as {entity_type.replace('_', ' ')} "
                    f"({row['entity_evidence'] or 'no evidence'})"
                )
                defect = "wrong_entity_type"
        elif strict_entity and entity_type == "unknown":
            if not allow_unknown_entity:
                demote_reason = "Entity type could not be determined from public content"
                defect = "wrong_entity_type"
            else:
                # Kept in the database for a human to look at, but NOT marked
                # clean-verified, so it cannot reach the workbook. An
                # unverified entity type is how Apollo Global Management (a
                # private equity firm) and ICF (a government consultancy)
                # reached a real export.
                unverified_entity = True

        # -- 1b. LLM second opinion (SOP Module 29: assistance, not proof) ---
        # The model never overrides the deterministic verdict on its own. Where
        # the two conflict and the model is confident, the record goes to a
        # human instead of being silently trusted either way.
        llm_type = row["llm_entity_type"]
        if (
            not demote_reason
            and llm_type
            and row["llm_agrees"] == 0
            and (row["llm_entity_conf"] or 0) >= 0.7
        ):
            llm_acceptable = classify.is_acceptable(llm_type, segment)
            det_acceptable = classify.is_acceptable(entity_type, segment)
            if llm_acceptable != det_acceptable:
                report["defects"]["llm_entity_conflict"] += 1
                _example(report, "llm_entity_conflict",
                         f"{name}: lexicon={entity_type} vs llm={llm_type}")
                if row["status"] == "approved":
                    updates["status"] = "review"
                    updates["status_reason"] = (
                        f"Entity type disputed: rules say {entity_type}, "
                        f"model says {llm_type} ({row['llm_entity_evidence'] or 'no quote'}) "
                        "— confirm before outreach"
                    )
                updates["review_notes"] = _append(
                    row["review_notes"], f"LLM disagrees on entity type ({llm_type})"
                )

        # -- 1c. LLM false-positive challenge (SOP Module 61) ---------------
        if not demote_reason and row["llm_challenge_verdict"] == "exclude":
            updates["review_notes"] = _append(
                row["review_notes"],
                f"False-positive challenge flagged: {(row['llm_challenge_notes'] or '')[:200]}",
            )
            if row["status"] == "approved" and row["tier"] == "A":
                updates["status"] = "review"
                updates["status_reason"] = (
                    "Tier A held by false-positive challenge — "
                    f"{(row['llm_challenge_notes'] or 'see LLM Concerns')[:200]}"
                )

        # -- 1d. size ceiling (SOP Modules 25 / 28) --------------------------
        if not demote_reason and drop_enterprise:
            too_big, why = classify.is_large_enterprise(
                (row["company_description"] or "") + " " + (row["services"] or ""),
                website, enterprise_domains, row["employee_estimate"], headcount_ceiling,
            )
            if too_big:
                demote_reason = why
                defect = "large_enterprise"

        # -- 2. geography ---------------------------------------------------
        if not demote_reason and strict_geo and row["is_us"] == 0:
            demote_reason = f"Not a US organisation ({row['geo_reason'] or 'non-US indicators'})"
            defect = "non_us"

        # -- 3. aggregator domain (re-applied; the blocklist grows over time)
        if not demote_reason and website and dedup.is_aggregator(website, aggregators):
            demote_reason = "Directory, job board or media site, not a company"
            defect = "aggregator_domain"

        # -- 4. name quality ------------------------------------------------
        if not demote_reason:
            if dedup.is_junk_name(name):
                repaired = nameres._from_domain(website)
                if repaired:
                    updates["company_name"] = repaired
                    updates["name_key"] = dedup.name_key(repaired)
                    updates["name_source"] = "domain (repaired in clean)"
                    report["repaired"] += 1
                else:
                    demote_reason = "Company name could not be resolved"
                    defect = "unresolved_name"
            elif nameres.is_descriptive(name, website):
                repaired = nameres._from_domain(website)
                if repaired and not nameres.is_descriptive(repaired, website):
                    updates["company_name"] = repaired
                    updates["name_key"] = dedup.name_key(repaired)
                    updates["name_source"] = "domain (repaired in clean)"
                    report["repaired"] += 1
                else:
                    # Keep it, but flag so a human fixes it before outreach.
                    report["defects"]["descriptive_name"] += 1
                    _example(report, "descriptive_name", f"{name} [{website}]")
                    updates["review_notes"] = _append(
                        row["review_notes"], "Name looks descriptive — verify the legal name"
                    )

        # -- 5. evidence ----------------------------------------------------
        if not demote_reason and row["status"] == "approved":
            evidence = store.get_evidence(cid)
            if not any(e["evidence_level"] == 1 for e in evidence):
                updates["status"] = "review"
                updates["status_reason"] = "No Level 1 (direct) evidence — verify on the website"
                report["defects"]["no_evidence"] += 1
                _example(report, "no_evidence", name)

        # -- 6. field hygiene ----------------------------------------------
        tidy_name = _tidy(updates.get("company_name", name))
        if tidy_name and tidy_name != name:
            updates["company_name"] = tidy_name
            updates["name_key"] = dedup.name_key(tidy_name)
        canonical = dedup.canonical_url(website)
        if canonical and canonical != website:
            updates["website"] = canonical
        for field in ("services", "customer_industries", "company_description"):
            value = row[field]
            if isinstance(value, str):
                tidied = _WS.sub(" ", value).strip()
                if tidied != value:
                    updates[field] = tidied

        # -- apply ----------------------------------------------------------
        if demote_reason:
            store.update_company(
                cid,
                status="excluded",
                status_reason=demote_reason,
                tier="Exclude",
                clean_verified=0,
                **updates,
            )
            store.add_exclusion(
                row["domain_key"], name, row["name_key"], demote_reason, run_id
            )
            report["demoted"] += 1
            if defect:
                report["defects"][defect] += 1
                _example(report, defect, f"{name} [{website}]")
            store.add_learning(
                run_id, "clean_demotion", demote_reason[:120], f"{name} [{website}]"
            )
        elif unverified_entity:
            updates["status"] = "review"
            updates["status_reason"] = (
                "Entity type unverified — confirm this is a managed-service "
                "operator before any outreach"
            )
            updates["review_notes"] = _append(
                row["review_notes"], "Entity type could not be determined"
            )
            updates["clean_verified"] = 0
            store.update_company(cid, **updates)
            report["defects"]["unverified_entity"] += 1
            _example(report, "unverified_entity", f"{name} [{website}]")
        else:
            # Stamped only here. The export reads this flag, so nothing that
            # has not passed the clean pass can reach the workbook.
            updates["clean_verified"] = 1
            updates["cleaned_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
            store.update_company(cid, **updates)
            report["verified"] += 1

    total_defects = sum(report["defects"].values())
    report["defect_rate_pct"] = (
        round(100.0 * total_defects / report["checked"], 2) if report["checked"] else 0.0
    )
    log.info(
        "CLEAN: %d checked, %d verified, %d demoted, %d names repaired, "
        "%d re-segmented to mid-market (defect rate %.1f%%)",
        report["checked"], report["verified"], report["demoted"],
        report["repaired"], report["resegmented"], report["defect_rate_pct"],
    )
    for defect, count in report["defects"].items():
        if count:
            log.info("  %-20s %d", defect, count)
    return report


def _tidy(name: str | None) -> str | None:
    if not name:
        return None
    tidy = _WS.sub(" ", name).strip(" -|·—–:,.")
    # ALL-CAPS names longer than an acronym read as shouting in the tracker.
    if len(tidy) > 5 and tidy.isupper():
        tidy = tidy.title()
    return tidy or None


def _append(existing: str | None, note: str) -> str:
    if not existing:
        return note
    if note in existing:
        return existing
    return f"{existing}; {note}"[:900]


def _example(report: dict[str, Any], defect: str, text: str) -> None:
    bucket = report["examples"].setdefault(defect, [])
    if len(bucket) < 10:
        bucket.append(text[:120])
