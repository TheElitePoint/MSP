"""End-of-run quality control (SOP Modules 62, 65, 69).

Checks that run before anything is exported:

  completeness   required tracker fields populated per record
  duplicates     residual near-duplicate names inside the same state
  links          sample of website URLs actually resolve
  evidence       every approved account has at least one Level 1 source
  consistency    score and tier agree; approved accounts have a fit reason

The run report is written to the Excel workbook and to the log so a weak run
is visible rather than silently shipped.
"""

from __future__ import annotations

import logging
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import requests

from . import dedup, score as scoring
from .store import Store

log = logging.getLogger("pipeline.quality")


def run_quality_checks(store: Store, cfg: Any, run_id: str) -> dict[str, Any]:
    report: dict[str, Any] = {"run_id": run_id, "issues": [], "passed": True}

    rows = store.companies_where("status IN ('approved','review')")
    report["records_checked"] = len(rows)

    if not rows:
        report["issues"].append("No approved or review records produced by this run")
        report["passed"] = False
        return report

    # -- completeness ------------------------------------------------------
    completeness_values = []
    for row in rows:
        pct = scoring.completeness(row)
        completeness_values.append(pct)
        if pct != row["completeness_pct"]:
            store.update_company(row["id"], completeness_pct=pct)

    avg_completeness = round(sum(completeness_values) / len(completeness_values), 1)
    report["avg_completeness_pct"] = avg_completeness
    min_completeness = float(cfg.get("quality.min_completeness_pct", 70))
    if avg_completeness < min_completeness:
        report["issues"].append(
            f"Average completeness {avg_completeness}% is below the {min_completeness}% standard"
        )
        report["passed"] = False

    # -- residual duplicates ----------------------------------------------
    threshold = float(cfg.get("quality.fuzzy_name_match_threshold", 92))
    by_state: dict[str, list[Any]] = defaultdict(list)
    for row in rows:
        by_state[row["headquarters_state"] or "??"].append(row)

    dup_pairs: list[tuple[str, str, str]] = []
    for state, group in by_state.items():
        for i, a in enumerate(group):
            for b in group[i + 1:]:
                if a["domain_key"] and b["domain_key"] and a["domain_key"] == b["domain_key"]:
                    dup_pairs.append((state, a["company_name"], b["company_name"]))
                elif dedup.is_near_duplicate(a["name_key"], b["name_key"], threshold):
                    dup_pairs.append((state, a["company_name"], b["company_name"]))

    dup_rate = round(100.0 * len(dup_pairs) / len(rows), 2)
    report["duplicate_pairs"] = len(dup_pairs)
    report["duplicate_rate_pct"] = dup_rate
    report["duplicate_examples"] = dup_pairs[:20]
    max_dup = float(cfg.get("quality.max_duplicate_rate_pct", 2.0))
    if dup_rate > max_dup:
        report["issues"].append(
            f"Duplicate rate {dup_rate}% exceeds the {max_dup}% limit ({len(dup_pairs)} pairs)"
        )
        report["passed"] = False
    for state, a, b in dup_pairs[:50]:
        store.add_learning(run_id, "duplicate", f"Possible duplicate in {state}", f"{a} <-> {b}")

    # -- evidence trail ----------------------------------------------------
    approved = [r for r in rows if r["status"] == "approved"]
    missing_evidence = []
    missing_rationale = []
    for row in approved:
        evidence = store.get_evidence(row["id"])
        if not any(e["evidence_level"] == 1 for e in evidence):
            missing_evidence.append(row["company_name"])
        if not (row["fit_reason"] or "").strip():
            missing_rationale.append(row["company_name"])

    report["approved_count"] = len(approved)
    report["approved_missing_level1_evidence"] = len(missing_evidence)
    report["approved_missing_rationale"] = len(missing_rationale)
    if missing_evidence:
        report["issues"].append(
            f"{len(missing_evidence)} approved accounts lack Level 1 (direct) evidence"
        )
    if missing_rationale:
        report["issues"].append(
            f"{len(missing_rationale)} approved accounts have no fit rationale"
        )
        report["passed"] = False

    # -- score/tier consistency -------------------------------------------
    tiers = cfg.get("scoring.tiers", {}) or {}
    inconsistent = 0
    for row in rows:
        total = row["score_total"] or 0
        expected = (
            "A" if total >= float(tiers.get("A", 75))
            else "B" if total >= float(tiers.get("B", 55))
            else "C" if total >= float(tiers.get("C", 35))
            else "Exclude"
        )
        if row["tier"] != expected:
            inconsistent += 1
    report["score_tier_mismatches"] = inconsistent
    if inconsistent:
        report["issues"].append(f"{inconsistent} records have a tier that disagrees with the score")

    # -- link verification -------------------------------------------------
    if cfg.get("quality.verify_links", True):
        sample_size = int(cfg.get("quality.link_check_sample", 250))
        # Prioritise the accounts that will actually be worked.
        ordered = sorted(rows, key=lambda r: -(r["score_total"] or 0))[:sample_size]
        broken = _check_links(
            [(r["id"], r["website"]) for r in ordered if r["website"]],
            user_agent=cfg.get("enrich.user_agent", "ManageITServicesResearchBot/1.0"),
        )
        report["links_checked"] = len(ordered)
        report["links_broken"] = len(broken)
        for cid in broken:
            store.update_company(
                cid,
                website_reachable=0,
                review_notes="Website did not resolve during final link check",
            )
        if broken:
            report["issues"].append(f"{len(broken)} websites failed the final link check")

    # -- distribution ------------------------------------------------------
    report["tier_distribution"] = dict(Counter(r["tier"] for r in rows))
    report["segment_distribution"] = dict(Counter(r["segment"] for r in rows))
    report["state_distribution"] = dict(
        Counter(r["headquarters_state"] for r in rows).most_common(15)
    )
    report["counts"] = store.counts()

    if report["passed"]:
        log.info("Quality checks passed (%d records, %.1f%% complete)", len(rows), avg_completeness)
    else:
        log.warning("Quality checks flagged issues: %s", "; ".join(report["issues"]))

    return report


def _check_links(
    items: list[tuple[int, str]],
    workers: int = 10,
    user_agent: str = "ManageITServicesResearchBot/1.0",
) -> list[int]:
    """HEAD/GET each website; return the ids that failed."""
    broken: list[int] = []
    if not items:
        return broken

    headers = {"User-Agent": user_agent}

    def check(pair: tuple[int, str]) -> tuple[int, bool]:
        cid, url = pair
        try:
            resp = requests.head(url, timeout=12, allow_redirects=True, headers=headers)
            if resp.status_code >= 400:
                resp = requests.get(url, timeout=15, allow_redirects=True, headers=headers)
            return cid, resp.status_code < 400
        except requests.RequestException:
            return cid, False

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(check, item) for item in items]
        for future in as_completed(futures):
            try:
                cid, ok = future.result()
            except Exception:  # noqa: BLE001
                continue
            if not ok:
                broken.append(cid)
    return broken
