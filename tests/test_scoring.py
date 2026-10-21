#!/usr/bin/env python3
"""Scoring, qualification and temporal-intent checks.

Run with: .venv/bin/python tests/test_scoring.py

Neither score_company's arithmetic nor qualify_company's minimum acceptance
rule (SOP Module 26) had any test coverage before this — and Change 1
(temporal intent: dated events vs. ongoing conditions) and Change 4
(mid-size measurability) both rewrote the scoring model these depend on.
"""

from __future__ import annotations

import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline import intent_signals as intent_lex
from pipeline import qualify, score
from pipeline.store import Store

PASS, FAIL = 0, 0


def check(label: str, condition: bool, detail: str = "") -> None:
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  PASS  {label}")
    else:
        FAIL += 1
        print(f"  FAIL  {label}  {detail}")


class FakeCfg:
    """Mirrors Config.get()'s dotted-path-key + default signature."""

    def __init__(self, overrides: dict | None = None) -> None:
        self._overrides = overrides or {}

    def get(self, key: str, default=None):
        return self._overrides.get(key, default)


DEFAULT_CFG = FakeCfg({
    "scoring.weights": {"ideal_company_fit": 40, "operational_need": 25,
                         "timing_signals": 20, "decision_maker_access": 15},
    "scoring.tiers": {"A": 75, "B": 55, "C": 35},
})


def _iso(days_ago: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days_ago)).isoformat(timespec="seconds")


def _fixture_company(store: Store, **field_overrides):
    base = {
        "company_name": "Fixture MSP", "website": "https://fixturemsp.com",
        "headquarters_state": "TX", "employee_range": "51-200",
        "status": "approved",
    }
    base.update(field_overrides)
    cid, _ = store.upsert_company(base, "run_fixture")
    return store.get_company(cid)


def test_event_condition_split() -> None:
    print("\nEvent/condition split (Change 1)")
    overlap = set(intent_lex.CONDITION_WEIGHTS) & set(intent_lex.INTENT_WEIGHTS)
    check("no signal is scored as both an event and a condition", not overlap, str(overlap))
    for moved in ("tool_fragmentation", "compliance_pursuit", "status_page",
                  "incident_history", "client_scale_claim", "vendor_partner",
                  "association_member", "operational_content"):
        check(
            f"{moved} is a condition, not a timing event",
            moved in intent_lex.CONDITION_WEIGHTS and moved not in intent_lex.INTENT_WEIGHTS,
        )


def test_recency_decay() -> None:
    print("\nRecency decay on real dates")
    fresh = intent_lex.recency_multiplier(_iso(30))
    two_years = intent_lex.recency_multiplier(_iso(730))
    check("a 1-month-old dated event outscores a 2-year-old one",
          fresh > two_years, f"fresh={fresh} old={two_years}")
    check("decay still floors at 0.25", two_years >= 0.25)

    # The core Change 1 fix: an undated event must not score as if it were
    # fresh just because it was detected today. Before this change, every
    # signal decayed from crawl time, making this comparison impossible to
    # distinguish — this is exactly the bug the audit found.
    dated_now = intent_lex.recency_multiplier(_iso(0))
    undated_now = intent_lex.recency_multiplier(None, detected_at=_iso(0))
    check("an undated event scores below an equally-fresh dated one",
          undated_now < dated_now, f"undated={undated_now} dated={dated_now}")
    check("undated-but-just-detected lands at the confidence penalty (0.5)",
          abs(undated_now - 0.5) < 0.01, f"got {undated_now}")


def test_score_intent_sums_multiple_dated_events() -> None:
    print("\nscore_intent: multiple dated events of the same type")

    class Row(dict):
        pass

    rows = [
        Row({"event_type": "acquisition", "event_date": _iso(10), "detected_at": _iso(10)}),
        Row({"event_type": "acquisition", "event_date": _iso(400), "detected_at": _iso(400)}),
    ]
    result = intent_lex.score_intent(rows, max_points=20.0)
    check("two acquisitions contribute two separately-decayed points",
          len(result["contributions"]) == 2, str(result["contributions"]))
    check("total reflects both events, not just the strongest one",
          result["raw"] > max(c[1] for c in result["contributions"]),
          str(result))


def test_score_company_arithmetic() -> None:
    print("\nscore_company arithmetic")
    with tempfile.TemporaryDirectory() as tmp:
        store = Store(Path(tmp) / "test.db")
        row = _fixture_company(store)
        cid = row["id"]

        store.add_signal(cid, "recurring", "managed_services", detail="x")
        store.add_signal(cid, "operational", "sla", detail="x")
        store.add_signal(cid, "operational", "noc", detail="x")
        store.add_signal(cid, "access", "exec_titles", detail="x")
        store.add_intent_event(cid, "acquisition", event_date=_iso(5), source_url="https://n/1")

        result = score.score_company(store.get_company(cid), store, DEFAULT_CFG)

        check("fit reflects the 51-200 size band (Module 25 strongest range)",
              result["score_fit"] > 0, str(result))
        check("need reflects the noc + sla operational bonuses",
              result["score_need"] >= 5.0, str(result["score_need"]))
        check("timing reflects the dated acquisition event",
              result["score_timing"] > 0, str(result["score_timing"]))
        # exec_titles is a website proxy for buyer access and is worth 4, not
        # the 6 it carried before decision-maker mapping existed: the proxy
        # weights were cut to leave room inside the SOP's 15-point cap for
        # confirmed named contacts (see score.py's access block).
        check("access reflects exec_titles",
              result["score_access"] >= 4.0, str(result["score_access"]))
        expected_total = round(
            result["score_fit"] + result["score_need"]
            + result["score_timing"] + result["score_access"], 1
        )
        check("total is the sum of the four components",
              abs(result["score_total"] - expected_total) < 0.05,
              f"total={result['score_total']} expected={expected_total}")

        store.close()


def test_score_tier_thresholds() -> None:
    print("\nTier thresholds")
    with tempfile.TemporaryDirectory() as tmp:
        store = Store(Path(tmp) / "test.db")
        row = _fixture_company(store)
        result = score.score_company(store.get_company(row["id"]), store, DEFAULT_CFG)
        check("a company with no evidence at all scores below Tier C (Exclude)",
              result["tier"] == "Exclude", str(result))
        store.close()


def test_condition_credit_does_not_inflate_unknown_headcount_uniformly() -> None:
    print("\nChange 4: maturity-scaled unknown-headcount credit")
    with tempfile.TemporaryDirectory() as tmp:
        store = Store(Path(tmp) / "test.db")

        thin = _fixture_company(store, website="https://thin.example",
                                 company_name="Thin Co", employee_range=None)
        rich = _fixture_company(store, website="https://rich.example",
                                 company_name="Rich Co", employee_range=None)
        for cat, sig in (("recurring", "managed_services"), ("operational", "sla"),
                          ("complexity", "multi_client"), ("access", "exec_titles")):
            store.add_signal(rich["id"], cat, sig, detail="x")

        thin_result = score.score_company(store.get_company(thin["id"]), store, DEFAULT_CFG)
        rich_result = score.score_company(store.get_company(rich["id"]), store, DEFAULT_CFG)
        check(
            "an unknown-headcount company with rich evidence scores higher fit "
            "than one with none — the old code gave both the same flat credit",
            rich_result["score_fit"] > thin_result["score_fit"],
            f"thin={thin_result['score_fit']} rich={rich_result['score_fit']}",
        )
        store.close()


def test_qualify_minimum_acceptance() -> None:
    print("\nqualify_company minimum acceptance rule (SOP Module 26)")
    with tempfile.TemporaryDirectory() as tmp:
        store = Store(Path(tmp) / "test.db")

        # Passes recurring + operational + access -> approved.
        good = _fixture_company(store, website="https://good.example", company_name="Good Co")
        store.add_signal(good["id"], "recurring", "managed_services", detail="x")
        store.add_signal(good["id"], "operational", "sla", detail="x")
        store.add_signal(good["id"], "access", "exec_titles", detail="x")
        store.update_company(good["id"], website_reachable=1)
        status, reason, _ = qualify.qualify_company(
            store.get_company(good["id"]), store, DEFAULT_CFG, "run1"
        )
        check("passes all three minimums -> approved", status == "approved", f"{status}: {reason}")

        # No recurring evidence at all -> Module 26: hold for review, do not
        # force an uncertain record into approved.
        thin = _fixture_company(store, website="https://thin.example", company_name="Thin Co")
        store.update_company(thin["id"], website_reachable=1)
        status2, reason2, _ = qualify.qualify_company(
            store.get_company(thin["id"]), store, DEFAULT_CFG, "run1"
        )
        check("no recurring evidence -> review, not approved or excluded",
              status2 == "review", f"{status2}: {reason2}")

        # An explicit exclusion signal must exclude outright, not just review.
        staffing = _fixture_company(store, website="https://staffing.example",
                                     company_name="Staffing Co")
        store.add_signal(staffing["id"], "exclusion", "staffing_or_recruiting", detail="x")
        store.update_company(staffing["id"], website_reachable=1)
        status3, reason3, _ = qualify.qualify_company(
            store.get_company(staffing["id"]), store, DEFAULT_CFG, "run1"
        )
        check("exclusion signal -> excluded, not review", status3 == "excluded", f"{status3}: {reason3}")

        # An unreachable site is held for review, never permanently excluded
        # (README: "Unreachable != excluded").
        down = _fixture_company(store, website="https://down.example", company_name="Down Co")
        store.update_company(down["id"], website_reachable=0)
        status4, reason4, _ = qualify.qualify_company(
            store.get_company(down["id"]), store, DEFAULT_CFG, "run1"
        )
        check("unreachable website -> review, not a permanent exclusion",
              status4 == "review", f"{status4}: {reason4}")

        store.close()


if __name__ == "__main__":
    print("=" * 62)
    print("SCORING / QUALIFY / TEMPORAL INTENT TESTS")
    print("=" * 62)
    test_event_condition_split()
    test_recency_decay()
    test_score_intent_sums_multiple_dated_events()
    test_score_company_arithmetic()
    test_score_tier_thresholds()
    test_condition_credit_does_not_inflate_unknown_headcount_uniformly()
    test_qualify_minimum_acceptance()
    print("\n" + "=" * 62)
    print(f"RESULT: {PASS} passed, {FAIL} failed")
    print("=" * 62)
    sys.exit(1 if FAIL else 0)
