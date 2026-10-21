#!/usr/bin/env python3
"""Decision-maker mapping checks (SOP Modules 40-44).

Run with: .venv/bin/python tests/test_contacts.py

Offline and deterministic — no Apify calls, no Groq calls, no live database.
Every paid path is mocked, so this suite can run before a token exists.

The assertions here are weighted towards the near-misses that a naive
implementation gets wrong: "Ontario, California" vs "Ontario, Canada",
"Associate Director" vs "Associate", "Presales" vs "Sales", and a batched
search returning a person from a similarly-named company.
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline import dedup, score as score_mod
from pipeline.config import Config
from pipeline.contacts import (SalesNavBudgetExhausted, SalesNavProvider,
                               attribute_people, band_for, build_search_input,
                               build_us_index, classify_location, classify_role,
                               plan_batches, rank_contacts, rank_score,
                               reject_reason)
from pipeline.contacts.mapping import Batch
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
    """Config stand-in with just the keys the contacts code reads."""

    def __init__(self, overrides: dict | None = None, bands: dict | None = None,
                 states: list | None = None) -> None:
        self._overrides = overrides or {}
        self.dm_bands = bands if bands is not None else {
            "51-200": {
                "title_keywords": ["CEO", "CTO", "VP Service Delivery"],
                "seniority_levels": ["CXO", "Vice President"],
                "functions": ["Operations", "Information Technology"],
            },
            "unknown": {
                "title_keywords": ["Founder", "CEO", "CTO"],
                "seniority_levels": ["Owner/Partner", "CXO"],
                "functions": ["Operations"],
            },
        }
        self.dm_exclusions = {"title_keywords": ["Recruiter"], "functions": ["Sales"]}
        self.dm_headcount_filter = {"enabled": False, "map": {}}
        self.all_states = states if states is not None else [
            {"code": "TX", "name": "Texas", "cities": ["Dallas", "Austin", "Fort Worth"]},
            {"code": "CA", "name": "California", "cities": ["Los Angeles", "Ontario"]},
            {"code": "NY", "name": "New York", "cities": ["New York", "Buffalo"]},
        ]

    def get(self, key: str, default=None):
        return self._overrides.get(key, default)


class FakeResponse:
    def __init__(self, status_code: int, json_data=None, text: str = ""):
        self.status_code = status_code
        self._json = json_data if json_data is not None else {}
        self.text = text

    def json(self):
        return self._json

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.payloads = []

    def post(self, url, json=None, headers=None, timeout=None):  # noqa: A002
        self.payloads.append(json)
        if self.responses:
            return self.responses.pop(0)
        return FakeResponse(200, {"data": [], "status": "success"})


def _row(cid, name, **kw):
    base = {
        "id": cid, "company_name": name, "employee_range": None,
        "employee_estimate": None, "linkedin_company_id": None, "tier": "A",
    }
    base.update(kw)
    return base


# -- 1. role classification (SOP Module 41) ---------------------------------


def test_classify_role():
    print("\ntest_classify_role")
    cases = {
        # Module 41 warns the CIO is not always the best first person and
        # Module 43 says not to select on seniority alone, so the ordering
        # matters: an operations owner must not be swept into a C-suite bucket.
        "Director of Managed Services": "operational",
        "VP Service Delivery": "operational",
        "Director of IT Operations": "operational",
        "Head of Operational Resilience": "risk",
        "CISO": "risk",
        # Module 41 names these exact titles as champions.
        "NOC Manager": "champion",
        "Infrastructure Manager": "champion",
        "Change Manager": "champion",
        # Documented choice: CIO is listed under both economic and technical in
        # Module 41; at a managed provider it is the evaluator role.
        "CIO": "technical",
        "Chief Technology Officer": "technical",
        "Enterprise Architect": "technical",
        "CEO": "economic",
        "Founder & CEO": "economic",
        "President": "economic",
        "Managing Partner": "economic",
    }
    for title, expected in cases.items():
        got = classify_role(title)
        check(f"{title!r} -> {expected}", got == expected, f"got {got!r}")
    check("unmapped title returns None", classify_role("Graphic Designer") is None)
    check("empty title returns None", classify_role("") is None)


# -- 2. Module 44 people-to-avoid -------------------------------------------


def test_reject_reason():
    print("\ntest_reject_reason (Module 44 near-misses)")
    keep = [
        # The (?! director) lookahead exists for exactly this title.
        "Associate Director of IT Operations",
        # \bsales\b must not swallow "Presales".
        "Presales Solutions Engineer",
        # The partnerships pattern must not swallow "Managing Partner".
        "Managing Partner",
        "VP Service Delivery",
        "Chief Technology Officer",
    ]
    for title in keep:
        got = reject_reason(title)
        check(f"keeps {title!r}", got is None, f"rejected as {got!r}")

    drop = {
        "Technical Recruiter": "recruiter",
        "Talent Acquisition Lead": "recruiter",
        "Senior Business Development Manager": "vendor_or_bd",
        "Account Executive": "vendor_or_bd",
        "Partnerships Manager": "vendor_or_bd",
        "IT Intern": "junior",
        "Former CTO": "former_or_unclear",
        "Marketing Manager": "non_operational",
    }
    for title, expected in drop.items():
        got = reject_reason(title)
        check(f"rejects {title!r} as {expected}", got == expected, f"got {got!r}")

    # Soft reject: an in-house principal consultant at an MSP is an employee,
    # not the outside adviser Module 44 warns about.
    check("consultant WITH an ops anchor is not hard-rejected",
          reject_reason("Principal Consultant, Managed Services") is None)
    check("consultant WITHOUT an ops anchor is rejected",
          reject_reason("Independent Consultant") == "consultant")
    check("missing title is rejected", reject_reason(None) == "no_title")


# -- 3. US-only location validation -----------------------------------------


def test_classify_location():
    print("\ntest_classify_location (country-last anchoring)")
    cfg = FakeCfg()
    index = build_us_index(cfg)
    cases = [
        ("Dallas, Texas, United States", True, "TX"),
        # The pair that breaks a naive substring scan.
        ("Ontario, California, United States", True, "CA"),
        ("Ontario, Canada", False, None),
        ("Greater London, England, United Kingdom", False, None),
        ("Bengaluru, Karnataka, India", False, None),
        ("Toronto, Ontario, Canada", False, None),
        ("Austin, TX", True, "TX"),
        ("Dallas-Fort Worth Metroplex", True, "TX"),
        ("United States", True, None),
        ("", None, None),
        ("Remote", None, None),
    ]
    for text, want_us, want_state in cases:
        got = classify_location(text, index)
        ok = got["is_us"] is want_us and got["state"] == want_state
        check(f"{text!r} -> is_us={want_us} state={want_state}", ok, f"got {got}")


# -- 4/5. targeting ----------------------------------------------------------


def test_band_and_search_input():
    print("\ntest_band_for / build_search_input")
    cfg = FakeCfg({"contacts.geo_codes": [103644278], "contacts.require_geo_codes": True})

    check("stated employee_range wins",
          band_for(_row(1, "A", employee_range="51-200"), cfg) == "51-200")
    # Proves the pipeline.signals.employee_range_for reuse rather than a
    # second, drifting copy of the SOP Module 25 bands.
    check("falls back to employee_estimate -> 51-200",
          band_for(_row(1, "A", employee_estimate=120), cfg) == "51-200")
    check("no size data -> unknown band",
          band_for(_row(1, "A"), cfg) == "unknown")
    check("a band absent from config falls back to unknown",
          band_for(_row(1, "A", employee_range="10001+"), cfg) == "unknown")

    payload = build_search_input(company_names=["Acme IT"], band="51-200", cfg=cfg, limit=60)
    check("sends company names", payload["current_company_names"] == ["Acme IT"])
    check("carries the band's titles", "CEO" in payload["title_keywords"])
    check("carries geo_codes", payload["geo_codes"] == [103644278])
    check("carries the limit", payload["limit"] == 60)
    check("Module 44 exclusions pushed actor-side",
          payload.get("title_keywords_exclude") == ["Recruiter"])
    # The schema says a Sales Navigator URL overrides every other filter, which
    # would silently discard the geo and title targeting.
    check("never sets `url`", "url" not in payload)

    payload_ids = build_search_input(company_ids=[123, 456], band="51-200", cfg=cfg, limit=10)
    check("prefers exact LinkedIn org ids when available",
          payload_ids["current_company_ids"] == [123, 456]
          and "current_company_names" not in payload_ids)

    wide = FakeCfg(
        {"contacts.geo_codes": [1], "contacts.require_geo_codes": True},
        bands={"unknown": {"title_keywords": [f"T{i}" for i in range(30)],
                           "seniority_levels": ["CXO", "Not A Real Level"],
                           "functions": ["Operations", "Nonsense"]}},
    )
    trimmed = build_search_input(company_names=["X"], band="unknown", cfg=wide, limit=5)
    check("truncates title_keywords to the actor maximum of 20",
          len(trimmed["title_keywords"]) == 20)
    check("drops invalid enum values", trimmed["seniority_levels"] == ["CXO"]
          and trimmed["functions"] == ["Operations"])

    strict = FakeCfg({"contacts.geo_codes": [], "contacts.require_geo_codes": True})
    try:
        build_search_input(company_names=["X"], band="unknown", cfg=strict, limit=5)
        check("refuses an unfiltered worldwide search", False, "no error raised")
    except ValueError:
        check("refuses an unfiltered worldwide search", True)


# -- 6/7. batching and attribution ------------------------------------------


def test_batching_and_attribution():
    print("\ntest_plan_batches / attribute_people")
    cfg = FakeCfg({"contacts.batch_size": 10, "contacts.company_match_min_ratio": 93,
                   "contacts.company_match_min_margin": 8})

    rows = [_row(1, "Acme IT", employee_range="51-200"),
            _row(2, "Acme I.T. LLC", employee_range="51-200"),
            _row(3, "Zenith Systems", employee_range="51-200"),
            _row(4, "Northwind Managed")]
    batches = plan_batches(rows, cfg)
    # Two names that normalise to the same key must not compete inside one
    # search, because attribution could not tell their people apart.
    together = any({"Acme IT", "Acme I.T. LLC"} <= set(b.names) for b in batches)
    check("near-identical names never share a batch", not together)
    check("bands are never mixed inside a batch",
          all(len({r.get("employee_range") or "unknown" for r in b.rows}) == 1 for b in batches))
    check("batches respect the size limit", all(len(b.rows) <= 10 for b in batches))

    def person(company, company_id=None):
        return {"full_name": "Jane Doe", "job_title": "CTO", "company": company,
                "company_id": company_id, "linkedin_url": "https://linkedin.com/in/jane",
                "profile_id": "p1", "location": "Austin, Texas, United States"}

    batch = Batch(band="51-200", rows=[_row(1, "Acme IT"), _row(2, "Acme Technologies")])
    att, un = attribute_people([person("Acme IT Solutions")], batch, cfg)
    check("ambiguous company name is left unattributed, not guessed",
          att == {} and len(un) == 1)

    att, un = attribute_people([person("acme it")], batch, cfg)
    check("exact match survives case and spacing differences",
          list(att) == [1] and att[1][0]["match_method"] == "exact_name")

    att, un = attribute_people([person("Acme IT, Inc.")], batch, cfg)
    check("exact match survives a legal suffix", list(att) == [1])

    id_batch = Batch(band="51-200", rows=[_row(1, "Acme IT", linkedin_company_id="999"),
                                          _row(2, "Zenith Systems")])
    att, un = attribute_people([person("Totally Different Name", company_id="999")],
                               id_batch, cfg)
    check("LinkedIn org id beats a mismatched name",
          list(att) == [1] and att[1][0]["match_method"] == "li_company_id")


# -- 8/9. provider budget and token handling --------------------------------


def test_provider_budget_and_tokens():
    print("\ntest_provider budget / tokens")
    with tempfile.TemporaryDirectory() as td:
        state = Path(td) / "s.db"
        cfg = FakeCfg({"contacts": {"max_searches_per_run": 1, "max_spend_usd_per_run": 100.0,
                                    "sleep_between_calls": 0}})
        session = FakeSession([FakeResponse(200, {"data": [{"full_name": "A"}, {"full_name": "B"}],
                                                  "status": "success"})])
        provider = SalesNavProvider(cfg, ["apify_api_one"], session, state_path=state)
        provider.search({"limit": 25})
        check("charges $0.50 init + $0.01 per result", provider.spend_usd == 0.52,
              f"got {provider.spend_usd}")
        posts_before = len(session.payloads)
        try:
            provider.search({"limit": 25})
            check("search cap raises", False, "no error")
        except SalesNavBudgetExhausted:
            check("search cap raises before issuing another POST",
                  len(session.payloads) == posts_before)
        provider.close()

        # The cap that actually matters: a single call can cost $25, so the
        # dollar check has to happen before the money is spent.
        cfg2 = FakeCfg({"contacts": {"max_searches_per_run": 99, "max_spend_usd_per_run": 1.0,
                                     "sleep_between_calls": 0}})
        session2 = FakeSession([])
        provider2 = SalesNavProvider(cfg2, ["apify_api_one"], session2, state_path=state)
        try:
            provider2.search({"limit": 100})
            check("dollar cap raises", False, "no error")
        except SalesNavBudgetExhausted:
            check("dollar cap raises with zero POSTs issued", len(session2.payloads) == 0)
        provider2.close()

        cfg3 = FakeCfg({"contacts": {"max_searches_per_run": 9, "max_spend_usd_per_run": 99.0,
                                     "sleep_between_calls": 0, "dead_key_retry_hours": 24}})
        session3 = FakeSession([FakeResponse(401), FakeResponse(200, {"data": [], "status": "success"})])
        provider3 = SalesNavProvider(cfg3, ["apify_api_dead", "apify_api_good"],
                                     session3, state_path=state)
        provider3.search({"limit": 5})
        check("401 retires the token and rotates", provider3.token_usage()["keys_active"] == 1)
        provider3.close()

        reloaded = SalesNavProvider(cfg3, ["apify_api_dead", "apify_api_good"],
                                    FakeSession([]), state_path=state)
        check("dead token is remembered across runs, not re-probed",
              reloaded.token_usage()["keys_active"] == 1)
        check("the dead token stays in the pool for a later retry",
              reloaded.token_usage()["keys_total"] == 2)
        reloaded.close()


def test_async_request_id_flow():
    print("\ntest_async request_id flow")
    with tempfile.TemporaryDirectory() as td:
        cfg = FakeCfg({"contacts": {"max_searches_per_run": 9, "max_spend_usd_per_run": 99.0,
                                    "sleep_between_calls": 0, "poll_sleep_seconds": 0,
                                    "poll_max_attempts": 3}})
        session = FakeSession([
            FakeResponse(200, {"data": [], "status": "pending", "request_id": "abc"}),
            FakeResponse(200, {"data": [{"full_name": "A"}], "status": "success"}),
        ])
        provider = SalesNavProvider(cfg, ["apify_api_one"], session,
                                    state_path=Path(td) / "s.db")
        result = provider.search({"limit": 10, "current_company_names": ["X"]})
        check("polls with request_id when the first reply is pending",
              session.payloads[1].get("request_id") == "abc")
        check("the poll does not resend the filters",
              "current_company_names" not in session.payloads[1])
        check("polling is not charged as a second search", provider.searches_made == 1)
        check("returns the eventual rows", len(result["data"]) == 1)
        provider.close()


# -- 10. store round-trip ----------------------------------------------------


def test_store_roundtrip():
    print("\ntest_store contacts round-trip")
    with tempfile.TemporaryDirectory() as td:
        store = Store(Path(td) / "t.db")
        cid, _ = store.upsert_company({"company_name": "Acme IT",
                                       "website": "https://acme.example"}, "run1")
        record = {"profile_key": "li:jane", "full_name": "Jane Doe", "job_title": "CTO",
                  "role_type": "technical", "status": "accepted"}
        first_id, created = store.upsert_contact(cid, record, "run1")
        second_id, created_again = store.upsert_contact(
            cid, dict(record, job_title="CIO"), "run2")
        check("same person upserts rather than duplicating",
              first_id == second_id and created and not created_again)

        row = store.get_contacts(cid)[0]
        check("mutable fields refresh", row["job_title"] == "CIO")
        check("first_seen_run is immutable", row["first_seen_run"] == "run1")
        check("last_seen_run advances", row["last_seen_run"] == "run2")

        other, _ = store.upsert_company({"company_name": "Zenith",
                                         "website": "https://z.example"}, "run1")
        store.upsert_contact(other, record, "run1")
        # A global unique on profile_key would swallow this silently; it is the
        # signature of a mis-batched search, so it must stay visible.
        check("the same person under two companies yields two rows",
              len(store.contacts_where("profile_key='li:jane'")) == 2)

        check("contact_counts groups by company", store.contact_counts("accepted")[cid] == 1)

        store.record_contact_search("k1", "{}", json.dumps({"data": []}), "success",
                                    0, 0.5, "run1", None)
        check("search cache round-trips", store.get_contact_search("k1")["cost_usd"] == 0.5)
        check("counts() reports contacts", store.counts()["contacts_accepted"] == 2)
        store.close()


# -- 11. buying-group balance (Module 41) ------------------------------------


def test_rank_contacts():
    print("\ntest_rank_contacts (Module 41 buying group)")
    titles = ["CEO", "President", "COO", "VP Service Delivery",
              "Director of IT Operations", "NOC Manager"]
    pool = [{"full_name": t, "job_title": t, "role_type": classify_role(t),
             "rank_score": rank_score(t, classify_role(t))} for t in titles]
    picked = rank_contacts(pool, 4)
    roles = {p["role_type"] for p in picked}
    check("returns the Module 31 depth target", len(picked) == 4)
    check("includes an economic sponsor", "economic" in roles)
    check("includes a technical or operational evaluator",
          bool(roles & {"technical", "operational"}))
    check("includes a champion", "champion" in roles)
    check("never exceeds the target", len(rank_contacts(pool, 2)) == 2)
    check("empty pool is safe", rank_contacts([], 4) == [])


# -- 12. access scoring ------------------------------------------------------


def test_access_scoring():
    print("\ntest_score_access with contact signals")
    cfg = Config()
    with tempfile.TemporaryDirectory() as td:
        store = Store(Path(td) / "t.db")

        def access_for(name, proxies, linkedin, extra):
            cid, _ = store.upsert_company(
                {"company_name": name, "website": f"https://{name}.example"}, "r1")
            if linkedin:
                store.update_company(cid, linkedin_url="https://linkedin.com/company/x")
            for signal in list(proxies) + list(extra):
                store.add_signal(cid, "access", signal)
            return score_mod.score_company(store.get_company(cid), store, cfg)["score_access"]

        weak = access_for("weak", [], True, [])
        weak_dm = access_for("weakdm", [], True, ["named_decision_maker"])
        weak_both = access_for("weakboth", [], True,
                               ["named_decision_maker", "economic_buyer_identified"])
        check("named decision-makers raise the access score", weak_dm > weak)
        check("an economic buyer raises it further", weak_both > weak_dm)

        everything = access_for(
            "max", ["leadership_page", "exec_titles", "contact_route"], True,
            ["named_decision_maker", "economic_buyer_identified"])
        check("never exceeds the SOP's 15-point cap", everything <= 15.0,
              f"got {everything}")
        store.close()


def test_profile_key():
    print("\ntest_dedup.profile_key")
    check("prefers the LinkedIn slug",
          dedup.profile_key("https://www.linkedin.com/in/jane-doe-123/?trk=x") == "li:jane-doe-123")
    check("falls back to profile id", dedup.profile_key(None, "ABC123") == "pid:abc123")
    check("falls back to name+title",
          dedup.profile_key(None, None, "Jane Doe", "CTO") == "nk:janedoe|cto")
    check("no identity at all returns None", dedup.profile_key(None, None, None, None) is None)
    check("a weak key can never collide with a strong one",
          dedup.profile_key(None, None, "Jane Doe", "CTO")
          != dedup.profile_key("https://linkedin.com/in/janedoe"))


if __name__ == "__main__":
    print("=" * 62)
    print("DECISION-MAKER MAPPING TESTS (SOP Modules 40-44)")
    print("=" * 62)
    test_classify_role()
    test_reject_reason()
    test_classify_location()
    test_band_and_search_input()
    test_batching_and_attribution()
    test_provider_budget_and_tokens()
    test_async_request_id_flow()
    test_store_roundtrip()
    test_rank_contacts()
    test_access_scoring()
    test_profile_key()
    print("\n" + "=" * 62)
    print(f"RESULT: {PASS} passed, {FAIL} failed")
    print("=" * 62)
    sys.exit(1 if FAIL else 0)
