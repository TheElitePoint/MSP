#!/usr/bin/env python3
"""Dedup and exclusion-ledger checks — run with: .venv/bin/python tests/test_dedup.py

These assert the safeguard the whole project depends on: repeat runs must never
produce a second row for a company that is already in the database.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline import dedup
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


def test_keys() -> None:
    print("\nKey normalisation")
    check("www stripped", dedup.domain_key("https://www.acme.com/services") == "acme.com")
    check("subdomain collapses", dedup.domain_key("http://mail.acme.com") == "acme.com")
    check("bare domain works", dedup.domain_key("acme.com") == "acme.com")
    check("multi-part TLD", dedup.domain_key("https://acme.co.uk/x") == "acme.co.uk")
    check("junk rejected", dedup.domain_key("not a url") is None)

    check("LLC stripped", dedup.name_key("Acme Technologies, LLC") == "acmetechnology")
    check("Inc stripped", dedup.name_key("Acme Technologies Inc.") == "acmetechnology")
    check("plural collapses", dedup.name_key("Acme Solutions") == dedup.name_key("Acme Solution"))
    check("'-ss' preserved", dedup.name_key("Business Systems") == "businesssystem")
    check("'The' stripped", dedup.name_key("The Acme Group") == "acme")
    check("case insensitive", dedup.name_key("ACME TECH") == dedup.name_key("acme tech"))

    check("phone digits", dedup.phone_key("+1 (512) 555-0134") == "5125550134")
    check("short phone rejected", dedup.phone_key("555-0134") is None)

    check(
        "near-duplicate detected",
        dedup.is_near_duplicate(
            dedup.name_key("Acme Technology Group"),
            dedup.name_key("Acme Technologies Group"),
        ),
    )
    check(
        "different companies not merged",
        not dedup.is_near_duplicate(dedup.name_key("Acme IT"), dedup.name_key("Zenith IT")),
    )


def test_aggregators() -> None:
    print("\nAggregator guard")
    aggs = {"clutch.co", "linkedin.com", "yelp.com", "indeed.com"}
    check("directory blocked", dedup.is_aggregator("https://clutch.co/profile/acme", aggs))
    check("subdomain blocked", dedup.is_aggregator("https://jobs.indeed.com/x", aggs))
    check("real company allowed", not dedup.is_aggregator("https://acme-it.com", aggs))


def test_junk_names() -> None:
    print("\nJunk-name guard")
    for junk in ("Just a moment...", "Attention Required! ", "403 Forbidden",
                 "Page Not Found", "Home", "Under Construction", "", "  ", "404"):
        check(f"rejects {junk!r}", dedup.is_junk_name(junk))
    for real in ("Pegasus Technologies", "Sourcepass", "TeamLogic IT", "PCS"):
        check(f"accepts {real!r}", not dedup.is_junk_name(real))


def test_bot_challenge() -> None:
    from bs4 import BeautifulSoup

    from pipeline.enrich import is_bot_challenge

    print("\nBot-challenge detection")

    def judge(html: str) -> bool:
        soup = BeautifulSoup(html, "lxml")
        text = soup.get_text(" ", strip=True)
        return is_bot_challenge(html, soup, text)

    cloudflare = (
        "<html><head><title>Just a moment...</title></head><body>"
        "<div id='cf-browser-verification'>Checking your browser</div></body></html>"
    )
    check("Cloudflare interstitial detected", judge(cloudflare))

    challenge_body = (
        "<html><head><title>Acme IT</title></head><body>"
        "<p>Enable JavaScript and cookies to continue</p><p>Ray ID: 8a2f</p></body></html>"
    )
    check("challenge body marker detected", judge(challenge_body))

    real = (
        "<html><head><title>Acme Managed Technology | Managed IT Services</title></head>"
        "<body><h1>Managed IT Services</h1><p>" + ("We deliver 24/7 managed IT "
        "services with SLA-backed support to healthcare and financial clients. " * 6)
        + "</p></body></html>"
    )
    check("real homepage not flagged", not judge(real))

    thin = "<html><head><title>Home</title></head><body><p>Hello</p></body></html>"
    check("thin placeholder page flagged", judge(thin))


def test_store_dedup() -> None:
    print("\nStore-level dedup across runs")
    with tempfile.TemporaryDirectory() as tmp:
        store = Store(Path(tmp) / "test.db")

        base = {
            "company_name": "Acme Managed Technology LLC",
            "website": "https://www.acmemt.com",
            "phone": "(512) 555-0134",
            "headquarters_state": "TX",
            "headquarters_city": "Austin",
            "discovery_source": "OSM Overpass (TX)",
        }

        id1, created1 = store.upsert_company(dict(base), "run_1")
        check("first sighting creates a row", created1)

        # RUN 2: same company, different URL form and legal suffix.
        variant = dict(base)
        variant["company_name"] = "Acme Managed Technology, Inc."
        variant["website"] = "http://acmemt.com/services"
        variant["discovery_source"] = "Tavily search (TX)"
        id2, created2 = store.upsert_company(variant, "run_2")
        check("same domain does not create a duplicate", not created2)
        check("resolves to the same company id", id1 == id2, f"{id1} vs {id2}")

        # RUN 3: no website at all, matched by normalised name + state.
        by_name = {
            "company_name": "Acme Managed Technology",
            "headquarters_state": "TX",
            "phone": "512-555-0134",
            "discovery_source": "Directory",
        }
        id3, created3 = store.upsert_company(by_name, "run_3")
        check("name+state match does not duplicate", not created3)
        check("name match resolves to same id", id1 == id3, f"{id1} vs {id3}")

        # A genuinely different company must still be created.
        other = {
            "company_name": "Zenith Cloud Operations",
            "website": "https://zenithcloudops.com",
            "headquarters_state": "TX",
        }
        _, created4 = store.upsert_company(other, "run_4")
        check("distinct company still created", created4)

        total = store.counts()["total_companies"]
        check("exactly 2 companies after 4 sightings", total == 2, f"got {total}")

        row = store.get_company(id1)
        check("times_seen incremented", row["times_seen"] == 3, f"got {row['times_seen']}")
        check(
            "discovery sources accumulated",
            "Tavily" in (row["discovery_source"] or "") and "OSM" in (row["discovery_source"] or ""),
            row["discovery_source"],
        )

        # Merge must not overwrite good data with blanks.
        store.update_company(id1, employee_estimate=90, employee_range="51-200")
        store.upsert_company(dict(base), "run_5")
        row = store.get_company(id1)
        check("existing data preserved on merge", row["employee_estimate"] == 90)

        print("\nExclusion ledger")
        store.add_exclusion("badco.com", "BadCo Web Design", dedup.name_key("BadCo Web Design"),
                            "Weak-fit business model: web_or_marketing_agency", "run_1")
        check("exclusion recalled by domain",
              store.is_excluded("badco.com") is not None)
        check("exclusion recalled by name",
              store.is_excluded(None, dedup.name_key("BadCo Web Design, LLC")) is not None)
        check("unrelated company not excluded", store.is_excluded("goodco.com") is None)

        print("\nQuery cooldown")
        store.log_query("tavily", '"managed IT services" Austin TX', "TX:Austin", 12, 4, "run_1")
        check("recent query is on cooldown",
              store.query_recently_run("tavily", '"managed IT services" Austin TX', "TX:Austin", 14))
        check("different query is not on cooldown",
              not store.query_recently_run("tavily", '"co-managed IT" Austin TX', "TX:Austin", 14))

        print("\nCheckpoints")
        check("stage not done initially", not store.is_done("run_9", "discover", "TX"))
        store.mark_done("run_9", "discover", "TX")
        check("stage marked done", store.is_done("run_9", "discover", "TX"))

        store.close()


def test_signals() -> None:
    from pipeline import signals

    print("\nSignal detection")
    msp_text = (
        "We are a managed service provider delivering 24/7 network operations center "
        "support with SLA-backed response, proactive monitoring, incident response, "
        "business continuity and disaster recovery for healthcare and credit union "
        "clients across multiple client environments. Our team of 85 professionals "
        "supports hybrid cloud and Microsoft 365. Meet the team: our Chief Technology "
        "Officer and VP of Service Delivery."
    )
    detected = signals.detect_all(msp_text)
    check("recurring detected", "managed_services" in detected["recurring"])
    check("NOC detected", "noc" in detected["operational"])
    check("SLA detected", "sla" in detected["operational"])
    check("healthcare vertical", "healthcare" in detected["vertical"])
    check("financial vertical", "financial" in detected["vertical"])
    check("complexity detected", "multi_client" in detected["complexity"])
    check("exec titles detected", "exec_titles" in detected["access"])
    check("headcount extracted", signals.extract_employee_estimate(msp_text) == 85)
    check("band mapped", signals.employee_range_for(85) == "51-200")
    check(
        "customer employee count not treated as provider headcount",
        signals.extract_employee_estimate(
            "We provide managed IT support for growing companies with 10-250 employees."
        ) is None,
    )
    check(
        "case-study employee count not treated as provider headcount",
        signals.extract_employee_estimate(
            "Our client has more than 150 employees across 12 locations."
        ) is None,
    )
    check("no false exclusions on a real MSP", signals.detect_exclusions(msp_text) == {})

    agency = (
        "We are a web design and digital marketing agency offering SEO services, "
        "branding agency work, social media marketing and PPC management."
    )
    check("agency excluded", "web_or_marketing_agency" in signals.detect_exclusions(agency))

    hiring = "Now hiring: Service Delivery Manager and NOC Manager. On-call rotation, SLA ownership."
    roles, context = signals.detect_hiring(hiring)
    check("operational roles detected", "service delivery manager" in roles and "noc manager" in roles)
    check("hiring context detected", len(context) >= 1)

    news = "Acme Technologies acquires Beta Networks to expand its managed services footprint."
    check("acquisition timing detected", "acquisition" in signals.detect_timing(news))


if __name__ == "__main__":
    print("=" * 60)
    print("DEDUP + SAFEGUARD TESTS")
    print("=" * 60)
    test_keys()
    test_aggregators()
    test_junk_names()
    test_bot_challenge()
    test_store_dedup()
    test_signals()
    print("\n" + "=" * 60)
    print(f"RESULT: {PASS} passed, {FAIL} failed")
    print("=" * 60)
    sys.exit(1 if FAIL else 0)
