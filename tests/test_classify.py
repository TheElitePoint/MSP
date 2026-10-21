#!/usr/bin/env python3
"""Live checks for entity classification, geography and name resolution.

These fetch real sites, because the failures being guarded against were all
real: Splunk scored as an MSP, "IT Managed Services" became a company name,
and German and Indian firms reached a US-only tracker.

Run with: .venv/bin/python tests/test_classify.py
Use --offline to skip the network checks.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from bs4 import BeautifulSoup

from pipeline import classify, nameres
from pipeline import intent_signals as intent_lex

PASS, FAIL = 0, 0


def check(label: str, condition: bool, detail: str = "") -> None:
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  PASS  {label}")
    else:
        FAIL += 1
        print(f"  FAIL  {label}  {detail}")


def test_descriptive_names() -> None:
    print("\nDescriptive-name rejection")
    bad = [
        ("IT Managed Services", "https://the20msp.com"),
        ("Managed IT Services and Business Computer Support", "https://helpmepcs.com"),
        ("Managed IT Services & Cybersecurity Solutions", "https://techsolutions.business"),
        ("Network security platform for business", "https://nordlayer.com"),
        ("Transforming Government with Tech, Speed and Scale", "https://maximus.com"),
        ("The Leader in OT Cybersecurity Technology", "https://nozominetworks.com"),
        ("Helping credit unions in Delaware and New Hampshire", "https://ccua.org"),
        ("www.cognizant.com", "https://cognizant.com"),
        ("Los Angeles IT Providers", "https://clearfuze.com"),
        ("IT Help and Support", "https://it.osu.edu"),
    ]
    for name, site in bad:
        check(f"rejects {name[:44]!r}", nameres.is_descriptive(name, site))

    good = [
        ("Sourcepass", "https://sourcepass.com"),
        ("Park Place Technologies", "https://parkplacetechnologies.com"),
        ("Corsica Technologies", "https://corsicatech.com"),
        ("Intelligent Technical Solutions", "https://itsasap.com"),
        ("Network Doctor", "https://networkdr.com"),
        ("MySherpa", "https://mysherpa.com"),
        ("Pegasus Technologies", "https://pegasustechnologies.com"),
    ]
    for name, site in good:
        check(f"accepts {name!r}", not nameres.is_descriptive(name, site))

    # All-generic name that genuinely IS the brand, proven by the domain.
    check(
        "keeps 'IT Solutions' for itsolutions-inc.com",
        not nameres.is_descriptive("IT Solutions", "https://itsolutions-inc.com"),
    )


def test_name_from_html() -> None:
    print("\nName resolution from structured markup")

    jsonld = """
    <html><head><title>Managed IT Services in Dallas | Top MSP</title>
    <script type="application/ld+json">
    {"@context":"https://schema.org","@type":"Organization",
     "name":"Acme Managed Technology","url":"https://acmemt.com"}
    </script></head><body><p>x</p></body></html>
    """
    soup = BeautifulSoup(jsonld, "lxml")
    name, source = nameres.resolve(soup, "x", "https://acmemt.com")
    check("JSON-LD Organization name wins", name == "Acme Managed Technology", f"got {name!r}")
    check("source recorded as jsonld", source == "jsonld", f"got {source!r}")

    copyright_html = """
    <html><head><title>Managed IT Services</title></head>
    <body><footer>© 2025 Brightline Technology Group. All rights reserved.</footer></body></html>
    """
    soup = BeautifulSoup(copyright_html, "lxml")
    text = soup.get_text(" ", strip=True)
    name, source = nameres.resolve(soup, text, "https://brightlinetg.com")
    check("copyright line used", name == "Brightline Technology Group", f"got {name!r}")

    nothing = "<html><head><title>Managed IT Services</title></head><body><p>hi</p></body></html>"
    soup = BeautifulSoup(nothing, "lxml")
    name, source = nameres.resolve(soup, "hi", "https://acme-tech-partners.com")
    check("falls back to domain", name == "Acme Tech Partners", f"got {name!r}")
    check("source recorded as domain", source == "domain")


def test_geography_offline() -> None:
    print("\nGeography classification (offline)")
    for site in ("https://dts.de", "https://cortecit.co.uk", "https://codleo.com/in/en"):
        geo = classify.classify_geography("some text", site)
        check(f"non-US: {site}", geo["is_us"] is False, str(geo["reasons"]))

    us_text = "Visit us at 1200 Market Street, Wilmington, DE 19801. Call (302) 555-0100."
    geo = classify.classify_geography(us_text, "https://acmemt.com")
    check("US address detected", geo["is_us"] is True and geo["state"] == "DE", str(geo))

    uk_text = "Registered in England and Wales. Company number 09876543. Prices from £99."
    geo = classify.classify_geography(uk_text, "https://acme.com")
    check("UK legal form + currency detected", geo["is_us"] is False, str(geo["reasons"]))


def test_intent_offline() -> None:
    print("\nIntent detection (offline)")
    msp = (
        "We run ConnectWise Manage and ConnectWise Automate with Auvik monitoring, "
        "IT Glue documentation, Veeam backup and SentinelOne security across Azure "
        "and Microsoft 365. We are SOC 2 Type II certified. View our system status "
        "page and incident history. We support 450 clients nationwide."
    )
    count, tools = intent_lex.detect_tool_fragmentation(msp)
    check("tool fragmentation across categories", count >= 5, f"got {count}: {list(tools)}")
    check("compliance pursuit detected", bool(intent_lex.detect_compliance_pursuit(msp)))
    check("incident history detected", bool(intent_lex.detect_incident_history(msp)))
    check("client scale detected", intent_lex.detect_client_scale(msp) == 450)

    roles = intent_lex.detect_high_value_hiring(
        "Open roles: Service Delivery Manager, NOC Manager, Change Manager"
    )
    check("high-value roles detected", len(set(roles)) >= 3, str(roles))

    # Recency decay: an old signal must be worth less than a fresh one.
    fresh = intent_lex.recency_multiplier("2026-08-01T00:00:00+00:00")
    old = intent_lex.recency_multiplier("2023-01-01T00:00:00+00:00")
    check("recent signal weighted higher", fresh > old, f"fresh={fresh} old={old}")
    check("decay floors at 0.25", old >= 0.25)


def test_entity_offline() -> None:
    print("\nEntity classification (offline fixtures)")

    operator = (
        "We are a managed service provider. Our clients trust us to manage their "
        "entire IT environment. Our NOC monitors client environments 24/7 under a "
        "managed services agreement with SLA-backed response. We become your IT "
        "department with co-managed IT and help desk services. Our engineers "
        "provide onsite support and remote support to businesses we support."
    ) * 2
    result = classify.classify_entity(operator, "https://acmemsp.com")
    check("operator classified", result["type"] == "msp_operator", str(result["scores"]))

    vendor = (
        "Request a demo of our platform. Start free trial. View pricing plans. "
        "Our software platform integrates with your stack. Read the API "
        "documentation and developer docs. Browse the integrations marketplace. "
        "See release notes and changelog. Named a Leader in the Magic Quadrant."
    ) * 2
    result = classify.classify_entity(vendor, "https://acmeplatform.com")
    check("vendor classified", result["type"] == "software_vendor", str(result["scores"]))

    directory = (
        "Top 10 managed service providers. Compare providers and read reviews and "
        "ratings. Advertise with us or get listed. Submit your company to claim "
        "your profile. Our editorial team publishes a buyer's guide and rankings."
    ) * 2
    result = classify.classify_entity(directory, "https://mspreviews.com")
    check("directory classified", result["type"] == "directory_media", str(result["scores"]))

    association = (
        "Become a member and enjoy membership benefits. Our members meet at the "
        "annual conference. See the member directory and board of directors. "
        "Membership dues support our advocacy as a trade association."
    ) * 2
    result = classify.classify_entity(association, "https://itcouncil.org")
    check("association classified", result["type"] == "association", str(result["scores"]))

    check("vendor is not an acceptable account",
          not classify.is_acceptable("software_vendor", "Managed technology provider"))
    check("operator is acceptable",
          classify.is_acceptable("msp_operator", "Managed technology provider"))
    check("end-user org acceptable for healthcare segment",
          classify.is_acceptable("end_user_org", "Healthcare IT environment"))


def test_entity_live() -> None:
    print("\nEntity classification (live sites)")
    import requests

    from pipeline.config import Config
    from pipeline.enrich import WebsiteEnricher

    enricher = WebsiteEnricher(Config())
    # Every one of these leaked into a real export before entity
    # classification existed. "NOT" means "must not be treated as an account".
    cases = [
        ("https://splunk.com", "software_vendor"),
        ("https://arcticwolf.com", "software_vendor"),
        ("https://mimecast.com", "software_vendor"),
        ("https://channelpronetwork.com", "directory_media"),
        ("https://mspranked.com", "directory_media"),
        ("https://imcconstruction.com", "NOT"),
        ("https://dvch.org", "NOT"),
        ("https://corsicatech.com", "msp_operator"),
        ("https://mysherpa.com", "msp_operator"),
        ("https://pegasustechnologies.com", "msp_operator"),
        ("https://networkdr.com", "msp_operator"),
        ("https://xpertechs.com", "msp_operator"),
        ("https://atlantic-it.net", "msp_operator"),
    ]
    for url, expected in cases:
        try:
            result = enricher.crawl(url)
        except Exception as exc:  # noqa: BLE001
            print(f"  SKIP  {url} ({exc})")
            continue
        if not result.reachable:
            print(f"  SKIP  {url} (unreachable: {result.error})")
            continue
        got = classify.classify_entity(result.text, url)
        ok = (got["type"] != "msp_operator") if expected == "NOT" else (got["type"] == expected)
        check(f"{url} -> {expected}", ok, f"got {got['type']} scores={got['scores']}")


if __name__ == "__main__":
    offline = "--offline" in sys.argv
    print("=" * 62)
    print("CLASSIFICATION / NAME / INTENT TESTS")
    print("=" * 62)
    test_descriptive_names()
    test_name_from_html()
    test_geography_offline()
    test_intent_offline()
    test_entity_offline()
    if not offline:
        test_entity_live()
    print("\n" + "=" * 62)
    print(f"RESULT: {PASS} passed, {FAIL} failed")
    print("=" * 62)
    sys.exit(1 if FAIL else 0)
