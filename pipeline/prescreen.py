"""Pre-crawl screen — reject obvious non-operators before spending a crawl.

SOP Module 31's recommended work order is: build a raw universe, quick-screen
obvious exclusions, THEN fully validate the strongest candidates. The
pipeline used to skip the middle step entirely — every discovered record got
a full website crawl (~2-3s each) before classify_entity ever ran, and only
then discovered that most non-operators (85% of all clean-stage defects in a
representative run) were software vendors, directories or associations that
a much cheaper classification of the discovery-time search snippet would
already have caught.

This runs classify.classify_entity against `company_description` — the
snippet Tavily already returned for free at discovery time — and only acts
on a HIGH-CONFIDENCE non-operator verdict. The snippet is much thinner
evidence than a full crawl, so the bar here is deliberately higher than
clean.py's crawl-time bar: a false screen-out here is unrecoverable (the
company never gets crawled to prove otherwise), so this only screens out
verdicts it is quite sure about, and always lets anything uncertain through
to a real crawl.
"""

from __future__ import annotations

from typing import Any

from . import classify

# Higher confidence than clean.py's post-crawl bar, which trusts a full site.
# Validated against this pipeline's own production data: at 0.75 this caught
# only 3 of 89 confirmed non-operators (3.4% — real but modest), and produced
# 2 false positives out of 63 confirmed real operators (Cognizant, Innowise)
# — both driven entirely by the generic word "platform" repeating in a short
# snippet, with no genuinely distinctive vendor language. At 0.97 the catch
# rate is unchanged (3/89) and the false-positive rate drops to 0/63. Given a
# prescreen mistake is much harder to notice than a post-crawl one (the
# company is never crawled to prove otherwise), this stays deliberately
# conservative rather than chasing a bigger catch rate.
MIN_CONFIDENCE = 0.97

# Only screen out types that are unambiguous discovery-time false positives
# for this pipeline (see classify.py's docstring: Splunk/Arctic Wolf/Mimecast
# as software_vendor, ChannelPro-style rankings as directory_media, member
# bodies as association). msp_operator, end_user_org, unknown, and anything
# else stays in — those need the full crawl to confirm either way, since a
# mid-market end-user org (SOP Module 23) must never be screened out here.
SCREEN_OUT_TYPES = {"software_vendor", "directory_media", "association"}

# Below this, classify_entity's marker-count gates are working off too little
# text to mean anything — always let it through to a real crawl.
MIN_SNIPPET_CHARS = 200


def prescreen(
    company_description: str | None,
    website: str | None,
    min_confidence: float = MIN_CONFIDENCE,
) -> tuple[bool, str | None]:
    """Return (screen_out, reason). No crawl involved — snippet only."""
    text = company_description or ""
    if len(text) < MIN_SNIPPET_CHARS:
        return False, None

    result: dict[str, Any] = classify.classify_entity(text, website)
    if result["type"] in SCREEN_OUT_TYPES and result["confidence"] >= min_confidence:
        evidence = ", ".join(result["evidence"][:4])
        reason = (
            f"Pre-crawl screen: classified as {result['type'].replace('_', ' ')} "
            f"from the search snippet ({evidence})"
        )
        return True, reason
    return False, None
