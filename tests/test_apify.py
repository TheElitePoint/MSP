#!/usr/bin/env python3
"""Apify discovery-provider and fresh-query-generation checks.

Run with: .venv/bin/python tests/test_apify.py

Offline and deterministic — HTTP calls are mocked (no real Apify/Groq spend),
mirroring the dead-key persistence pattern already verified for Tavily this
session (see pipeline/discovery/tavily.py).
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline import llm_tasks
from pipeline.discovery.apify import ApifyBudgetExhausted, ApifyProvider

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
    def __init__(self, overrides: dict | None = None) -> None:
        self._overrides = overrides or {}

    def get(self, key: str, default=None):
        return self._overrides.get(key, default)


class FakeResponse:
    def __init__(self, status_code: int, json_data=None, text: str = ""):
        self.status_code = status_code
        self._json = json_data
        self.text = text

    def json(self):
        return self._json

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeSession:
    """Replays a scripted sequence of responses, one per .post() call."""

    def __init__(self, responses: list[FakeResponse]):
        self._responses = list(responses)
        self.calls: list[dict] = []

    def post(self, url, json=None, headers=None, timeout=None):
        self.calls.append({"url": url, "json": json, "headers": headers})
        if not self._responses:
            raise RuntimeError("FakeSession: no more scripted responses")
        return self._responses.pop(0)


SAMPLE_ITEM = {
    "crawl": {"httpStatusCode": 200},
    "metadata": {"title": "Acme Managed IT | 24/7 Support", "url": "https://acmeit.com"},
    "searchResult": {
        "rank": 1, "title": "Acme Managed IT - Search Result",
        "url": "https://acmeit.com/services", "description": "We manage IT for growing businesses.",
    },
    "markdown": "# Acme Managed IT\nWe provide managed IT services...",
    "text": "Acme Managed IT We provide managed IT services...",
    "query": "managed IT services Ohio",
}


def test_candidates_shape() -> None:
    print("\nApifyProvider.candidates() — candidate dict shape")
    with tempfile.TemporaryDirectory() as tmp:
        session = FakeSession([FakeResponse(200, json_data=[SAMPLE_ITEM])])
        provider = ApifyProvider(
            FakeCfg(), "apify_api_test_token", session, state_path=Path(tmp) / "state.db"
        )
        results = list(provider.candidates("managed IT services Ohio", "Managed technology provider", set()))
        check("yields exactly one candidate", len(results) == 1, str(results))
        c = results[0]
        check("company_name extracted from title", c["company_name"] == "Acme Managed IT", c["company_name"])
        check("website prefers metadata.url over searchResult.url",
              c["website"] == "https://acmeit.com", c["website"])
        check("company_description from searchResult.description",
              c["company_description"] == "We manage IT for growing businesses.", c["company_description"])
        check("headquarters_state intentionally unset (nationwide stage)",
              c["headquarters_state"] is None)
        check("_pipeline tagged apify", c["_pipeline"] == "apify")
        check("_evidence_level is 2 (fetched page, not yet full-crawled)", c["_evidence_level"] == 2)
        check("_evidence_url set", c["_evidence_url"] == "https://acmeit.com")
        provider.close()


def test_candidates_skips_bad_pages() -> None:
    print("\nApifyProvider.candidates() — filters non-2xx and empty items")
    with tempfile.TemporaryDirectory() as tmp:
        bad_status = dict(SAMPLE_ITEM, crawl={"httpStatusCode": 404})
        no_url = {"crawl": {"httpStatusCode": 200}, "metadata": {"title": "No URL Here"},
                   "searchResult": {}}
        session = FakeSession([FakeResponse(200, json_data=[bad_status, no_url])])
        provider = ApifyProvider(
            FakeCfg(), "apify_api_test_token", session, state_path=Path(tmp) / "state.db"
        )
        results = list(provider.candidates("query", "Managed technology provider", set()))
        check("404 page and URL-less item both skipped", results == [], str(results))
        provider.close()


def test_dead_token_persistence() -> None:
    print("\nApifyProvider — dead-token memory (never removed, only skipped)")
    with tempfile.TemporaryDirectory() as tmp:
        state_path = Path(tmp) / "state.db"
        tokens = ["apify_api_aaa", "apify_api_bbb", "apify_api_ccc"]

        # Run 1: token "aaa" gets a 401 and is retired.
        session1 = FakeSession([FakeResponse(401, text="invalid token")])
        p1 = ApifyProvider(FakeCfg(), tokens, session1, state_path=state_path)
        check("keys list never shrinks at construction", len(p1.tokens) == 3)
        p1._retire_token("apify_api_aaa", "HTTP 401")
        check("retired token stays in .tokens, only added to dead set",
              "apify_api_aaa" in p1.tokens and "apify_api_aaa" in p1.dead_tokens)
        p1.close()

        # Run 2 (fresh instance, same state file): "aaa" pre-loaded as dead,
        # without a wasted probe, but still present in the pool.
        p2 = ApifyProvider(FakeCfg(), tokens, FakeSession([]), state_path=state_path)
        check("known-dead token pre-loaded as dead", "apify_api_aaa" in p2.dead_tokens)
        check("known-dead token still present in the pool (never dropped)",
              len(p2.tokens) == 3 and "apify_api_aaa" in p2.tokens)
        check("a live token is chosen as active", p2.active_token in ("apify_api_bbb", "apify_api_ccc"))
        p2.close()


def test_budget_exhaustion() -> None:
    print("\nApifyProvider — budget exhaustion raises, doesn't crash")
    with tempfile.TemporaryDirectory() as tmp:
        cfg = FakeCfg({"discovery.apify": {"max_calls_per_run": 1}})
        session = FakeSession([FakeResponse(200, json_data=[SAMPLE_ITEM]), FakeResponse(200, json_data=[])])
        provider = ApifyProvider(cfg, "apify_api_test_token", session, state_path=Path(tmp) / "state.db")
        list(provider.candidates("first query", "seg", set()))  # spends the 1-call budget
        raised = False
        try:
            list(provider.candidates("second query", "seg", set()))
        except ApifyBudgetExhausted:
            raised = True
        check("second call raises ApifyBudgetExhausted once budget is spent", raised)
        provider.close()


# --------------------------------------------------------------------------
# generate_fresh_queries
# --------------------------------------------------------------------------

class FakeGroqClient:
    def __init__(self, response, available: bool = True):
        self.available = available
        self._response = response
        self.last_user_prompt: str | None = None

    def complete_json(self, system, user, role="fast", max_tokens=700):
        self.last_user_prompt = user
        return self._response


class FakeQueryCfg:
    all_states = [{"name": "Texas"}, {"name": "California"}, {"name": "New York"},
                  {"name": "Florida"}, {"name": "Ohio"}, {"name": "Georgia"}]


def test_generate_fresh_queries_validation() -> None:
    print("\ngenerate_fresh_queries — validation guardrails")
    response = {"queries": [
        {"query": '"Service Delivery Manager" hiring credit union Ohio'},
        {"query": '"has acquired" managed IT services Georgia'},
        {"query": "list of top MSPs"},          # bad prefix, rejected
        {"query": "x"},                          # too short, rejected
        {"query": '"Service Delivery Manager" hiring credit union Ohio'},  # dup, rejected
    ]}
    client = FakeGroqClient(response)
    out = llm_tasks.generate_fresh_queries(client, FakeQueryCfg(), count=5, recent_queries=["old query"])
    check("keeps only the 2 valid, non-duplicate, well-formed queries", len(out) == 2, str(out))
    check("question/list-shaped query rejected", "list of top MSPs" not in out)
    check("too-short query rejected", "x" not in out)

    check("None response returns []", llm_tasks.generate_fresh_queries(
        FakeGroqClient(None), FakeQueryCfg(), count=5) == [])
    check("missing 'queries' key returns []", llm_tasks.generate_fresh_queries(
        FakeGroqClient({"not_queries": []}), FakeQueryCfg(), count=5) == [])
    check("unavailable client short-circuits to []", llm_tasks.generate_fresh_queries(
        FakeGroqClient({"queries": []}, available=False), FakeQueryCfg(), count=5) == [])

    recent = ["credit union IT operations partner Ohio"]
    dup_client = FakeGroqClient({"queries": [{"query": recent[0]}]})
    out2 = llm_tasks.generate_fresh_queries(dup_client, FakeQueryCfg(), count=5, recent_queries=recent)
    check("a query identical to a recent one is suppressed", out2 == [], str(out2))

    never_pads_client = FakeGroqClient({"queries": [{"query": '"NOC Manager" hiring Texas'}]})
    out3 = llm_tasks.generate_fresh_queries(never_pads_client, FakeQueryCfg(), count=10)
    check("never pads short of the requested count — returns only what's valid",
          out3 == ['"NOC Manager" hiring Texas'], str(out3))


def test_generate_fresh_queries_prompt_varies() -> None:
    print("\ngenerate_fresh_queries — prompt varies to defeat the response cache")
    client = FakeGroqClient({"queries": [{"query": "test query one two three four"}]})
    llm_tasks.generate_fresh_queries(client, FakeQueryCfg(), count=1)
    prompt_a = client.last_user_prompt
    llm_tasks.generate_fresh_queries(client, FakeQueryCfg(), count=1)
    prompt_b = client.last_user_prompt
    check("today's date is present in the prompt", str(__import__("datetime").datetime.now().year) in prompt_a)
    check("prompt is non-empty and includes generation instructions",
          "Generate 1 distinct" in prompt_a)
    # theme/state sampling is randomised, so prompts differ often but not
    # guaranteed every single pair — just confirm the mechanism ran without error.
    check("second call also produced a prompt", bool(prompt_b))


if __name__ == "__main__":
    print("=" * 62)
    print("APIFY DISCOVERY + FRESH-QUERY-GENERATION TESTS")
    print("=" * 62)
    test_candidates_shape()
    test_candidates_skips_bad_pages()
    test_dead_token_persistence()
    test_budget_exhaustion()
    test_generate_fresh_queries_validation()
    test_generate_fresh_queries_prompt_varies()
    print("\n" + "=" * 62)
    print(f"RESULT: {PASS} passed, {FAIL} failed")
    print("=" * 62)
    sys.exit(1 if FAIL else 0)
