"""Apify-powered fresh-discovery: LLM-generated queries, scraped via the
`apify/rag-web-browser` actor.

The static SOP query library (config/queries.yaml) is a fixed set of
product-label phrases, each gated by a 14-day exact-text cooldown — once a
geography is well-mined, there is very little new ground left. This provider
is the other half of the fix: pipeline/llm_tasks.py:generate_fresh_queries
produces query text that genuinely varies run to run, so it naturally
sidesteps that cooldown instead of re-mining the same phrasing.

One HTTP call to rag-web-browser does search + full-page scrape in one step —
functionally the closest analog to what TavilyProvider does, just backed by a
different search+scrape engine. Mirrors TavilyProvider's shape closely: same
budget-exception pattern, same persisted dead-token memory so a bad token
isn't re-probed every run (a token is skipped, never removed from the pool —
it is retried automatically after `dead_key_retry_hours`).
"""

from __future__ import annotations

import hashlib
import logging
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

import requests

from .titles import clean_company_name

log = logging.getLogger("pipeline.apify")

API_URL = "https://api.apify.com/v2/acts/apify~rag-web-browser/run-sync-get-dataset-items"


def _key_fingerprint(token: str) -> str:
    """Stable, non-secret identifier for a token — never persist the token itself."""
    return hashlib.sha256(token.encode()).hexdigest()[:24]


class ApifyBudgetExhausted(RuntimeError):
    """Raised when the configured per-run call budget is used up."""


class ApifyProvider:
    """Apify client with automatic rotation across a pool of API tokens.

    Apify is compute-unit billed (each call scrapes several full pages), not
    a flat per-call credit like Tavily — see discovery.apify.max_calls_per_run
    in settings.yaml for the guardrail this implies.
    """

    source_name = "apify"

    # Apify's auth failure. Usage/quota exhaustion is a 429 with a body that
    # mentions usage/limit — Apify doesn't have Tavily's distinct dead codes.
    KEY_DEAD_CODES = {401}

    def __init__(
        self,
        cfg: Any,
        api_tokens: str | list[str],
        session: requests.Session | None = None,
        state_path: Path | None = None,
    ) -> None:
        node = cfg.get("discovery.apify", {}) or {}
        if isinstance(api_tokens, str):
            api_tokens = [api_tokens]
        self.tokens: list[str] = [t.strip() for t in api_tokens if t and t.strip()]
        if not self.tokens:
            raise ValueError("ApifyProvider requires at least one API token")
        self.token_index = 0
        self.dead_tokens: set[str] = set()
        self.calls_per_token: dict[str, int] = {t: 0 for t in self.tokens}

        self.max_calls = int(node.get("max_calls_per_run", 25))
        self.max_results = int(node.get("max_results_per_query", 8))
        self.output_formats = node.get("output_formats", ["markdown"])
        self.request_timeout = int(node.get("request_timeout_seconds", 300))
        self.sleep_between = float(node.get("sleep_between_calls", 1.0))
        self.session = session or requests.Session()
        self.calls_made = 0
        self.failed_calls = 0

        # Cross-run token-death memory — same pattern as TavilyProvider.
        self._retry_after_hours = float(node.get("dead_key_retry_hours", 24))
        self._state: sqlite3.Connection | None = None
        self._open_state(state_path or Path("data/apify_key_state.db"))
        self._load_persisted_dead_tokens()

    def _open_state(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._state = sqlite3.connect(path, timeout=30, check_same_thread=False)
        self._state.execute(
            "CREATE TABLE IF NOT EXISTS dead_tokens ("
            "token_hash TEXT PRIMARY KEY, reason TEXT, retired_at TEXT)"
        )
        self._state.commit()

    def _load_persisted_dead_tokens(self) -> None:
        if self._state is None:
            return
        cutoff = datetime.now(timezone.utc) - timedelta(hours=self._retry_after_hours)
        rows = self._state.execute(
            "SELECT token_hash, reason, retired_at FROM dead_tokens"
        ).fetchall()
        loaded_reasons: list[str] = []
        for token_hash, reason, retired_at in rows:
            try:
                retired = datetime.fromisoformat(retired_at)
            except (TypeError, ValueError):
                continue
            if retired.tzinfo is None:
                retired = retired.replace(tzinfo=timezone.utc)
            if retired < cutoff:
                continue  # old enough to deserve a retry — quota may have reset
            for token in self.tokens:
                if _key_fingerprint(token) == token_hash:
                    self.dead_tokens.add(token)
                    loaded_reasons.append(reason)
        if loaded_reasons:
            log.info(
                "Apify: %d of %d token(s) already known dead from a previous run (%s) — "
                "not re-probed",
                len(loaded_reasons), len(self.tokens), ", ".join(sorted(set(loaded_reasons))),
            )

    def _persist_dead_token(self, token: str, reason: str) -> None:
        if self._state is None:
            return
        self._state.execute(
            "INSERT OR REPLACE INTO dead_tokens(token_hash, reason, retired_at) VALUES (?,?,?)",
            (_key_fingerprint(token), reason,
             datetime.now(timezone.utc).isoformat(timespec="seconds")),
        )
        self._state.commit()

    @property
    def budget_left(self) -> int:
        return max(0, self.max_calls - self.calls_made)

    @property
    def active_token(self) -> str | None:
        for offset in range(len(self.tokens)):
            token = self.tokens[(self.token_index + offset) % len(self.tokens)]
            if token not in self.dead_tokens:
                self.token_index = (self.token_index + offset) % len(self.tokens)
                return token
        return None

    def _retire_token(self, token: str, why: str) -> None:
        self.dead_tokens.add(token)
        self._persist_dead_token(token, why)
        self.token_index = (self.token_index + 1) % len(self.tokens)
        remaining = len(self.tokens) - len(self.dead_tokens)
        log.warning(
            "Apify token ...%s retired (%s). %d of %d tokens still active.",
            token[-6:], why, remaining, len(self.tokens),
        )

    def close(self) -> None:
        if self._state is not None:
            self._state.close()
            self._state = None

    def run(self, query: str, max_results: int | None = None) -> list[dict[str, Any]]:
        """One rag-web-browser call: search + full-page scrape. Returns []
        on transient failure; raises when the run-level budget or every
        token is exhausted."""
        if self.calls_made >= self.max_calls:
            raise ApifyBudgetExhausted(
                f"Apify budget of {self.max_calls} calls exhausted for this run"
            )

        payload: dict[str, Any] = {
            "query": query,
            "maxResults": max_results or self.max_results,
            "outputFormats": self.output_formats,
        }

        attempts = max(3, len(self.tokens) + 1)
        for attempt in range(attempts):
            token = self.active_token
            if token is None:
                raise ApifyBudgetExhausted("All Apify tokens are exhausted or invalid")

            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

            try:
                resp = self.session.post(
                    API_URL, json=payload, headers=headers, timeout=self.request_timeout
                )
                self.calls_made += 1
                self.calls_per_token[token] = self.calls_per_token.get(token, 0) + 1

                if resp.status_code in self.KEY_DEAD_CODES:
                    self._retire_token(token, f"HTTP {resp.status_code}")
                    continue

                if resp.status_code == 429:
                    detail = (resp.text or "")[:200].lower()
                    if any(w in detail for w in ("usage", "limit", "exceeded", "quota")):
                        self._retire_token(token, "usage limit exhausted")
                        continue
                    if len(self.tokens) - len(self.dead_tokens) > 1:
                        self.token_index = (self.token_index + 1) % len(self.tokens)
                        continue
                    wait = 5 * (attempt + 1)
                    log.warning("Apify rate-limited; waiting %ds", wait)
                    time.sleep(wait)
                    continue

                resp.raise_for_status()
                time.sleep(self.sleep_between)
                data = resp.json()
                return data if isinstance(data, list) else []

            except ApifyBudgetExhausted:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning("Apify query failed (attempt %d) %r: %s", attempt + 1, query[:60], exc)
                time.sleep(2 * (attempt + 1))

        self.failed_calls += 1
        return []

    def token_usage(self) -> dict[str, Any]:
        return {
            "keys_total": len(self.tokens),
            "keys_active": len(self.tokens) - len(self.dead_tokens),
            "calls_made": self.calls_made,
            "per_token": {f"...{t[-6:]}": v for t, v in self.calls_per_token.items()},
        }

    # -- discovery -----------------------------------------------------------

    def candidates(
        self,
        query: str,
        segment: str,
        aggregators: set[str],
    ) -> Iterator[dict[str, Any]]:
        """Turn rag-web-browser results into candidate company records.

        Unlike Tavily, rag-web-browser actually fetches each page, so a
        result carries both the SERP snippet (searchResult.*) and the real
        fetched page (metadata.*) — prefer the latter when present.
        headquarters_state is intentionally left unset: this stage is
        nationwide by design (see llm_tasks.generate_fresh_queries), so there
        is no single searched region to fall back on the way Tavily/Overpass
        have. The enrich stage resolves it from the crawled page's address,
        same as every other source relies on for confirmation.

        `aggregators` is accepted for interface parity with
        TavilyProvider.candidates() (so run.py can call every discovery
        provider the same way) — the actor's input schema has no
        domain-exclusion option, so filtering happens where it already
        happens for every source: run.py's _ingest aggregator-domain guard.
        """
        items = self.run(query)
        for item in items:
            crawl = item.get("crawl") or {}
            status = crawl.get("httpStatusCode")
            if status is not None and not (200 <= int(status) < 300):
                continue  # page never really loaded — not a company

            meta = item.get("metadata") or {}
            search_result = item.get("searchResult") or {}
            url = (meta.get("url") or search_result.get("url") or "").strip()
            title = (meta.get("title") or search_result.get("title") or "").strip()
            if not url or not title:
                continue

            name = clean_company_name(title)
            if not name:
                continue

            description = (search_result.get("description") or "").strip()
            if not description:
                body = item.get("text") or item.get("markdown") or ""
                description = body[:1500]

            yield {
                "company_name": name,
                "website": url,
                "headquarters_city": None,
                "headquarters_state": None,
                "country": "United States",
                "segment": segment,
                "company_description": description[:1500] or None,
                "discovery_source": "Apify rag-web-browser search",
                "discovery_query": query,
                "status": "raw",
                "_pipeline": "apify",
                "_evidence_url": url,
                # A genuinely fetched page is stronger than a bare search
                # snippet (Tavily's level 3), but hasn't been through the
                # full site crawl yet (level 1).
                "_evidence_level": 2,
            }
