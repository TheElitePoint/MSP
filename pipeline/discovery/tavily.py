"""Tavily search discovery and news/intent lookups.

Two uses:
  1. Discovery — run the SOP query library (Modules 36/37/58) per state and
     city, and turn each non-aggregator result domain into a candidate company.
  2. Intent    — targeted news search per company for the timing events in
     SOP Module 55 (acquisition, expansion, migration, leadership change).

Credit discipline: a hard per-run call budget plus a query cooldown so a
repeat run never re-spends credits on a query that recently succeeded.
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

log = logging.getLogger("pipeline.tavily")

API_URL = "https://api.tavily.com/search"


def _key_fingerprint(key: str) -> str:
    """Stable, non-secret identifier for a key — never persist the key itself."""
    return hashlib.sha256(key.encode()).hexdigest()[:24]


class TavilyBudgetExhausted(RuntimeError):
    """Raised when the configured per-run call budget is used up."""


class TavilyProvider:
    """Tavily client with automatic rotation across a pool of API keys.

    Free Tavily dev keys carry a monthly credit allowance. When a key returns
    an auth error or a credit/usage error, it is retired for the rest of the
    run and the next key takes over, so a long 50-state run keeps going
    instead of stopping at the first exhausted key.
    """

    source_name = "tavily"

    # Status codes that mean "this key is done", not "this request failed".
    KEY_DEAD_CODES = {401, 403, 402, 432, 433}

    def __init__(
        self,
        cfg: Any,
        api_keys: str | list[str],
        session: requests.Session | None = None,
        state_path: Path | None = None,
    ) -> None:
        node = cfg.get("discovery.tavily", {}) or {}
        if isinstance(api_keys, str):
            api_keys = [api_keys]
        self.keys: list[str] = [k.strip() for k in api_keys if k and k.strip()]
        if not self.keys:
            raise ValueError("TavilyProvider requires at least one API key")
        self.key_index = 0
        self.dead_keys: set[str] = set()
        self.calls_per_key: dict[str, int] = {k: 0 for k in self.keys}

        self.max_calls = int(node.get("max_calls_per_run", 1000))
        self.search_depth = node.get("search_depth", "basic")
        self.max_results = int(node.get("max_results_per_query", 20))
        self.sleep_between = float(node.get("sleep_between_calls", 0.6))
        self.session = session or requests.Session()
        self.calls_made = 0
        self.failed_calls = 0

        # Cross-run key-death memory. Without this, a key that dies on HTTP
        # 432/quota two seconds into every run gets re-probed (and re-fails)
        # at the start of every future run too — a small but real, and
        # completely avoidable, spend on a call that cannot succeed.
        self._retry_after_hours = float(node.get("dead_key_retry_hours", 24))
        self._state: sqlite3.Connection | None = None
        self._open_state(state_path or Path("data/tavily_key_state.db"))
        self._load_persisted_dead_keys()

    def _open_state(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._state = sqlite3.connect(path, timeout=30, check_same_thread=False)
        self._state.execute(
            "CREATE TABLE IF NOT EXISTS dead_keys ("
            "key_hash TEXT PRIMARY KEY, reason TEXT, retired_at TEXT)"
        )
        self._state.commit()

    def _load_persisted_dead_keys(self) -> None:
        if self._state is None:
            return
        cutoff = datetime.now(timezone.utc) - timedelta(hours=self._retry_after_hours)
        rows = self._state.execute("SELECT key_hash, reason, retired_at FROM dead_keys").fetchall()
        loaded_reasons: list[str] = []
        for key_hash, reason, retired_at in rows:
            try:
                retired = datetime.fromisoformat(retired_at)
            except (TypeError, ValueError):
                continue
            if retired.tzinfo is None:
                retired = retired.replace(tzinfo=timezone.utc)
            if retired < cutoff:
                continue  # old enough to deserve a retry — a free-tier quota may have reset
            for key in self.keys:
                if _key_fingerprint(key) == key_hash:
                    self.dead_keys.add(key)
                    loaded_reasons.append(reason)
        if loaded_reasons:
            log.info(
                "Tavily: %d of %d key(s) already known dead from a previous run (%s) — "
                "not re-probed",
                len(loaded_reasons), len(self.keys), ", ".join(sorted(set(loaded_reasons))),
            )

    def _persist_dead_key(self, key: str, reason: str) -> None:
        if self._state is None:
            return
        self._state.execute(
            "INSERT OR REPLACE INTO dead_keys(key_hash, reason, retired_at) VALUES (?,?,?)",
            (_key_fingerprint(key), reason, datetime.now(timezone.utc).isoformat(timespec="seconds")),
        )
        self._state.commit()

    @property
    def budget_left(self) -> int:
        return max(0, self.max_calls - self.calls_made)

    @property
    def active_key(self) -> str | None:
        for offset in range(len(self.keys)):
            key = self.keys[(self.key_index + offset) % len(self.keys)]
            if key not in self.dead_keys:
                self.key_index = (self.key_index + offset) % len(self.keys)
                return key
        return None

    def _retire_key(self, key: str, why: str) -> None:
        self.dead_keys.add(key)
        self._persist_dead_key(key, why)
        self.key_index = (self.key_index + 1) % len(self.keys)
        remaining = len(self.keys) - len(self.dead_keys)
        log.warning(
            "Tavily key ...%s retired (%s). %d of %d keys still active.",
            key[-6:], why, remaining, len(self.keys),
        )

    def close(self) -> None:
        if self._state is not None:
            self._state.close()
            self._state = None

    def search(
        self,
        query: str,
        max_results: int | None = None,
        topic: str = "general",
        days: int | None = None,
        exclude_domains: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """One Tavily search. Returns [] on transient failure; raises when the
        run-level budget or every key is exhausted."""
        if self.calls_made >= self.max_calls:
            raise TavilyBudgetExhausted(
                f"Tavily budget of {self.max_calls} calls exhausted for this run"
            )

        payload: dict[str, Any] = {
            "query": query,
            "search_depth": self.search_depth,
            "max_results": max_results or self.max_results,
            "topic": topic,
            "include_answer": False,
            "include_raw_content": False,
        }
        if days is not None:
            payload["days"] = days
        if exclude_domains:
            # Tavily caps this list; send the most noise-heavy ones.
            payload["exclude_domains"] = exclude_domains[:50]

        # One attempt per key, plus retries for transient errors.
        attempts = max(3, len(self.keys) + 1)
        for attempt in range(attempts):
            key = self.active_key
            if key is None:
                raise TavilyBudgetExhausted("All Tavily API keys are exhausted or invalid")

            headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
            # Older Tavily keys authenticate via the body; sending both is harmless.
            body = dict(payload, api_key=key)

            try:
                resp = self.session.post(API_URL, json=body, headers=headers, timeout=45)
                self.calls_made += 1
                self.calls_per_key[key] = self.calls_per_key.get(key, 0) + 1

                if resp.status_code in self.KEY_DEAD_CODES:
                    self._retire_key(key, f"HTTP {resp.status_code}")
                    continue

                if resp.status_code == 429:
                    # Could be per-minute throttling or a spent monthly quota.
                    detail = (resp.text or "")[:200].lower()
                    if any(w in detail for w in ("credit", "quota", "usage limit", "exceeded")):
                        self._retire_key(key, "credits exhausted")
                        continue
                    if len(self.keys) - len(self.dead_keys) > 1:
                        # Other keys are available — switch rather than sleep.
                        self.key_index = (self.key_index + 1) % len(self.keys)
                        continue
                    wait = 5 * (attempt + 1)
                    log.warning("Tavily rate-limited; waiting %ds", wait)
                    time.sleep(wait)
                    continue

                resp.raise_for_status()
                time.sleep(self.sleep_between)
                return resp.json().get("results", []) or []

            except TavilyBudgetExhausted:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning("Tavily query failed (attempt %d) %r: %s", attempt + 1, query[:60], exc)
                time.sleep(2 * (attempt + 1))

        self.failed_calls += 1
        return []

    def key_usage(self) -> dict[str, Any]:
        return {
            "keys_total": len(self.keys),
            "keys_active": len(self.keys) - len(self.dead_keys),
            "calls_made": self.calls_made,
            "per_key": {f"...{k[-6:]}": v for k, v in self.calls_per_key.items()},
        }

    # -- discovery -----------------------------------------------------------

    def candidates(
        self,
        query: str,
        state_code: str,
        city: str | None,
        segment: str,
        aggregators: set[str],
    ) -> Iterator[dict[str, Any]]:
        """Turn search results into candidate company records."""
        results = self.search(query, exclude_domains=sorted(aggregators))
        for res in results:
            url = (res.get("url") or "").strip()
            title = (res.get("title") or "").strip()
            if not url or not title:
                continue

            name = clean_company_name(title)
            if not name:
                continue

            yield {
                "company_name": name,
                "website": url,
                "headquarters_city": city,
                "headquarters_state": state_code,
                "country": "United States",
                "segment": segment,
                "company_description": (res.get("content") or "")[:1500] or None,
                "discovery_source": f"Tavily search ({state_code})",
                "discovery_query": query,
                "status": "raw",
                "_pipeline": "tavily",
                "_evidence_url": url,
                "_evidence_level": 3,  # search snippet = discovery only, must verify
            }

    # -- intent --------------------------------------------------------------

    def news_for_company(self, company_name: str, days: int = 365) -> list[dict[str, Any]]:
        """Timing-signal search for one company (SOP Module 55)."""
        query = (
            f'"{company_name}" (acquisition OR acquires OR expansion OR '
            f'"new office" OR "cloud migration" OR "appoints" OR "launches")'
        )
        return self.search(query, max_results=6, topic="news", days=days)
