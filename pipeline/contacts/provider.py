"""Apify Sales Navigator client — decision-maker search (SOP Module 40).

Actor: bestscrapers/sales-navigator-scraper-by-filters (id GhuCZ8JQ8IDGqsZaN),
cookieless, PAY_PER_EVENT: **$0.50 per search plus $0.01 per result returned**.

That pricing is why this provider counts money rather than calls. Every other
provider in this codebase has a flat per-call credit, so `max_calls_per_run` is
a meaningful guardrail for them. Here a single call can cost anywhere from
$0.51 to $25.50 depending on `limit`, so a call ceiling would not bound spend at
all. `search()` therefore refuses **before** issuing the request when the
projected cost would breach the run's dollar cap — never after, when the money
is already gone.

Token pool is shared with ApifyProvider (same credentials, same
cfg.apify_tokens) but the dead-token state file is separate: a token can be
perfectly healthy for rag-web-browser and rejected here for an unrelated
billing reason, and retiring it in the shared file would disable fresh
discovery for 24h as a side effect.
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Any

import requests

from ..discovery.keypool import TokenPool

log = logging.getLogger("pipeline.contacts")

ACTOR_ID = "GhuCZ8JQ8IDGqsZaN"
API_URL = f"https://api.apify.com/v2/acts/{ACTOR_ID}/run-sync-get-dataset-items"

INIT_COST_USD = 0.50
RESULT_COST_USD = 0.01


class SalesNavBudgetExhausted(RuntimeError):
    """Raised before a request when the run's search or dollar cap is reached."""


class SalesNavProvider:
    source_name = "salesnav"
    # 403 covers both bad auth and Apify's "monthly usage hard limit exceeded"
    # (platform-feature-disabled) — both mean this token cannot serve this
    # actor right now, so retire and rotate rather than burning all retry
    # attempts on the same dead token.
    KEY_DEAD_CODES = {401, 403}

    def __init__(
        self,
        cfg: Any,
        api_tokens: str | list[str],
        session: requests.Session | None = None,
        state_path: Path | None = None,
    ) -> None:
        node = cfg.get("contacts", {}) or {}
        if isinstance(api_tokens, str):
            api_tokens = [api_tokens]
        self.pool = TokenPool(
            list(api_tokens),
            state_path or Path("data/salesnav_key_state.db"),
            retry_after_hours=float(node.get("dead_key_retry_hours", 24)),
            label="SalesNav",
        )

        self.max_searches = int(node.get("max_searches_per_run", 40))
        self.max_spend = float(node.get("max_spend_usd_per_run", 30.0))
        self.request_timeout = int(node.get("request_timeout_seconds", 300))
        self.sleep_between = float(node.get("sleep_between_calls", 1.0))
        self.poll_max_attempts = int(node.get("poll_max_attempts", 6))
        self.poll_sleep = float(node.get("poll_sleep_seconds", 10))

        self.session = session or requests.Session()
        self.searches_made = 0
        self.results_returned = 0
        self.failed_calls = 0

    # -- budget --------------------------------------------------------------

    @property
    def spend_usd(self) -> float:
        return round(
            self.searches_made * INIT_COST_USD + self.results_returned * RESULT_COST_USD, 2
        )

    def _assert_affordable(self, limit: int) -> None:
        """Refuse before spending, not after."""
        if self.searches_made >= self.max_searches:
            raise SalesNavBudgetExhausted(
                f"SalesNav search cap of {self.max_searches} reached for this run"
            )
        projected = self.spend_usd + INIT_COST_USD + limit * RESULT_COST_USD
        if projected > self.max_spend:
            raise SalesNavBudgetExhausted(
                f"SalesNav spend cap of ${self.max_spend:.2f} would be exceeded "
                f"(${self.spend_usd:.2f} spent, next search up to ${projected:.2f})"
            )

    # -- requests ------------------------------------------------------------

    def search(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Run one people search. Returns {"data": [...], "request_id", "status"}.

        Raises SalesNavBudgetExhausted before the POST when the run cap is hit,
        and when every token is dead.
        """
        limit = int(payload.get("limit", 25))
        self._assert_affordable(limit)
        result = self._post(payload, count_as_search=True)
        result = _normalize_init_stub(result)

        # Some runs return a queued handle rather than rows. Re-fetching by
        # request_id is a continuation of the same paid search, so it is not
        # counted as a new $0.50 init.
        attempts = 0
        while (
            result.get("status") in ("pending", "queued", "running")
            and not result.get("data")
            and result.get("request_id")
            and attempts < self.poll_max_attempts
        ):
            attempts += 1
            time.sleep(self.poll_sleep)
            result = self._post(
                {"request_id": result["request_id"], "page": 1}, count_as_search=False
            )
            result = _normalize_init_stub(result)

        rows = result.get("data") or []
        self.results_returned += len(rows)
        return result

    def fetch_page(self, request_id: str, page: int) -> dict[str, Any]:
        """Fetch a further page of an existing search (no new init charge)."""
        result = self._post({"request_id": request_id, "page": page}, count_as_search=False)
        self.results_returned += len(result.get("data") or [])
        return result

    def _post(self, payload: dict[str, Any], count_as_search: bool) -> dict[str, Any]:
        attempts = max(3, len(self.pool.keys) + 1)
        for attempt in range(attempts):
            token = self.pool.active
            if token is None:
                raise SalesNavBudgetExhausted("All Apify tokens are exhausted or invalid")

            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            try:
                resp = self.session.post(
                    API_URL, json=payload, headers=headers, timeout=self.request_timeout
                )
                self.pool.record_call(token)
                if count_as_search:
                    self.searches_made += 1
                    count_as_search = False  # a retry is not a second search

                if resp.status_code in self.KEY_DEAD_CODES:
                    self.pool.retire(token, f"HTTP {resp.status_code}")
                    continue

                if resp.status_code == 402:
                    self.pool.retire(token, "insufficient prepaid balance")
                    continue

                if resp.status_code == 429:
                    detail = (resp.text or "")[:200].lower()
                    if any(w in detail for w in ("usage", "limit", "exceeded", "quota")):
                        self.pool.retire(token, "usage limit exhausted")
                        continue
                    if self.pool.active_count > 1:
                        self.pool.advance()
                        continue
                    wait = 5 * (attempt + 1)
                    log.warning("SalesNav rate-limited; waiting %ds", wait)
                    time.sleep(wait)
                    continue

                resp.raise_for_status()
                time.sleep(self.sleep_between)
                return _coerce(resp.json())

            except SalesNavBudgetExhausted:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "SalesNav request failed (attempt %d): %s", attempt + 1, exc
                )
                time.sleep(2 * (attempt + 1))

        self.failed_calls += 1
        return {"data": [], "status": "failed", "request_id": None}

    # -- reporting -----------------------------------------------------------

    def token_usage(self) -> dict[str, Any]:
        usage = dict(self.pool.usage())
        usage.update(
            searches_made=self.searches_made,
            results_returned=self.results_returned,
            spend_usd=self.spend_usd,
        )
        return usage

    def close(self) -> None:
        self.pool.close()


def _coerce(body: Any) -> dict[str, Any]:
    """Normalise the actor's reply shape.

    run-sync-get-dataset-items returns the dataset items directly (a list), but
    this actor's own documented output nests them under `data` alongside a
    request_id/status. Accept both rather than depending on which one a given
    run produces.
    """
    if isinstance(body, list):
        if len(body) == 1 and isinstance(body[0], dict) and "data" in body[0]:
            return _coerce(body[0])
        return {"data": body, "status": "success", "request_id": None}
    if isinstance(body, dict):
        data = body.get("data")
        return {
            "data": data if isinstance(data, list) else [],
            "status": body.get("status") or "success",
            "request_id": body.get("request_id"),
            "message": body.get("message"),
        }
    return {"data": [], "status": "unknown", "request_id": None}


def _normalize_init_stub(result: dict[str, Any]) -> dict[str, Any]:
    """Detect the actor's "search initialized, check back in 5-10 min" reply.

    This actor answers the sync run-sync-get-dataset-items endpoint with a
    single dataset item like {"request_id": ..., "message": "Successfully
    initialized..."} and top-level status "success" — not the "pending"/
    request_id-at-top-level shape the polling loop above checks for. Left
    undetected, that one stub row is treated as the (empty) final result and
    silently discarded downstream as an unmatched non-person record, so every
    search looked like it ran but never actually fetched people.
    """
    rows = result.get("data") or []
    if (
        len(rows) == 1
        and isinstance(rows[0], dict)
        and "request_id" in rows[0]
        and "message" in rows[0]
        and not any(k in rows[0] for k in ("full_name", "name", "current_title", "company"))
    ):
        return {
            "data": [],
            "status": "pending",
            "request_id": rows[0]["request_id"],
            "message": rows[0]["message"],
        }
    return result


def canonical_payload(payload: dict[str, Any]) -> str:
    """Stable JSON for the search_key content hash."""
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
