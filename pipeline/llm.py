"""Groq LLM client — key rotation, content-hash caching and a run budget.

Used only where an LLM genuinely beats the deterministic code:

    entity classification   the lexicon is accurate but brittle; every new
                            phrasing needs a marker. A model reading the page
                            generalises.
    name resolution         replaces a heuristic cascade with one extraction.
    fit rationale           SOP Module 63 prose, which is what models are for.
    false-positive challenge SOP Module 61 specifies this prompt verbatim.

Deliberately NOT used for dedup keys, the exclusion ledger, scoring
arithmetic, tier thresholds or geography. Those must be reproducible across
runs — the SOP requires another researcher to reach the same conclusion from
the record, and a nondeterministic classifier breaks that.

Two SOP constraints are enforced in code, not just in the prompt:

  Module 29  "AI summaries are research assistance, not proof."
  Module 59  "Never ask AI for a list of companies and copy the output
              directly into the final tracker without verification."

So every call is grounded: the model receives page text already crawled and is
instructed to answer only from it. It never generates companies, headcounts,
customers or tools. Anything it returns is stored alongside the deterministic
verdict rather than replacing it, and disagreements are surfaced for review.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sqlite3
import time
from pathlib import Path
from typing import Any

import requests

log = logging.getLogger("pipeline.llm")

API_URL = "https://api.groq.com/openai/v1/chat/completions"

# Model roles. Groq hosts open-weights models; these are chosen per task by
# capability/cost. Override in settings.yaml under `llm.models`.
DEFAULT_MODELS = {
    # Groq retired the Llama chat models (llama-3.1-8b-instant /
    # llama-3.3-70b-versatile now 404 on /chat/completions). These are the
    # current general-purpose replacements. Both return reasoning in a separate
    # `message.reasoning` field, so `message.content` stays clean JSON under
    # response_format=json_object — verified live before switching.
    "fast": "openai/gpt-oss-20b",           # high-volume: names, summaries
    "reasoning": "openai/gpt-oss-120b",     # classification, challenge
}


class LLMBudgetExhausted(RuntimeError):
    """Raised when the per-run call budget or every key is spent."""


class GroqClient:
    """Chat-completions client with a pooled key rotation and disk cache."""

    # Codes meaning "this key is finished", not "this request failed".
    KEY_DEAD_CODES = {401, 403}

    def __init__(self, cfg: Any, cache_path: Path | None = None) -> None:
        node = cfg.get("llm", {}) or {}
        self.enabled = bool(node.get("enabled", True))
        self.keys = _load_keys()
        self.key_index = 0
        self.dead_keys: set[str] = set()

        self.models = {**DEFAULT_MODELS, **(node.get("models") or {})}
        self.max_calls = int(node.get("max_calls_per_run", 2000))
        self.timeout = int(node.get("timeout_seconds", 45))
        self.sleep = float(node.get("sleep_between_calls", 0.2))
        self.max_input_chars = int(node.get("max_input_chars", 12000))
        self.temperature = float(node.get("temperature", 0.0))
        # gpt-oss models spend part of `max_tokens` on a hidden reasoning pass
        # before writing the JSON content. At the default/unset effort, that
        # pass alone measured 600-700+ tokens on a plain-prose rationale task,
        # so every one of our 350-900 token budgets was consumed by reasoning
        # with nothing left for content -> Groq returns 400 json_validate_failed
        # (not a truncation warning) instead of a usable response. "low" cut
        # reasoning to ~15-20 tokens with no observed quality loss on these
        # classification/short-JSON tasks. Verified live before shipping.
        self.reasoning_effort = str(node.get("reasoning_effort", "low"))

        self.calls_made = 0
        self.cache_hits = 0
        self.failures = 0
        self.session = requests.Session()

        self._cache: sqlite3.Connection | None = None
        if cache_path is not None:
            self._open_cache(cache_path)

    # -- cache ---------------------------------------------------------------

    def _open_cache(self, path: Path) -> None:
        """Content-hash cache so re-runs never re-spend on unchanged pages."""
        path.parent.mkdir(parents=True, exist_ok=True)
        self._cache = sqlite3.connect(path, timeout=30, check_same_thread=False)
        self._cache.execute(
            "CREATE TABLE IF NOT EXISTS llm_cache ("
            " key TEXT PRIMARY KEY, model TEXT, response TEXT, created_at TEXT)"
        )
        self._cache.commit()

    @staticmethod
    def _cache_key(model: str, system: str, user: str) -> str:
        digest = hashlib.sha256(f"{model}\x00{system}\x00{user}".encode()).hexdigest()
        return digest

    def _cache_get(self, key: str) -> str | None:
        if self._cache is None:
            return None
        row = self._cache.execute(
            "SELECT response FROM llm_cache WHERE key=?", (key,)
        ).fetchone()
        return row[0] if row else None

    def _cache_put(self, key: str, model: str, response: str) -> None:
        if self._cache is None:
            return
        self._cache.execute(
            "INSERT OR REPLACE INTO llm_cache(key, model, response, created_at)"
            " VALUES (?,?,?,datetime('now'))",
            (key, model, response),
        )
        self._cache.commit()

    # -- keys ----------------------------------------------------------------

    @property
    def available(self) -> bool:
        return self.enabled and bool(self.keys) and len(self.dead_keys) < len(self.keys)

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
        self.key_index = (self.key_index + 1) % max(1, len(self.keys))
        log.warning(
            "Groq key ...%s retired (%s). %d of %d keys still active.",
            key[-6:], why, len(self.keys) - len(self.dead_keys), len(self.keys),
        )

    # -- calls ---------------------------------------------------------------

    def complete(
        self,
        system: str,
        user: str,
        role: str = "fast",
        json_mode: bool = True,
        max_tokens: int = 700,
    ) -> str | None:
        """One chat completion. Returns the raw content, or None on failure."""
        if not self.available:
            return None

        model = self.models.get(role, self.models["fast"])
        user = user[: self.max_input_chars]
        cache_key = self._cache_key(model, system, user)

        cached = self._cache_get(cache_key)
        if cached is not None:
            self.cache_hits += 1
            return cached

        if self.calls_made >= self.max_calls:
            raise LLMBudgetExhausted(
                f"Groq budget of {self.max_calls} calls exhausted for this run"
            )

        payload: dict[str, Any] = {
            "model": model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "temperature": self.temperature,
            "max_tokens": max_tokens,
        }
        if json_mode:
            payload["response_format"] = {"type": "json_object"}
        if model.startswith("openai/gpt-oss") and self.reasoning_effort:
            payload["reasoning_effort"] = self.reasoning_effort

        attempts = max(3, len(self.keys) + 1)
        for attempt in range(attempts):
            key = self.active_key
            if key is None:
                raise LLMBudgetExhausted("All Groq API keys are exhausted or invalid")
            try:
                resp = self.session.post(
                    API_URL,
                    json=payload,
                    headers={
                        "Authorization": f"Bearer {key}",
                        "Content-Type": "application/json",
                    },
                    timeout=self.timeout,
                )
                self.calls_made += 1

                if resp.status_code in self.KEY_DEAD_CODES:
                    self._retire_key(key, f"HTTP {resp.status_code}")
                    continue
                if resp.status_code == 429:
                    detail = (resp.text or "")[:200].lower()
                    if "quota" in detail or "daily" in detail or "tokens per day" in detail:
                        self._retire_key(key, "quota exhausted")
                        continue
                    if len(self.keys) - len(self.dead_keys) > 1:
                        self.key_index = (self.key_index + 1) % len(self.keys)
                        continue
                    wait = 4 * (attempt + 1)
                    log.warning("Groq rate-limited; waiting %ds", wait)
                    time.sleep(wait)
                    continue
                if resp.status_code >= 500:
                    time.sleep(2 * (attempt + 1))
                    continue
                if resp.status_code == 400:
                    # A persistent client-side error (bad payload/prompt) will
                    # never succeed on retry with the same body, and the prior
                    # behaviour of swallowing this into a bare exception string
                    # is what let the json_validate_failed bug above run silent
                    # for two hours. Log the body once and stop burning the
                    # attempt budget on it.
                    log.warning("Groq 400, giving up on this call: %s", (resp.text or "")[:300])
                    self.failures += 1
                    return None

                resp.raise_for_status()
                content = (
                    resp.json().get("choices", [{}])[0].get("message", {}).get("content")
                )
                if content:
                    self._cache_put(cache_key, model, content)
                    time.sleep(self.sleep)
                    return content
                return None

            except LLMBudgetExhausted:
                raise
            except Exception as exc:  # noqa: BLE001
                log.debug("Groq call failed (attempt %d): %s", attempt + 1, exc)
                time.sleep(2 * (attempt + 1))

        self.failures += 1
        return None

    def complete_json(self, system: str, user: str, role: str = "fast",
                      max_tokens: int = 700) -> dict[str, Any] | None:
        """Chat completion parsed as JSON, tolerating fenced output."""
        raw = self.complete(system, user, role=role, json_mode=True, max_tokens=max_tokens)
        if not raw:
            return None
        return parse_json(raw)

    def usage(self) -> dict[str, Any]:
        return {
            "keys_total": len(self.keys),
            "keys_active": len(self.keys) - len(self.dead_keys),
            "calls_made": self.calls_made,
            "cache_hits": self.cache_hits,
            "failures": self.failures,
        }


# --------------------------------------------------------------------------


def _load_keys() -> list[str]:
    """GROQ_API_KEYS (comma-separated), GROQ_API_KEY, and GROQ_API_KEY_1..N."""
    keys: list[str] = []
    multi = os.getenv("GROQ_API_KEYS", "")
    if multi:
        keys += [k.strip() for k in multi.split(",")]
    single = os.getenv("GROQ_API_KEY", "")
    if single:
        keys.append(single.strip())
    for i in range(1, 21):
        numbered = os.getenv(f"GROQ_API_KEY_{i}", "")
        if numbered:
            keys.append(numbered.strip())

    seen: list[str] = []
    for key in keys:
        if key and key not in seen and key.startswith("gsk_"):
            seen.append(key)
    return seen


_FENCE = re.compile(r"^\s*```(?:json)?\s*|\s*```\s*$", re.IGNORECASE)


def parse_json(raw: str) -> dict[str, Any] | None:
    """Parse model output as JSON, tolerating code fences and trailing prose."""
    if not raw:
        return None
    text = _FENCE.sub("", raw.strip())
    try:
        value = json.loads(text)
        return value if isinstance(value, dict) else {"value": value}
    except json.JSONDecodeError:
        pass
    # Fall back to the first balanced {...} block.
    start = text.find("{")
    if start == -1:
        return None
    depth = 0
    for index in range(start, len(text)):
        if text[index] == "{":
            depth += 1
        elif text[index] == "}":
            depth -= 1
            if depth == 0:
                try:
                    value = json.loads(text[start:index + 1])
                    return value if isinstance(value, dict) else None
                except json.JSONDecodeError:
                    return None
    return None
