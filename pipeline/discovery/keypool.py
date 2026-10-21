"""Rotating pool of API keys with cross-run memory of which ones are spent.

Extracted from the identical logic in tavily.py and apify.py. Those two are
left alone deliberately — they are working paid providers, and converting them
is a separate change that can be reverted on its own. New providers compose
this instead of copying it a third time.

The rule this encodes, and the reason it is not just a list: a key that hits
its quota is *skipped*, never removed. Quotas reset, so the key is retried
automatically once `retry_after_hours` has passed. What the persisted state
buys is not permanence — it is not re-probing a known-dead key at the start of
every single run.
"""

from __future__ import annotations

import hashlib
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

log = logging.getLogger("pipeline.keypool")


def key_fingerprint(key: str) -> str:
    """Stable, non-secret identifier — the key itself is never persisted."""
    return hashlib.sha256(key.encode()).hexdigest()[:24]


class TokenPool:
    """Round-robin over API keys, with sqlite-backed dead-key memory."""

    def __init__(
        self,
        keys: list[str],
        state_path: Path,
        retry_after_hours: float = 24.0,
        label: str = "provider",
    ) -> None:
        self.keys = [k.strip() for k in keys if k and k.strip()]
        if not self.keys:
            raise ValueError(f"{label}: at least one API key is required")
        self.label = label
        self.index = 0
        self.dead: set[str] = set()
        self.calls_per_key: dict[str, int] = {k: 0 for k in self.keys}
        self._retry_after_hours = float(retry_after_hours)
        self._state: sqlite3.Connection | None = None
        self._open_state(state_path)
        self._load_persisted()

    # -- persistence ---------------------------------------------------------

    def _open_state(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._state = sqlite3.connect(path, timeout=30, check_same_thread=False)
        self._state.execute(
            "CREATE TABLE IF NOT EXISTS dead_keys ("
            "key_hash TEXT PRIMARY KEY, reason TEXT, retired_at TEXT)"
        )
        self._state.commit()

    def _load_persisted(self) -> None:
        if self._state is None:
            return
        cutoff = datetime.now(timezone.utc) - timedelta(hours=self._retry_after_hours)
        reasons: list[str] = []
        for key_hash, reason, retired_at in self._state.execute(
            "SELECT key_hash, reason, retired_at FROM dead_keys"
        ).fetchall():
            try:
                retired = datetime.fromisoformat(retired_at)
            except (TypeError, ValueError):
                continue
            if retired.tzinfo is None:
                retired = retired.replace(tzinfo=timezone.utc)
            if retired < cutoff:
                continue  # old enough to deserve a retry — the quota may have reset
            for key in self.keys:
                if key_fingerprint(key) == key_hash:
                    self.dead.add(key)
                    reasons.append(reason)
        if reasons:
            log.info(
                "%s: %d of %d key(s) already known dead from a previous run (%s) — "
                "not re-probed",
                self.label, len(reasons), len(self.keys), ", ".join(sorted(set(reasons))),
            )

    def _persist(self, key: str, reason: str) -> None:
        if self._state is None:
            return
        self._state.execute(
            "INSERT OR REPLACE INTO dead_keys(key_hash, reason, retired_at) VALUES (?,?,?)",
            (key_fingerprint(key), reason,
             datetime.now(timezone.utc).isoformat(timespec="seconds")),
        )
        self._state.commit()

    # -- rotation ------------------------------------------------------------

    @property
    def active(self) -> str | None:
        for offset in range(len(self.keys)):
            key = self.keys[(self.index + offset) % len(self.keys)]
            if key not in self.dead:
                self.index = (self.index + offset) % len(self.keys)
                return key
        return None

    @property
    def active_count(self) -> int:
        return len(self.keys) - len(self.dead)

    def advance(self) -> None:
        """Move to the next key without retiring the current one."""
        self.index = (self.index + 1) % len(self.keys)

    def retire(self, key: str, why: str) -> None:
        self.dead.add(key)
        self._persist(key, why)
        self.advance()
        log.warning(
            "%s key ...%s retired (%s). %d of %d keys still active.",
            self.label, key[-6:], why, self.active_count, len(self.keys),
        )

    def record_call(self, key: str) -> None:
        self.calls_per_key[key] = self.calls_per_key.get(key, 0) + 1

    def usage(self) -> dict[str, object]:
        return {
            "keys_total": len(self.keys),
            "keys_active": self.active_count,
            "per_key": {f"...{k[-6:]}": v for k, v in self.calls_per_key.items()},
        }

    def close(self) -> None:
        if self._state is not None:
            self._state.close()
            self._state = None
