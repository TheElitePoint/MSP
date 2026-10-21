"""Configuration loading and shared paths."""

from __future__ import annotations

import logging
import os
from datetime import datetime
import sys
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = ROOT / "config"
DATA_DIR = ROOT / "data"
EXPORT_DIR = DATA_DIR / "exports"
LOG_DIR = ROOT / "logs"
DB_PATH = DATA_DIR / "companies.db"

load_dotenv(ROOT / ".env")


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Missing config file: {path}")
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


class Config:
    """Merged view of settings.yaml, geography.yaml, queries.yaml and
    decision_makers.yaml."""

    def __init__(self) -> None:
        self.settings = _read_yaml(CONFIG_DIR / "settings.yaml")
        self.geography = _read_yaml(CONFIG_DIR / "geography.yaml")
        self.queries = _read_yaml(CONFIG_DIR / "queries.yaml")
        self.decision_makers = _read_yaml(CONFIG_DIR / "decision_makers.yaml")

    def get(self, dotted: str, default: Any = None) -> Any:
        """Fetch a nested setting, e.g. cfg.get('discovery.tavily.max_calls_per_run')."""
        node: Any = self.settings
        for part in dotted.split("."):
            if not isinstance(node, dict) or part not in node:
                return default
            node = node[part]
        return node

    # -- geography -----------------------------------------------------------

    @property
    def all_states(self) -> list[dict[str, Any]]:
        return self.geography.get("states", [])

    def selected_states(self) -> list[dict[str, Any]]:
        wanted = self.get("discovery.states", "all")
        if wanted in ("all", None, ""):
            return self.all_states
        if isinstance(wanted, str):
            wanted = [wanted]
        codes = {str(c).upper() for c in wanted}
        return [s for s in self.all_states if str(s["code"]).upper() in codes]

    # -- queries -------------------------------------------------------------

    @property
    def query_groups(self) -> dict[str, Any]:
        groups = self.queries.get("groups", {})
        active = self.get("discovery.tavily.active_query_groups") or []
        if active:
            return {k: v for k, v in groups.items() if k in active}
        return groups

    @property
    def aggregator_domains(self) -> set[str]:
        return {d.lower().strip() for d in self.queries.get("aggregator_domains", [])}

    @property
    def enterprise_domains(self) -> set[str]:
        """Global enterprises and big tech — never a first-pilot account."""
        return {d.lower().strip() for d in self.queries.get("enterprise_domains", [])}

    # -- decision-maker targeting (SOP Modules 41/42/44) ---------------------

    @property
    def dm_bands(self) -> dict[str, Any]:
        return self.decision_makers.get("bands", {}) or {}

    @property
    def dm_exclusions(self) -> dict[str, Any]:
        return self.decision_makers.get("exclude", {}) or {}

    @property
    def dm_headcount_filter(self) -> dict[str, Any]:
        return self.decision_makers.get("headcount_filter", {}) or {}

    # -- secrets -------------------------------------------------------------

    @property
    def tavily_api_keys(self) -> list[str]:
        """All configured Tavily keys, pooled for rotation.

        Accepts TAVILY_API_KEYS (comma-separated) and/or the single-key
        TAVILY_API_KEY, plus numbered TAVILY_API_KEY_1..N.
        """
        keys: list[str] = []
        multi = os.getenv("TAVILY_API_KEYS", "")
        if multi:
            keys += [k.strip() for k in multi.split(",")]
        single = os.getenv("TAVILY_API_KEY", "")
        if single:
            keys.append(single.strip())
        for i in range(1, 21):
            numbered = os.getenv(f"TAVILY_API_KEY_{i}", "")
            if numbered:
                keys.append(numbered.strip())

        seen: list[str] = []
        for key in keys:
            if key and key not in seen and not key.startswith("tvly-xxx"):
                seen.append(key)
        return seen

    @property
    def tavily_api_key(self) -> str | None:
        keys = self.tavily_api_keys
        return keys[0] if keys else None

    @property
    def apify_tokens(self) -> list[str]:
        """All configured Apify tokens, pooled for rotation.

        Accepts APIFY_TOKENS (comma-separated) and/or the single-token
        APIFY_TOKEN, plus numbered APIFY_TOKEN_1..N.
        """
        tokens: list[str] = []
        multi = os.getenv("APIFY_TOKENS", "")
        if multi:
            tokens += [t.strip() for t in multi.split(",")]
        single = os.getenv("APIFY_TOKEN", "")
        if single:
            tokens.append(single.strip())
        for i in range(1, 21):
            numbered = os.getenv(f"APIFY_TOKEN_{i}", "")
            if numbered:
                tokens.append(numbered.strip())

        seen: list[str] = []
        for token in tokens:
            if token and token not in seen and not token.lower().startswith("apify_api_xxx"):
                seen.append(token)
        return seen


def setup_logging(run_id: str, verbose: bool = False) -> logging.Logger:
    """Console + per-run file logging."""
    log_dir = LOG_DIR / datetime.now().strftime("%Y-%m-%d")
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("pipeline")
    logger.setLevel(logging.DEBUG if verbose else logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s  %(levelname)-7s %(name)-22s %(message)s", "%H:%M:%S")

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.DEBUG if verbose else logging.INFO)
    console.setFormatter(fmt)
    logger.addHandler(console)

    fh = logging.FileHandler(log_dir / f"{run_id}.log", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s  %(levelname)-7s %(name)-22s %(message)s"))
    logger.addHandler(fh)

    # Third-party noise
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("charset_normalizer").setLevel(logging.WARNING)
    return logger


def ensure_dirs() -> None:
    for path in (DATA_DIR, EXPORT_DIR, LOG_DIR):
        path.mkdir(parents=True, exist_ok=True)
