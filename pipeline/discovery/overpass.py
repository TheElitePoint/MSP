"""OpenStreetMap Overpass discovery — the free 'maps' layer.

Overpass is the queryable OSM database behind most map tooling. It gives us
geo-located business records (name, website, phone, address) with no API key
and no cost. Coverage of small US businesses is thinner than Google Places,
so this is one input among several, not the only one.

Tags queried per state:
    office=it                 IT companies / MSPs
    office=telecommunication  network and telecom operators
    office=company + it       generic companies self-tagged as IT
"""

from __future__ import annotations

import logging
import time
from typing import Any, Iterator

import requests

log = logging.getLogger("pipeline.overpass")

# Overpass QL. `{iso}` is an ISO3166-2 code such as US-TX.
QUERY_TEMPLATE = """
[out:json][timeout:{timeout}];
area["ISO3166-2"="{iso}"][admin_level=4]->.state;
(
  nwr["office"="it"](area.state);
  nwr["office"="telecommunication"](area.state);
  nwr["office"="company"]["company"~"it|computer|technology|telecom",i](area.state);
  nwr["craft"="computer"]["name"~"managed|network|technolog",i](area.state);
);
out center tags;
"""


class OverpassProvider:
    source_name = "osm_overpass"

    def __init__(self, cfg: Any, session: requests.Session | None = None) -> None:
        node = cfg.get("discovery.overpass", {}) or {}
        self.endpoints: list[str] = node.get("endpoints") or [
            "https://overpass-api.de/api/interpreter"
        ]
        self.timeout: int = int(node.get("timeout_seconds", 180))
        self.sleep_between: float = float(node.get("sleep_between_states", 8))
        self.max_retries: int = int(node.get("max_retries", 3))
        self.session = session or requests.Session()
        self.calls_made = 0

    def fetch_state(self, iso: str) -> list[dict[str, Any]]:
        """Run one Overpass query for a state; returns raw OSM elements."""
        query = QUERY_TEMPLATE.format(iso=iso, timeout=self.timeout)
        last_error: Exception | None = None

        for attempt in range(self.max_retries):
            endpoint = self.endpoints[attempt % len(self.endpoints)]
            try:
                log.debug("Overpass %s attempt %d -> %s", iso, attempt + 1, endpoint)
                resp = self.session.post(
                    endpoint,
                    data={"data": query},
                    timeout=self.timeout + 30,
                    headers={"User-Agent": "ManageITServicesResearchBot/1.0"},
                )
                self.calls_made += 1
                if resp.status_code == 429 or resp.status_code == 504:
                    wait = 30 * (attempt + 1)
                    log.warning("Overpass rate-limited (%s); waiting %ds", resp.status_code, wait)
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                elements = resp.json().get("elements", [])
                log.info("Overpass %s -> %d raw elements", iso, len(elements))
                return elements
            except Exception as exc:  # noqa: BLE001 - provider must never kill the run
                last_error = exc
                log.warning("Overpass %s attempt %d failed: %s", iso, attempt + 1, exc)
                time.sleep(10 * (attempt + 1))

        log.error("Overpass %s failed after %d attempts: %s", iso, self.max_retries, last_error)
        return []

    def candidates(self, state: dict[str, Any]) -> Iterator[dict[str, Any]]:
        """Yield normalised company candidates for a state."""
        elements = self.fetch_state(state["iso"])
        for el in elements:
            tags = el.get("tags") or {}
            name = (tags.get("name") or "").strip()
            website = (
                tags.get("website")
                or tags.get("contact:website")
                or tags.get("url")
                or ""
            ).strip()
            if not name:
                continue
            # A record with neither website nor phone cannot be validated later.
            phone = (tags.get("phone") or tags.get("contact:phone") or "").strip()
            if not website and not phone:
                continue

            city = (
                tags.get("addr:city")
                or tags.get("addr:town")
                or tags.get("addr:village")
                or ""
            ).strip()

            yield {
                "company_name": name,
                "website": website or None,
                "phone": phone or None,
                "headquarters_city": city or None,
                "headquarters_state": state["code"],
                "country": "United States",
                "segment": "Managed technology provider",
                "discovery_source": f"OSM Overpass ({state['code']})",
                "discovery_query": f"office=it|telecommunication in {state['iso']}",
                "status": "raw",
                "_pipeline": "overpass",
                "_evidence_url": f"https://www.openstreetmap.org/{el.get('type')}/{el.get('id')}",
                "_evidence_level": 2,
            }

        time.sleep(self.sleep_between)
