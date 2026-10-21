"""Batch planning and person-to-company attribution.

The Sales Navigator actor charges $0.50 per search regardless of how many
companies that search covers, and accepts up to 10 companies at once — so
batching is the entire cost story. 210 companies cost ~$27 batched and ~$113
one at a time.

The price of batching is ambiguity: a search for ten company names returns a
flat list of people, each tagged only with the LinkedIn company name the actor
saw. Attributing those people back to the right row is the hard part, and
getting it wrong is worse than getting nothing — a decision-maker filed under
the wrong account is a bad call to a real person.

Two defences, in this order:
  1. plan_batches never puts two similarly-named companies in the same batch,
     so the ambiguous case mostly cannot arise. The live database already has
     five name collisions inside the qualified set, so this is not theoretical.
  2. attribute_people refuses to guess: a fuzzy match must both clear a high
     ratio and beat the runner-up by a margin, or the person is dropped.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from rapidfuzz import fuzz

from ..dedup import name_key
from ..dedup import profile_key as person_key
from .targeting import MAX_COMPANIES_PER_SEARCH
from .targeting import profile_key as batch_key

log = logging.getLogger("pipeline.contacts")

# Two companies whose normalised names are at least this similar never share a
# batch. Lower than the acceptance threshold on purpose: it is cheap to split a
# batch and expensive to mis-attribute a person.
BATCH_CONFLICT_RATIO = 90.0


@dataclass
class Batch:
    """One paid search covering up to 10 companies that share a targeting band."""

    band: str
    rows: list[Any] = field(default_factory=list)

    @property
    def company_ids(self) -> list[int]:
        return [int(r["id"]) for r in self.rows]

    @property
    def names(self) -> list[str]:
        return [str(r["company_name"]) for r in self.rows]

    @property
    def linkedin_ids(self) -> list[int]:
        out = []
        for row in self.rows:
            value = _get(row, "linkedin_company_id")
            if value:
                try:
                    out.append(int(value))
                except (TypeError, ValueError):
                    continue
        return out

    @property
    def all_have_linkedin_ids(self) -> bool:
        return len(self.linkedin_ids) == len(self.rows) and bool(self.rows)


def plan_batches(rows: list[Any], cfg: Any, batch_size: int | None = None) -> list[Batch]:
    """Group companies into paid searches.

    Grouped by targeting band first, because title_keywords/seniority/functions
    are per-SEARCH: a batch spanning two bands would apply one band's filter to
    the other band's companies. Within a band, similarly-named companies are
    pushed into different batches so attribution stays unambiguous.
    """
    size = int(batch_size or cfg.get("contacts.batch_size", MAX_COMPANIES_PER_SEARCH))
    size = max(1, min(size, MAX_COMPANIES_PER_SEARCH))

    buckets: dict[str, list[Any]] = {}
    for row in rows:
        buckets.setdefault(batch_key(row, cfg), []).append(row)

    batches: list[Batch] = []
    for band, members in buckets.items():
        open_batches: list[Batch] = []
        for row in members:
            placed = False
            for batch in open_batches:
                if len(batch.rows) >= size:
                    continue
                if _conflicts(row, batch):
                    continue
                batch.rows.append(row)
                placed = True
                break
            if not placed:
                open_batches.append(Batch(band=band, rows=[row]))
        batches.extend(open_batches)

    return batches


def attribute_people(
    items: list[dict[str, Any]],
    batch: Batch,
    cfg: Any,
) -> tuple[dict[int, list[dict[str, Any]]], list[dict[str, Any]]]:
    """Map returned people onto the companies in this batch.

    Returns ({company_id: [person, ...]}, unattributed). Attribution cascades
    most-reliable first and stops rather than guessing:

      1. LinkedIn organisation id — exact, and self-bootstrapping: once a
         company has one stored, later runs filter by it and this whole class
         of ambiguity disappears.
      2. Exact normalised name, compared only against the <=10 names in this
         batch — never against the whole database.
      3. Fuzzy, but only when the best candidate clears `min_ratio` AND beats
         the runner-up by `min_margin`. The margin is the important half: if
         two batch members both look like a match, neither wins.
      4. Otherwise unattributed. The person still cost $0.01, so the caller
         logs them for tuning rather than silently dropping them.
    """
    min_ratio = float(cfg.get("contacts.company_match_min_ratio", 93))
    min_margin = float(cfg.get("contacts.company_match_min_margin", 8))

    by_li_id: dict[str, int] = {}
    by_name: dict[str, int] = {}
    keys: list[tuple[str, int]] = []
    for row in batch.rows:
        cid = int(row["id"])
        li_id = _get(row, "linkedin_company_id")
        if li_id:
            by_li_id[str(li_id)] = cid
        key = name_key(str(row["company_name"]))
        if key:
            by_name.setdefault(key, cid)
            keys.append((key, cid))

    attributed: dict[int, list[dict[str, Any]]] = {}
    unattributed: list[dict[str, Any]] = []

    for item in items:
        person = _normalise(item)
        li_company_id = person.get("li_company_id")
        li_company_name = person.get("li_company_name") or ""

        cid = None
        method = None
        confidence = 0.0

        if li_company_id and str(li_company_id) in by_li_id:
            cid = by_li_id[str(li_company_id)]
            method, confidence = "li_company_id", 1.0
        else:
            candidate_key = name_key(li_company_name)
            if candidate_key and candidate_key in by_name:
                cid = by_name[candidate_key]
                method, confidence = "exact_name", 1.0
            elif candidate_key and keys:
                scored = sorted(
                    ((float(fuzz.ratio(candidate_key, key)), key_cid)
                     for key, key_cid in keys),
                    key=lambda pair: pair[0],
                    reverse=True,
                )
                best, best_cid = scored[0]
                runner_up = scored[1][0] if len(scored) > 1 else 0.0
                if best >= min_ratio and (best - runner_up) >= min_margin:
                    cid, method, confidence = best_cid, "fuzzy_name", best / 100.0

        if cid is None:
            unattributed.append(person)
            continue

        person["match_method"] = method
        person["match_confidence"] = round(confidence, 3)
        attributed.setdefault(cid, []).append(person)

    return attributed, unattributed


# -- helpers ----------------------------------------------------------------


def _conflicts(row: Any, batch: Batch) -> bool:
    """True when `row`'s name is too close to something already in `batch`."""
    key = name_key(str(row["company_name"]))
    if not key:
        return False
    for member in batch.rows:
        other = name_key(str(member["company_name"]))
        if other and float(fuzz.ratio(key, other)) >= BATCH_CONFLICT_RATIO:
            return True
    return False


def _normalise(item: dict[str, Any]) -> dict[str, Any]:
    """Actor result -> the field names the contacts table uses."""
    full_name = (item.get("full_name") or "").strip()
    first = (item.get("first_name") or "").strip()
    last = (item.get("last_name") or "").strip()
    if not full_name:
        full_name = " ".join(p for p in (first, last) if p).strip()

    linkedin_url = (item.get("linkedin_url") or "").strip() or None
    profile_id = (item.get("profile_id") or "").strip() or None
    job_title = (item.get("job_title") or "").strip() or None

    return {
        "full_name": full_name or None,
        "first_name": first or None,
        "last_name": last or None,
        "job_title": job_title,
        "linkedin_url": linkedin_url,
        "profile_id": profile_id,
        "li_company_id": (str(item.get("company_id")).strip() or None)
        if item.get("company_id") is not None else None,
        "li_company_name": (item.get("company") or "").strip() or None,
        "location": (item.get("location") or "").strip() or None,
        "about": (item.get("about") or "").strip()[:2000] or None,
        "profile_key": person_key(linkedin_url, profile_id, full_name, job_title),
    }


def _get(row: Any, name: str) -> Any:
    if isinstance(row, dict):
        return row.get(name)
    try:
        return row[name]
    except (IndexError, KeyError):
        return None
