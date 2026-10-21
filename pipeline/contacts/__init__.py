"""Decision-maker mapping for approved accounts (SOP Modules 40-44).

Runs after `clean`, over companies that already passed qualification, scoring
and entity verification — SOP Module 31's work order step 4: "Map
decision-makers for approved Tier A accounts." Contacts are found via Apify's
Sales Navigator actor, restricted to the United States, classified into the
Module 41 buying group, and filtered by the Module 44 people-to-avoid rules.
"""

from .mapping import Batch, attribute_people, plan_batches
from .provider import (
    INIT_COST_USD,
    RESULT_COST_USD,
    SalesNavBudgetExhausted,
    SalesNavProvider,
    canonical_payload,
)
from .roles import classify_role, rank_contacts, rank_score, reject_reason
from .targeting import band_for, build_search_input
from .us_geo import build_us_index, classify_location

__all__ = [
    "Batch",
    "INIT_COST_USD",
    "RESULT_COST_USD",
    "SalesNavBudgetExhausted",
    "SalesNavProvider",
    "attribute_people",
    "band_for",
    "build_search_input",
    "build_us_index",
    "canonical_payload",
    "classify_location",
    "classify_role",
    "plan_batches",
    "rank_contacts",
    "rank_score",
    "reject_reason",
]
