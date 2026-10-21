"""Intent pipeline registry.

SOP Module 13 defines two discovery routes — direct and reverse-engineered —
that both feed *the same* validation, scoring and mapping process, explicitly
"to prevent inconsistent standards across different sources". This module
generalises that to N pipelines converging on one funnel:

    overpass  ─┐
    tavily    ─┤
    ats_jobs  ─┼─→ [ dedup store ] → enrich → intent → qualify → score → clean → export
    partners  ─┤
    awards    ─┘

Convergence is already safe: the dedup store's four keys plus the alias table
mean the same company arriving from five pipelines merges instead of
duplicating, checkpoints are per-(run_id, stage, key) so each pipeline resumes
independently, and every provider carries its own budget.

Each pipeline records `source_pipeline` on the companies it contributes, so the
run report can show qualified-account rate *by pipeline* — the SOP Module 69
weekly question, "which discovery sources produced the best qualified-account
rate".
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator

log = logging.getLogger("pipeline.registry")


@dataclass
class PipelineSpec:
    """One discovery or signal pipeline."""

    name: str
    kind: str                      # "discovery" | "signal"
    description: str
    enabled: bool = True
    # discovery pipelines yield candidate dicts for a state
    candidates: Callable[[dict[str, Any]], Iterator[dict[str, Any]]] | None = None
    # signal pipelines enrich companies already in the store
    run_signals: Callable[[], dict[str, Any]] | None = None
    stats: dict[str, Any] = field(default_factory=dict)


class PipelineRegistry:
    """Holds every pipeline and reports their individual yield."""

    def __init__(self) -> None:
        self._pipelines: dict[str, PipelineSpec] = {}

    def register(self, spec: PipelineSpec) -> None:
        self._pipelines[spec.name] = spec

    def get(self, name: str) -> PipelineSpec | None:
        return self._pipelines.get(name)

    @property
    def all(self) -> list[PipelineSpec]:
        return list(self._pipelines.values())

    def enabled(self, kind: str | None = None) -> list[PipelineSpec]:
        return [
            p for p in self._pipelines.values()
            if p.enabled and (kind is None or p.kind == kind)
        ]

    def names(self, kind: str | None = None) -> list[str]:
        return [p.name for p in self.enabled(kind)]

    def record(self, name: str, **stats: Any) -> None:
        spec = self._pipelines.get(name)
        if not spec:
            return
        for key, value in stats.items():
            spec.stats[key] = spec.stats.get(key, 0) + value

    def summary(self) -> dict[str, dict[str, Any]]:
        return {
            p.name: {"kind": p.kind, "enabled": p.enabled, **p.stats}
            for p in self._pipelines.values()
        }


def yield_by_pipeline(store: Any) -> list[dict[str, Any]]:
    """Qualified-account rate per pipeline (SOP Module 69).

    Answers which discovery routes are worth more effort next week.
    """
    rows = store.conn.execute(
        """
        SELECT COALESCE(source_pipeline, 'unknown') AS pipeline,
               COUNT(*)                                        AS discovered,
               SUM(CASE WHEN status='approved' THEN 1 ELSE 0 END) AS approved,
               SUM(CASE WHEN status='review'   THEN 1 ELSE 0 END) AS review,
               SUM(CASE WHEN status='excluded' THEN 1 ELSE 0 END) AS excluded,
               SUM(CASE WHEN tier='A' THEN 1 ELSE 0 END)          AS tier_a,
               ROUND(AVG(COALESCE(score_total,0)), 1)             AS avg_score,
               ROUND(AVG(COALESCE(intent_score,0)), 1)            AS avg_intent
        FROM companies
        GROUP BY pipeline
        ORDER BY approved DESC, discovered DESC
        """
    ).fetchall()

    out = []
    for row in rows:
        discovered = row["discovered"] or 0
        approved = row["approved"] or 0
        out.append({
            "pipeline": row["pipeline"],
            "discovered": discovered,
            "approved": approved,
            "review": row["review"] or 0,
            "excluded": row["excluded"] or 0,
            "tier_a": row["tier_a"] or 0,
            "qualified_rate_pct": round(100.0 * approved / discovered, 1) if discovered else 0.0,
            "avg_score": row["avg_score"] or 0.0,
            "avg_intent": row["avg_intent"] or 0.0,
        })
    return out


def yield_by_query_group(store: Any) -> list[dict[str, Any]]:
    """Qualified accounts per API credit, by queries.yaml group (SOP Module 34:
    "record which filters produced the strongest qualified-account rate";
    Module 69: "sources and queries producing the best qualified-account rate").

    Finer-grained than yield_by_pipeline — every query group here shares the
    same "tavily" pipeline, but a hiring-signal sweep and a generic
    "managed IT services" sweep can have very different qualified rates per
    credit spent, and this is the only place that distinction is visible.
    """
    company_rows = store.conn.execute(
        """
        SELECT discovery_group AS group_name,
               COUNT(*)                                           AS discovered,
               SUM(CASE WHEN status='approved' THEN 1 ELSE 0 END) AS approved,
               SUM(CASE WHEN tier='A' THEN 1 ELSE 0 END)          AS tier_a
        FROM companies
        WHERE discovery_group IS NOT NULL
        GROUP BY discovery_group
        """
    ).fetchall()
    calls_by_group = dict(
        store.conn.execute(
            "SELECT group_name, COUNT(*) FROM query_log"
            " WHERE group_name IS NOT NULL GROUP BY group_name"
        ).fetchall()
    )

    out = []
    for row in company_rows:
        discovered = row["discovered"] or 0
        approved = row["approved"] or 0
        calls = calls_by_group.get(row["group_name"], 0)
        out.append({
            "group": row["group_name"],
            "discovered": discovered,
            "approved": approved,
            "tier_a": row["tier_a"] or 0,
            "calls_spent": calls,
            "qualified_rate_pct": round(100.0 * approved / discovered, 1) if discovered else 0.0,
            "approved_per_call": round(approved / calls, 3) if calls else 0.0,
        })
    out.sort(key=lambda r: -r["approved_per_call"])
    return out
