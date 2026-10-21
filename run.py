#!/usr/bin/env python3
"""RUN SWITCH — starts every pipeline stage and produces the Excel tracker.

    ./run.py                      full run, all stages enabled in settings.yaml
    ./run.py --states TX CA NY    limit the geography for this run
    ./run.py --only discover      run a single stage
    ./run.py --skip intent        run everything except one stage
    ./run.py --resume RUN_ID      continue an interrupted run from its checkpoints
    ./run.py --export-only        rebuild the workbook from existing data
    ./run.py --dry-run            show the plan and budget, change nothing
    ./run.py --stats              print database totals and exit

Stages: discover -> apify_discover -> prescreen -> enrich -> intent -> qualify -> score -> llm -> clean -> contacts -> quality -> export

The run is idempotent. Companies already in the database are merged, not
duplicated; queries inside their cooldown are skipped; a company that already
had a paid news search recently is not searched again; excluded companies are
never researched again.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import signal as os_signal
import sys
import time
from datetime import datetime, timezone
from typing import Any

import requests

from pipeline import export as exporter
from pipeline import intent as intent_mod
from pipeline import prescreen as prescreen_mod
from pipeline import qualify as qualify_mod
from pipeline import quality as quality_mod
from pipeline import score as score_mod
from pipeline import classify, nameres
from pipeline import clean as clean_mod
from pipeline import llm_tasks
from pipeline.llm import GroqClient, LLMBudgetExhausted
from pipeline import intent_signals as intent_lex
from pipeline import signals as signal_lex
from pipeline.config import DATA_DIR, DB_PATH, Config, ensure_dirs, setup_logging
from pipeline.dedup import domain_key, is_aggregator, is_junk_name
from pipeline.discovery import ApifyProvider, OverpassProvider, TavilyProvider
from pipeline.discovery.ats import (ATSProvider, extract_titles_from_html,
                                    jobs_from_jsonld)
from pipeline.pipelines import (PipelineRegistry, PipelineSpec,
                                yield_by_pipeline, yield_by_query_group)
from pipeline.discovery.apify import ApifyBudgetExhausted
from pipeline.discovery.tavily import TavilyBudgetExhausted
from pipeline.enrich import WebsiteEnricher, summarise_industries, summarise_services
from pipeline import contacts as contacts_mod
from pipeline.contacts import SalesNavBudgetExhausted, SalesNavProvider
from pipeline import master_export
from pipeline.store import Store

STAGES = ["discover", "apify_discover", "prescreen", "enrich", "intent", "qualify",
          "score", "llm", "clean", "contacts", "quality", "export"]

_interrupted = False


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _before(timestamp: str, cutoff: float) -> bool:
    try:
        return datetime.fromisoformat(timestamp).timestamp() < cutoff
    except (TypeError, ValueError):
        return True


def _search_key(payload: dict[str, Any]) -> str:
    """Content hash of a paid search request.

    Keyed on the request itself rather than the run, so an identical batch is
    never bought twice — not even across run ids.
    """
    return hashlib.sha256(
        contacts_mod.canonical_payload(payload).encode()
    ).hexdigest()


def _handle_sigint(signum, frame):  # noqa: ANN001, ARG001
    global _interrupted
    if _interrupted:
        print("\nSecond interrupt — exiting immediately.", file=sys.stderr)
        sys.exit(130)
    _interrupted = True
    print(
        "\nInterrupt received. Finishing the current item, then stopping cleanly.\n"
        "Re-run with --resume <RUN_ID> to continue.",
        file=sys.stderr,
    )


class Pipeline:
    def __init__(self, cfg: Config, args: argparse.Namespace) -> None:
        self.cfg = cfg
        self.args = args
        self.run_id = args.resume or datetime.now().strftime("run_%Y%m%d_%H%M%S")
        self.log = setup_logging(self.run_id, args.verbose)
        self.store = Store(DB_PATH)
        self.stats: dict[str, Any] = {
            "run_id": self.run_id,
            "candidates_seen": 0,
            "companies_created": 0,
            "companies_merged": 0,
            "aggregators_skipped": 0,
            "junk_names_skipped": 0,
            "prior_exclusions_skipped": 0,
            "prescreened_out": 0,
            "enriched": 0,
            "crawl_failures": 0,
            "news_searched": 0,
            "news_search_cooldown_skipped": 0,
            "tavily_calls": 0,
            "overpass_calls": 0,
            "apify_calls": 0,
            # Decision-maker mapping (SOP Modules 40-44). Seeded here because
            # _print_summary indexes self.stats directly.
            "contact_searches": 0,
            "contact_results": 0,
            "contacts_created": 0,
            "contacts_updated": 0,
            "contacts_rejected": 0,
            "contacts_non_us": 0,
            "contact_companies_done": 0,
            "contact_companies_empty": 0,
            "contact_match_failures": 0,
            "contact_spend_usd": 0.0,
        }
        self.started = time.time()
        self.deadline = None
        limit = int(cfg.get("run.max_runtime_minutes", 0) or 0)
        if limit > 0:
            self.deadline = self.started + limit * 60

        self.session = requests.Session()
        self.overpass = OverpassProvider(cfg, self.session)
        self.tavily: TavilyProvider | None = None
        keys = cfg.tavily_api_keys
        if keys and cfg.get("discovery.tavily.enabled", True):
            self.tavily = TavilyProvider(
                cfg, keys, self.session, state_path=DATA_DIR / "tavily_key_state.db"
            )
            self.log.info("Tavily enabled with %d key(s) in the rotation pool", len(keys))
        elif cfg.get("discovery.tavily.enabled", True):
            self.log.warning(
                "No Tavily key found — Tavily discovery and news intent are disabled. "
                "Set TAVILY_API_KEYS in .env to enable them."
            )
        self.apify: ApifyProvider | None = None
        apify_tokens = cfg.apify_tokens
        if apify_tokens and cfg.get("discovery.apify.enabled", True):
            self.apify = ApifyProvider(
                cfg, apify_tokens, self.session, state_path=DATA_DIR / "apify_key_state.db"
            )
            self.log.info("Apify enabled with %d token(s) in the rotation pool", len(apify_tokens))
        elif cfg.get("discovery.apify.enabled", True):
            self.log.warning(
                "No Apify token found — Apify fresh-discovery is disabled. "
                "Set APIFY_TOKEN(S) in .env to enable it."
            )
        # Same credentials as Apify above, but its own budget and its own
        # dead-token file: this is a pay-per-event actor, so a token can fail
        # here for a billing reason while staying healthy for rag-web-browser.
        self.salesnav: SalesNavProvider | None = None
        if apify_tokens and cfg.get("contacts.enabled", True):
            self.salesnav = SalesNavProvider(
                cfg, apify_tokens, self.session, state_path=DATA_DIR / "salesnav_key_state.db"
            )
            self.log.info(
                "Sales Navigator contacts enabled (cap $%.2f / %d searches per run)",
                float(cfg.get("contacts.max_spend_usd_per_run", 30.0)),
                int(cfg.get("contacts.max_searches_per_run", 40)),
            )
        elif cfg.get("contacts.enabled", True):
            self.log.warning(
                "No Apify token found — decision-maker mapping is disabled. "
                "Set APIFY_TOKEN(S) in .env to enable it."
            )
        self.enricher = WebsiteEnricher(cfg)
        self.aggregators = cfg.aggregator_domains
        self.ats = ATSProvider(cfg, self.session)
        self.llm = GroqClient(cfg, DATA_DIR / "llm_cache.db")
        if self.llm.available:
            self.log.info(
                "Groq enabled with %d key(s); models=%s",
                len(self.llm.keys), self.llm.models,
            )
        elif cfg.get("llm.enabled", True):
            self.log.warning(
                "No Groq key found — LLM verification disabled. "
                "Set GROQ_API_KEYS in .env to enable it."
            )

        # One switch drives every pipeline; each records its own yield so the
        # run report can answer "which source produced qualified accounts?".
        self.registry = PipelineRegistry()
        self.registry.register(PipelineSpec(
            name="overpass", kind="discovery",
            description="OpenStreetMap geo-located IT companies",
            enabled=bool(cfg.get("discovery.overpass.enabled", True)),
        ))
        self.registry.register(PipelineSpec(
            name="tavily", kind="discovery",
            description="SOP query library via Tavily web search",
            enabled=bool(cfg.get("discovery.tavily.enabled", True)) and self.tavily is not None,
        ))
        self.registry.register(PipelineSpec(
            name="apify", kind="discovery",
            description="LLM-generated fresh queries via Apify rag-web-browser",
            enabled=bool(cfg.get("discovery.apify.enabled", True)) and self.apify is not None,
        ))
        self.registry.register(PipelineSpec(
            name="ats_jobs", kind="signal",
            description="Structured hiring intent from public ATS job boards",
            enabled=bool(cfg.get("discovery.ats.enabled", True)),
        ))
        self.registry.register(PipelineSpec(
            name="news", kind="signal",
            description="Business-change timing signals via Tavily news",
            enabled=bool(cfg.get("intent.news_search", True)) and self.tavily is not None,
        ))
        self.registry.register(PipelineSpec(
            name="llm_verify", kind="signal",
            description="Groq entity second-opinion, rationale and FP challenge",
            enabled=bool(cfg.get("stages.llm", True)) and self.llm.available,
        ))
        self.registry.register(PipelineSpec(
            name="site_signals", kind="signal",
            description="First-party intent from the company website",
            enabled=True,
        ))
        self.registry.register(PipelineSpec(
            name="salesnav_contacts", kind="signal",
            description="US decision-maker mapping via Apify Sales Navigator",
            enabled=bool(cfg.get("contacts.enabled", True)) and self.salesnav is not None,
        ))

    # -- helpers -----------------------------------------------------------

    def should_stop(self) -> bool:
        if _interrupted:
            return True
        if self.deadline and time.time() > self.deadline:
            self.log.warning("Run time limit reached; stopping cleanly.")
            return True
        return False

    def stage_enabled(self, stage: str) -> bool:
        if self.args.only:
            return stage in self.args.only
        if stage in (self.args.skip or []):
            return False
        return bool(self.cfg.get(f"stages.{stage}", True))

    def _weak_query_groups(self) -> set[str]:
        """Query groups whose historical qualified rate justifies a longer
        cooldown (SOP Module 34/69 learning loop). Reads accumulated data
        from prior runs — this run's own new discoveries haven't been
        qualified yet at this point in the pipeline.
        """
        min_sample = int(self.cfg.get("discovery.tavily.weak_group_min_sample", 20))
        threshold = float(self.cfg.get("discovery.tavily.weak_group_threshold_pct", 5.0))
        weak = set()
        for row in yield_by_query_group(self.store):
            if row["discovered"] >= min_sample and row["qualified_rate_pct"] < threshold:
                weak.add(row["group"])
        return weak

    # ------------------------------------------------------------------
    # STAGE 1 — DISCOVER
    # ------------------------------------------------------------------

    def stage_discover(self) -> None:
        states = self.cfg.selected_states()
        if self.args.states:
            wanted = {s.upper() for s in self.args.states}
            states = [s for s in states if s["code"].upper() in wanted]

        self.log.info("DISCOVER: %d states", len(states))
        cap = int(self.cfg.get("discovery.max_new_companies_per_state", 0) or 0)
        run_cap = int(self.cfg.get("discovery.max_new_companies_per_run", 0) or 0)
        run_created = int(self.store.conn.execute(
            "SELECT COUNT(*) FROM companies WHERE first_seen_run = ?",
            (self.run_id,),
        ).fetchone()[0])

        def run_cap_reached() -> bool:
            return bool(run_cap and run_created >= run_cap)

        cooldown = int(self.cfg.get("discovery.tavily.query_cooldown_days", 14))
        cities_per_state = int(self.cfg.get("discovery.tavily.cities_per_state", 4))
        use_overpass = bool(self.cfg.get("discovery.overpass.enabled", True))
        weak_groups = self._weak_query_groups()
        cooldown_multiplier = int(self.cfg.get("discovery.tavily.weak_group_cooldown_multiplier", 3))
        if weak_groups:
            self.log.info(
                "  Learning loop: %s below the qualified-rate threshold — "
                "cooldown x%d this run",
                ", ".join(sorted(weak_groups)), cooldown_multiplier,
            )

        for index, state in enumerate(states, start=1):
            if self.should_stop() or run_cap_reached():
                break
            checkpoint = f"discover:{state['code']}"
            if self.store.is_done(self.run_id, "discover", checkpoint):
                self.log.debug("Skipping %s (already done in this run)", state["code"])
                continue

            self.log.info(
                "[%d/%d] %s (%s)", index, len(states), state["name"], state["code"]
            )
            created_here = 0

            # --- OSM Overpass ---------------------------------------------
            if use_overpass:
                self.stats["overpass_calls"] += 1
                osm_total = 0
                osm_new = 0
                try:
                    for candidate in self.overpass.candidates(state):
                        osm_total += 1
                        if (cap and created_here >= cap) or run_cap_reached():
                            break
                        created = self._ingest(candidate)
                        if created:
                            created_here += 1
                            run_created += 1
                            osm_new += 1
                except Exception as exc:  # noqa: BLE001
                    self.log.warning("Overpass failed for %s: %s", state["code"], exc)
                self.store.log_query(
                    "osm_overpass", f"office=it {state['iso']}", state["code"],
                    osm_total, osm_new, self.run_id,
                )
                self.log.info("  OSM: %d candidates, %d new", osm_total, osm_new)

            # --- Tavily query library -------------------------------------
            if self.tavily:
                cities = (state.get("cities") or [])[:cities_per_state]
                for group_name, group in self.cfg.query_groups.items():
                    if (self.should_stop() or run_cap_reached()
                            or (cap and created_here >= cap)):
                        break
                    scope = group.get("scope", "state")
                    segment = group.get("segment", "Managed technology provider")
                    # SOP Module 13: Route A (direct, product-label search) vs
                    # Route B (reverse-engineered — hiring/event/ecosystem
                    # signals). Tagged per query group so stage_enrich can
                    # crawl the higher-signal Route B discoveries first.
                    route = group.get("route", "direct")
                    contexts = (
                        [(c, f"{state['code']}:{c}") for c in cities]
                        if scope == "city"
                        else [(None, state["code"])]
                    )

                    for city, scope_key in contexts:
                        if (self.should_stop() or run_cap_reached()
                                or (cap and created_here >= cap)):
                            break
                        for template in group.get("queries", []):
                            if (self.should_stop() or run_cap_reached()
                                    or (cap and created_here >= cap)):
                                break
                            query = (
                                template.replace("{city}", city or "")
                                .replace("{state}", state["code"])
                                .replace("{state_name}", state["name"])
                                .strip()
                            )
                            effective_cooldown = (
                                cooldown * cooldown_multiplier
                                if group_name in weak_groups else cooldown
                            )
                            if self.store.query_recently_run(
                                "tavily", query, scope_key, effective_cooldown
                            ):
                                self.log.debug("  cooldown skip: %s", query[:70])
                                continue
                            try:
                                found = 0
                                new = 0
                                for candidate in self.tavily.candidates(
                                    query, state["code"], city, segment, self.aggregators
                                ):
                                    found += 1
                                    if run_cap_reached():
                                        break
                                    candidate["discovery_route"] = route
                                    candidate["discovery_group"] = group_name
                                    if self._ingest(candidate):
                                        created_here += 1
                                        run_created += 1
                                        new += 1
                                self.store.log_query(
                                    "tavily", query, scope_key, found, new, self.run_id, group_name
                                )
                                if new:
                                    self.log.debug(
                                        "  %s -> %d new (%s)", group_name, new, query[:60]
                                    )
                            except TavilyBudgetExhausted as exc:
                                self.log.warning("%s — stopping Tavily discovery.", exc)
                                self.tavily = None
                                break
                        if self.tavily is None:
                            break
                    if self.tavily is None:
                        break

            self.store.mark_done(self.run_id, "discover", checkpoint)
            self.log.info("  %s complete: %d new companies", state["code"], created_here)

        if run_cap_reached():
            self.log.info(
                "DISCOVER: per-run new-company target reached (%d/%d)",
                run_created, run_cap,
            )

        if self.tavily:
            self.stats["tavily_calls"] = self.tavily.calls_made

    def _ingest(self, candidate: dict[str, Any]) -> bool:
        """Apply the dedup safeguards, then upsert. Returns True if newly created."""
        self.stats["candidates_seen"] += 1
        website = candidate.get("website")
        name = (candidate.get("company_name") or "").strip()

        # Guard 0 — page furniture, error pages and bot challenges are not companies.
        if is_junk_name(name):
            self.stats["junk_names_skipped"] += 1
            return False

        # Guard 1 — directories, social and news sites never become companies.
        if website and is_aggregator(website, self.aggregators):
            self.stats["aggregators_skipped"] += 1
            return False

        # Guard 2 — the permanent exclusion ledger (SOP Module 28).
        dk = domain_key(website)
        from pipeline.dedup import name_key as _name_key

        prior = self.store.is_excluded(dk, _name_key(name))
        if prior:
            self.stats["prior_exclusions_skipped"] += 1
            return False

        evidence_url = candidate.pop("_evidence_url", None)
        evidence_level = candidate.pop("_evidence_level", 3)

        # Keep the region we searched separate from the HQ we later verify.
        candidate.setdefault("discovery_region", candidate.get("headquarters_state"))
        pipeline_name = candidate.pop("_pipeline", None)
        if pipeline_name:
            candidate.setdefault("source_pipeline", pipeline_name)

        company_id, created = self.store.upsert_company(
            candidate,
            self.run_id,
            float(self.cfg.get("quality.fuzzy_name_match_threshold", 92)),
        )
        if created:
            self.stats["companies_created"] += 1
        else:
            self.stats["companies_merged"] += 1

        if evidence_url:
            self.store.add_evidence(
                company_id,
                candidate.get("discovery_source", "Discovery"),
                evidence_url,
                f"Discovered via {candidate.get('discovery_query', 'search')}",
                evidence_level=int(evidence_level),
            )
        return created

    # ------------------------------------------------------------------
    # STAGE 1A — APIFY FRESH DISCOVERY
    # ------------------------------------------------------------------

    def stage_apify_discover(self) -> None:
        """LLM-generated queries, scraped via Apify — a second discovery
        route that stays fresh where the static query library (Modules 36/
        37/58) has already been mined. Query text varies every run (see
        llm_tasks.generate_fresh_queries), which naturally sidesteps the
        exact-text query_cooldown that limits the static library instead of
        replacing it.

        Nationwide by design, not per-state — the point is escaping
        already-mined geography, not re-looping the same states.
        """
        if not self.apify:
            self.log.info("APIFY: no token configured; skipping")
            return

        checkpoint = f"apify_discover:{self.run_id}"
        if self.store.is_done(self.run_id, "apify_discover", checkpoint):
            return

        node = self.cfg.get("discovery.apify", {}) or {}
        query_count = int(node.get("queries_per_run", 15))
        cooldown = int(node.get("query_cooldown_days", 14))
        default_segment = "Managed technology provider"
        scope_key = "US-NATIONAL"

        recent = self.store.recent_queries("apify", limit=60)
        try:
            queries = llm_tasks.generate_fresh_queries(self.llm, self.cfg, query_count, recent)
        except LLMBudgetExhausted as exc:
            self.log.warning("%s — skipping Apify discovery this run.", exc)
            self.store.mark_done(self.run_id, "apify_discover", checkpoint)
            return
        if not queries:
            self.log.info("APIFY: no usable queries generated; skipping")
            self.store.mark_done(self.run_id, "apify_discover", checkpoint)
            return

        self.log.info("APIFY: %d fresh queries generated", len(queries))
        for query in queries:
            if self.should_stop():
                break
            if self.store.query_recently_run("apify", query, scope_key, cooldown):
                self.log.debug("  cooldown skip: %s", query[:70])
                continue
            try:
                found = new = 0
                for candidate in self.apify.candidates(query, default_segment, self.aggregators):
                    found += 1
                    candidate["discovery_route"] = "reverse_engineered"
                    candidate["discovery_group"] = "apify_fresh"
                    if self._ingest(candidate):
                        new += 1
                self.store.log_query(
                    "apify", query, scope_key, found, new, self.run_id, "apify_fresh"
                )
                if new:
                    self.log.debug("  apify_fresh -> %d new (%s)", new, query[:60])
            except ApifyBudgetExhausted as exc:
                self.log.warning("%s — stopping Apify discovery.", exc)
                break

        self.store.mark_done(self.run_id, "apify_discover", checkpoint)
        self.stats["apify_calls"] = self.apify.calls_made

    # ------------------------------------------------------------------
    # STAGE 1B — PRESCREEN
    # ------------------------------------------------------------------

    def stage_prescreen(self) -> None:
        """Reject obvious non-operators before spending a crawl on them.

        SOP Module 31's work order: build a raw universe, quick-screen
        obvious exclusions, THEN fully validate the strongest candidates.
        Only ever runs against this run's freshly discovered ('raw')
        companies — anything already touched by a prior run has already been
        crawled, so the crawl cost is sunk and re-screening it from a
        thinner snippet than the full crawl already used would only risk
        knocking out an already-verified account.

        Deliberately NOT written to the permanent exclusion ledger, unlike
        clean.py's post-crawl demotions. A search snippet is much thinner
        evidence than a full crawl, so even at prescreen.MIN_CONFIDENCE this
        stays a reversible, this-run-only decision — a human can simply
        reset the row's status rather than first clearing a permanent ledger
        entry to reconsider it.
        """
        if not bool(self.cfg.get("prescreen.enabled", True)):
            return
        min_confidence = float(self.cfg.get("prescreen.min_confidence", prescreen_mod.MIN_CONFIDENCE))
        rows = self.store.companies_where("status = 'raw'")
        self.log.info("PRESCREEN: checking %d newly discovered companies", len(rows))

        screened = 0
        for row in rows:
            if self.should_stop():
                break
            screen_out, reason = prescreen_mod.prescreen(
                row["company_description"], row["website"], min_confidence
            )
            if not screen_out:
                continue
            self.store.update_company(
                row["id"], status="excluded", status_reason=reason, tier="Exclude",
            )
            self.store.add_learning(
                self.run_id, "prescreen_exclusion", reason[:120],
                f"{row['company_name']} [{row['website']}]",
            )
            screened += 1

        self.stats["prescreened_out"] = screened
        self.log.info("  %d screened out before crawl (saved a crawl each)", screened)

    # ------------------------------------------------------------------
    # STAGE 2 — ENRICH
    # ------------------------------------------------------------------

    def stage_enrich(self) -> None:
        refresh_days = int(self.cfg.get("run.refresh_after_days", 30))
        cutoff = (
            datetime.now().timestamp() - refresh_days * 86400
        )
        rows = self.store.companies_where(
            "website IS NOT NULL AND status != 'excluded' ORDER BY id"
        )
        todo = []
        for row in rows:
            last = row["last_enriched_at"]
            if not last:
                todo.append(row)
                continue
            try:
                if datetime.fromisoformat(last).timestamp() < cutoff:
                    todo.append(row)
            except ValueError:
                todo.append(row)

        # Route B (reverse-engineered — hiring/event/ecosystem signals)
        # discoveries crawl first: SOP Module 04 says this route "produces
        # better timing and relevance evidence", so an interrupted or
        # budget-capped run still lands on the strongest candidates. Stable
        # sort — order is otherwise unchanged.
        todo.sort(key=lambda r: 0 if r["discovery_route"] == "reverse_engineered" else 1)

        self.log.info("ENRICH: %d companies to crawl (%d total in db)", len(todo), len(rows))
        if not todo:
            return

        scan_careers = bool(self.cfg.get("intent.scan_careers", True))
        batch_size = max(self.enricher.max_workers * 4, 20)

        for start in range(0, len(todo), batch_size):
            if self.should_stop():
                break
            batch = todo[start:start + batch_size]
            pairs = [(r["id"], r["website"]) for r in batch]
            results = self.enricher.crawl_many(pairs)

            for row in batch:
                result = results.get(row["id"])
                if result is None:
                    continue
                self._apply_crawl(row, result, scan_careers)

            done = min(start + batch_size, len(todo))
            self.log.info("  enriched %d/%d", done, len(todo))

    def _apply_crawl(self, row: Any, result: Any, scan_careers: bool) -> None:
        company_id = row["id"]
        now = datetime.now().isoformat(timespec="seconds")

        if not result.reachable:
            self.stats["crawl_failures"] += 1
            self.store.update_company(
                company_id,
                website_reachable=0,
                last_enriched_at=now,
                review_notes=(result.error or "unreachable")[:300],
            )
            return

        self.stats["enriched"] += 1
        text = result.text
        updates: dict[str, Any] = {
            "website_reachable": 1,
            "crawl_pages": result.pages_fetched,
            "raw_text_len": len(text),
            "last_enriched_at": now,
        }

        # -- name resolution (structured sources first, domain as fallback) --
        resolved, name_source = nameres.resolve(
            result.home_soup, text, row["website"], fallback=row["company_name"]
        )
        if resolved:
            from pipeline.dedup import name_key as _nk

            updates["company_name"] = resolved
            updates["name_key"] = _nk(resolved)
            updates["name_source"] = name_source

        # -- what kind of organisation is this? (SOP Module 26) --------------
        entity = classify.classify_entity(text, row["website"])
        updates["entity_type"] = entity["type"]
        updates["entity_confidence"] = entity["confidence"]
        updates["entity_evidence"] = ", ".join(entity["evidence"])[:500]

        # -- is it a US organisation? ---------------------------------------
        # The original discovery URL still carries any regional path the
        # crawler dropped when it preferred the root domain.
        geo = classify.classify_geography(
            text, row["website"], result.phone,
            extra_urls=[row["website"], result.page_urls.get("home", "")],
        )
        if geo["is_us"] is not None:
            updates["is_us"] = 1 if geo["is_us"] else 0
        updates["geo_reason"] = "; ".join(geo["reasons"])[:300]
        if geo.get("state"):
            result.state_code = result.state_code or geo["state"]

        # Record the URL that actually served the homepage, so the tracker
        # links to the root site rather than a deep search landing page.
        home_url = result.page_urls.get("home")
        if home_url:
            from pipeline.dedup import canonical_url as _canon

            canonical = _canon(home_url)
            if canonical and canonical != row["website"]:
                updates["website"] = canonical

        if result.linkedin_url and not row["linkedin_url"]:
            updates["linkedin_url"] = result.linkedin_url
        if result.phone and not row["phone"]:
            from pipeline.dedup import phone_key as _pk

            updates["phone"] = result.phone
            updates["phone_key"] = _pk(result.phone)
        # Discovery only knows which state we *searched*, not where the company
        # is headquartered — a national provider with a Wilmington landing page
        # is not a Delaware company. An address parsed from the company's own
        # site is authoritative; otherwise the search region stays but is
        # flagged unconfirmed so nobody treats it as verified.
        if result.state_code:
            updates["headquarters_state"] = result.state_code
            updates["hq_confirmed"] = 1
            if result.state_code != row["headquarters_state"]:
                self.store.add_evidence(
                    company_id, "Company website",
                    result.page_urls.get("contact") or result.page_urls.get("home"),
                    f"Address on company site indicates {result.state_code}, "
                    f"not the searched region {row['headquarters_state']}",
                    evidence_level=1,
                )
        else:
            updates["hq_confirmed"] = 0

        # Headcount (SOP Module 25 bands). employee_range_source is never
        # fabricated — 'stated' only when a real number was found on the
        # site; otherwise it stays 'unknown' rather than a guessed band
        # (see score.py for why: signal density does not reliably predict
        # actual headcount in this pipeline's own data).
        estimate = signal_lex.extract_employee_estimate(text)
        if estimate:
            updates["employee_estimate"] = estimate
            updates["employee_range"] = signal_lex.employee_range_for(estimate)
            updates["employee_range_source"] = "stated"

        services = summarise_services(text)
        if services:
            updates["services"] = services
        industries = summarise_industries(text)
        if industries:
            updates["customer_industries"] = industries
        if not row["company_description"]:
            home = result.page_texts.get("home", "")
            updates["company_description"] = home[:1200] or None

        # -- signals + evidence ------------------------------------------
        detected = signal_lex.detect_all(text)
        recurring_phrases: list[str] = []
        operational_phrases: list[str] = []

        for category, hits in detected.items():
            for name, phrases in hits.items():
                source = self._best_source(result, category)
                self.store.add_signal(
                    company_id, category, name,
                    detail=", ".join(phrases[:6]), source_url=source,
                )
                if category == "recurring":
                    recurring_phrases.extend(phrases[:3])
                elif category == "operational":
                    operational_phrases.extend(phrases[:3])

        if recurring_phrases:
            fact = "Website states recurring services: " + ", ".join(
                sorted(set(recurring_phrases))[:8]
            )
            updates["recurring_evidence"] = fact[:900]
            self.store.add_evidence(
                company_id, "Company website",
                result.page_urls.get("services") or result.page_urls.get("home"),
                fact, evidence_level=1,
            )
        if operational_phrases:
            updates["operational_signals"] = ", ".join(sorted(set(operational_phrases))[:12])[:900]
            self.store.add_evidence(
                company_id, "Company website",
                result.page_urls.get("services") or result.page_urls.get("home"),
                "Operational responsibility evidence: "
                + ", ".join(sorted(set(operational_phrases))[:8]),
                evidence_level=1,
            )

        # -- exclusion patterns -------------------------------------------
        for name, phrases in signal_lex.detect_exclusions(text).items():
            self.store.add_signal(
                company_id, "exclusion", name,
                detail=", ".join(phrases[:6]),
                source_url=result.page_urls.get("home"),
            )

        # -- ATS pipeline runs first: structured postings with real posting
        # dates are strictly better hiring evidence than a text scan of the
        # same careers page, so it should never lose the signal slot to it. --
        ats_found_hiring = self._ats_intent(company_id, result, updates)

        # -- careers text scan: fallback only, and only when ATS found
        # nothing — otherwise this coarse, undated scan would claim the
        # evidence slot before the richer, dated ATS signal runs. --
        careers_hiring_found: list[str] = []
        if scan_careers and "careers" in result.page_texts and not ats_found_hiring:
            careers_hiring_found = intent_mod.careers_signals(
                self.store, company_id,
                result.page_texts["careers"],
                result.page_urls.get("careers"),
            )

        # -- Tier 1 intent: everything observable on the company's own site --
        self._first_party_intent(
            company_id, result, text, updates,
            hiring_already_found=ats_found_hiring or bool(careers_hiring_found),
        )

        self.store.update_company(company_id, **updates)
        for page_type, url in result.page_urls.items():
            self.store.mark_url_seen(url, 200, company_id)

    def _ats_intent(self, company_id: int, result: Any, updates: dict[str, Any]) -> bool:
        """Structured postings beat a text scan of the careers page.

        The text scan can only answer "is a NOC Manager mentioned somewhere".
        A hosted ATS publishes titles, posting dates, locations and full
        descriptions as JSON, which turns hiring from a binary flag into a
        measured, dated signal — and job descriptions name the actual RMM/PSA/
        monitoring stack far more explicitly than marketing copy does.

        Returns True when operational hiring was found, so the caller can
        skip the weaker, undated careers-page text scan.
        """
        spec = self.registry.get("ats_jobs")
        if not spec or not spec.enabled:
            return False

        careers_url = result.page_urls.get("careers")
        # Search the careers page's links first, then the homepage's — the ATS
        # link lives in an href, not in visible text.
        link_blob = " ".join(
            result.page_links.get(page, "") for page in ("careers", "home")
        )
        soup = result.careers_soup or result.home_soup

        handle, jobs = self.ats.jobs_for_company(link_blob, soup)

        # Fall back to schema.org JobPosting, which Google Jobs requires and
        # most career pages embed even without a hosted ATS.
        if not jobs:
            jobs = jobs_from_jsonld(result.careers_soup) or jobs_from_jsonld(result.home_soup)
            if jobs:
                handle = handle or "jsonld:JobPosting"

        # Last resort, and the common case for this ICP: most mid-market MSPs
        # list openings as plain links on their own careers page, with no ATS
        # and no JobPosting markup. Titles and a count, but no dates — so this
        # route's events fall back to the undated confidence penalty rather
        # than a measured one (see intent_signals.recency_multiplier).
        if not jobs and result.careers_soup is not None:
            jobs = extract_titles_from_html(result.careers_soup)
            if jobs:
                handle = handle or "careers-page"

        if not jobs:
            return False

        self.registry.record("ats_jobs", companies=1, postings=len(jobs))
        updates["ats_provider"] = handle

        operational_titles: list[str] = []
        newest: str | None = None
        all_descriptions: list[str] = []

        leadership_titles: list[str] = []
        for job in jobs:
            title = (job.get("title") or "").strip()
            blob = f"{title} {job.get('description', '')}"
            lead_roles, front_roles = intent_lex.detect_any_operational_role(blob)
            is_operational = bool(lead_roles or front_roles)
            _, tools = intent_lex.detect_tool_fragmentation(blob)
            self.store.add_job(
                company_id, job, handle or "unknown", is_operational,
                "; ".join(sorted(tools))[:400],
            )
            if is_operational:
                operational_titles.append(title or (lead_roles or front_roles)[0])
                if lead_roles:
                    leadership_titles.append(title or lead_roles[0])
                all_descriptions.append(blob)
                posted = job.get("posted_at")
                if posted and (newest is None or posted > newest):
                    newest = posted

        updates["open_roles"] = len(operational_titles)
        if not operational_titles:
            return False

        # A real posting date (`newest`) gives this event full recency
        # credit; without one it still counts — it clearly happened — but at
        # a permanent confidence penalty (intent_signals.recency_multiplier).
        # A leadership hire means the company is building or replacing the
        # function that owns the problem. Front-line hiring only shows the
        # function is growing, so it lands on the weaker signal.
        event_name = "hiring_operational" if leadership_titles else "hiring_context"
        self.store.add_intent_event(
            company_id, event_name,
            detail=f"{len(operational_titles)} operational role(s) open via {handle}: "
                   + ", ".join(operational_titles[:5]),
            source_url=careers_url,
            source_type="ATS job board",
            evidence_level=1,
            event_date=newest,
        )

        if len(operational_titles) >= 2:
            self.store.add_intent_event(
                company_id, "hiring_volume",
                detail=f"{len(operational_titles)} operational roles open simultaneously",
                source_url=careers_url,
                source_type="ATS job board",
                evidence_level=1,
                event_date=newest,
            )

        self.store.add_evidence(
            company_id, "ATS job board", careers_url,
            f"{len(operational_titles)} open operational role(s) ({handle}): "
            + ", ".join(operational_titles[:5]),
            evidence_level=1,
        )

        # Job descriptions are the richest source of tool names. Tool
        # fragmentation is an ongoing condition, not a dated event (Change 1),
        # so it scores Operational need, not Timing — see score.py.
        if all_descriptions:
            count, tools = intent_lex.detect_tool_fragmentation(" ".join(all_descriptions))
            if count > int(updates.get("tool_categories", 0) or 0):
                updates["tool_categories"] = count
                updates["tools_found"] = "; ".join(
                    f"{cat}: {', '.join(names[:3])}" for cat, names in tools.items()
                )[:900]
            if count >= 4:
                self.store.add_signal(
                    company_id, "complexity", "tool_fragmentation",
                    detail=f"Job postings name tooling across {count} categories: "
                           + ", ".join(sorted(tools))[:300],
                    source_url=careers_url,
                )

        return True

    def _first_party_intent(
        self, company_id: int, result: Any, text: str, updates: dict[str, Any],
        hiring_already_found: bool = False,
    ) -> None:
        """Tier 1 intent — observable on the company's own site, no API cost.

        These are the strongest signals available because they are first-party
        and current. Change 1 splits them by kind: most of what follows is an
        ongoing CONDITION (compliance programme, status page, tool estate —
        SOP Module 25 "maturity signals") and is written to `signals` so it
        scores Operational need in score.py. Only genuinely dated, discrete
        EVENTS go to `intent_events` and draw Timing points.
        """
        home_url = result.page_urls.get("home")

        # Tool fragmentation — the pain the platform exists to solve. A
        # condition (breadth of the current tool estate), not an event.
        category_count, tools = intent_lex.detect_tool_fragmentation(text)
        if category_count:
            updates["tool_categories"] = category_count
            updates["tools_found"] = "; ".join(
                f"{cat}: {', '.join(names[:3])}" for cat, names in tools.items()
            )[:900]
        if category_count >= 4:
            self.store.add_signal(
                company_id, "complexity", "tool_fragmentation",
                detail=f"{category_count} distinct tool categories in use: "
                       + ", ".join(sorted(tools))[:300],
                source_url=home_url,
            )
            self.store.add_evidence(
                company_id, "Company website", home_url,
                f"Operates tooling across {category_count} categories "
                f"({', '.join(sorted(tools))}) — fragmented operational context",
                evidence_level=1,
            )

        # Active compliance / certification programme — a standing state.
        compliance = intent_lex.detect_compliance_pursuit(text)
        if compliance:
            self.store.add_signal(
                company_id, "operational", "compliance_pursuit",
                detail=", ".join(sorted(set(compliance))[:6]), source_url=home_url,
            )

        # Public status page / incident history — SLA accountability, ongoing.
        incidents = intent_lex.detect_incident_history(text)
        if incidents:
            name = "status_page" if any(
                "status" in i for i in incidents
            ) else "incident_history"
            self.store.add_signal(
                company_id, "operational", name,
                detail=", ".join(sorted(set(incidents))[:6]), source_url=home_url,
            )

        # Scale of the managed estate — a standing state, not a moment.
        scale = intent_lex.detect_client_scale(text)
        if scale and scale >= 50:
            self.store.add_signal(
                company_id, "operational", "client_scale_claim",
                detail=f"Publicly claims ~{scale} managed clients/endpoints",
                source_url=home_url,
            )

        # Ecosystem participation. Partner/association membership is ongoing;
        # an award is a discrete, dated-in-principle achievement, so it stays
        # a timing event even though the exact award date isn't extracted yet
        # (it still counts, at the undated confidence penalty).
        for name, hits in intent_lex.detect_ecosystem(text).items():
            detail = ", ".join(sorted(set(hits))[:6])
            if name == "recent_award":
                self.store.add_intent_event(
                    company_id, name, detail=detail, source_url=home_url,
                    source_type="Company website", evidence_level=1,
                )
            else:
                self.store.add_signal(
                    company_id, "operational", name, detail=detail, source_url=home_url,
                )

        # Leaders writing about the operational problem — voice, not a moment.
        content = intent_lex.detect_operational_content(text)
        if len(content) >= 2:
            self.store.add_signal(
                company_id, "operational", "operational_content",
                detail=", ".join(sorted(set(content))[:6]), source_url=home_url,
            )

        # Hiring volume: several operational roles open at once. Only a
        # fallback — the ATS/careers-text routes above already cover this
        # with better (sometimes dated) evidence when they find anything.
        if not hiring_already_found and "careers" in result.page_texts:
            roles = intent_lex.detect_high_value_hiring(result.page_texts["careers"])
            if len(set(roles)) >= 2:
                self.store.add_intent_event(
                    company_id, "hiring_volume",
                    detail=f"{len(set(roles))} operational roles open: "
                           + ", ".join(sorted(set(roles))[:5]),
                    source_url=result.page_urls.get("careers"),
                    source_type="Careers page", evidence_level=1,
                )

    @staticmethod
    def _best_source(result: Any, category: str) -> str | None:
        preferred = {
            "recurring": ("services", "home"),
            "operational": ("services", "home"),
            "complexity": ("services", "industries", "home"),
            "vertical": ("industries", "case_studies", "home"),
            "access": ("about", "contact", "home"),
        }.get(category, ("home",))
        for page in preferred:
            if page in result.page_urls:
                return result.page_urls[page]
        return None

    # ------------------------------------------------------------------
    # STAGE 3 — INTENT (news)
    # ------------------------------------------------------------------

    def stage_intent(self) -> None:
        if not bool(self.cfg.get("intent.news_search", True)) or not self.tavily:
            self.log.info("INTENT: news search disabled or Tavily unavailable; "
                          "careers-based intent already captured during enrich.")
            self._refresh_timing_summaries()
            return

        min_score = float(self.cfg.get("intent.news_search_min_base_score", 45))
        max_companies = int(self.cfg.get("intent.max_news_companies_per_run", 300))
        lookback = int(self.cfg.get("intent.news_lookback_days", 365))

        # Cross-run spend guard: `checkpoints` resets every run_id, so without
        # this a company that already had a paid news search gets searched
        # again on every future run, forever. Mirrors query_recently_run.
        news_cooldown_days = int(self.cfg.get("intent.news_search_cooldown_days", 21))
        cooldown_cutoff = datetime.now().timestamp() - news_cooldown_days * 86400

        # Provisional score decides who is worth spending a credit on.
        candidates = []
        skipped_cooldown = 0
        for row in self.store.companies_where(
            "website_reachable = 1 AND status != 'excluded'"
        ):
            last_searched = row["last_news_search_at"]
            if last_searched:
                try:
                    if datetime.fromisoformat(last_searched).timestamp() >= cooldown_cutoff:
                        skipped_cooldown += 1
                        continue  # already spent a news credit on this company recently
                except ValueError:
                    pass
            provisional = score_mod.score_company(row, self.store, self.cfg)
            base = provisional["score_fit"] + provisional["score_need"]
            if base >= min_score * 0.65:
                candidates.append((base, row))

        candidates.sort(key=lambda pair: -pair[0])
        candidates = candidates[:max_companies]
        self.log.info(
            "INTENT: news search for %d companies (%d skipped — searched within %dd)",
            len(candidates), skipped_cooldown, news_cooldown_days,
        )

        for base, row in candidates:
            if self.should_stop():
                break
            key = f"news:{row['id']}"
            if self.store.is_done(self.run_id, "intent", key):
                continue
            try:
                found = intent_mod.news_signals(self.store, self.tavily, row, lookback)
                self.stats["news_searched"] += 1
                if found:
                    self.log.debug("  %s -> %s", row["company_name"], ", ".join(found))
            except TavilyBudgetExhausted as exc:
                self.log.warning("%s — stopping news intent.", exc)
                break
            # Stamp even on an empty result — an empty answer still cost a
            # credit and must not be re-asked again next run.
            self.store.update_company(
                row["id"], last_news_search_at=datetime.now().isoformat(timespec="seconds")
            )
            self.store.mark_done(self.run_id, "intent", key)

        if self.tavily:
            self.stats["tavily_calls"] = self.tavily.calls_made
        self.stats["news_search_cooldown_skipped"] = skipped_cooldown
        self._refresh_timing_summaries()

    def _refresh_timing_summaries(self) -> None:
        for row in self.store.companies_where("status != 'excluded'"):
            summary = intent_mod.summarise_timing(self.store, row["id"])
            if summary != (row["timing_signal"] or ""):
                self.store.update_company(row["id"], timing_signal=summary or None)

    # ------------------------------------------------------------------
    # STAGE 4 — QUALIFY
    # ------------------------------------------------------------------

    def stage_qualify(self) -> None:
        rows = self.store.companies_where("status != 'excluded'")
        self.log.info("QUALIFY: assessing %d companies", len(rows))
        tally = {"approved": 0, "review": 0, "excluded": 0}

        for row in rows:
            status, reason, checked = qualify_mod.qualify_company(
                row, self.store, self.cfg, self.run_id
            )
            tally[status] = tally.get(status, 0) + 1
            self.store.update_company(
                row["id"],
                status=status,
                status_reason=reason[:900],
                exclusions_checked=", ".join(checked)[:400],
            )

        self.log.info(
            "  approved=%d  review=%d  excluded=%d",
            tally.get("approved", 0), tally.get("review", 0), tally.get("excluded", 0),
        )
        self.stats["qualify"] = tally

        for reason, count in self._top_exclusion_reasons():
            self.store.add_learning(
                self.run_id, "exclusion_pattern",
                f"{count} companies excluded: {reason}",
                "Consider a negative keyword or a tighter discovery query.",
            )

    def _top_exclusion_reasons(self, limit: int = 8) -> list[tuple[str, int]]:
        rows = self.store.conn.execute(
            "SELECT reason, COUNT(*) AS n FROM exclusions WHERE run_id=?"
            " GROUP BY reason ORDER BY n DESC LIMIT ?",
            (self.run_id, limit),
        ).fetchall()
        return [(r["reason"], r["n"]) for r in rows]

    # ------------------------------------------------------------------
    # STAGE 5 — SCORE
    # ------------------------------------------------------------------

    def stage_score(self) -> None:
        # timing_signal feeds build_fit_reason() below and is normally
        # refreshed at the end of stage_intent — but that stage may have been
        # skipped (--skip intent, stages.intent: false) while enrich still
        # wrote fresh timing signals via _first_party_intent/_ats_intent.
        # Refresh here too so scoring never reads a stale summary.
        self._refresh_timing_summaries()

        rows = self.store.companies_where("status IN ('approved','review')")
        self.log.info("SCORE: scoring %d companies", len(rows))

        for row in rows:
            result = score_mod.score_company(row, self.store, self.cfg)
            fit_reason = qualify_mod.build_fit_reason(row, self.store)
            next_action = qualify_mod.next_action_for(row["status"], result["tier"])

            # Module 30: a low score is itself an exclusion decision.
            status = row["status"]
            if result["tier"] == "Exclude" and status == "approved":
                status = "review"
                self.store.update_company(
                    row["id"],
                    status=status,
                    status_reason="Passed qualification but scored below Tier C threshold",
                )

            self.store.update_company(
                row["id"],
                fit_reason=fit_reason,
                next_action=next_action,
                **result,
            )

        counts = self.store.counts()
        self.log.info(
            "  Tier A=%d  Tier B=%d  Tier C=%d",
            counts["tier_a"], counts["tier_b"], counts["tier_c"],
        )
        self.stats["tiers"] = {
            "A": counts["tier_a"], "B": counts["tier_b"], "C": counts["tier_c"],
        }

    # ------------------------------------------------------------------
    # STAGE 6/7 — QUALITY + EXPORT
    # ------------------------------------------------------------------

    def stage_llm(self) -> dict[str, Any]:
        """Groq assistance: entity second opinion, rationale, FP challenge.

        Gated hard on cost and value: the entity check only runs where the
        deterministic classifier was unsure or where a wrong call would let a
        non-account through, and the rationale/challenge only run on records
        that will actually be worked.
        """
        report = {"entity_checked": 0, "entity_disagreed": 0, "entity_gap_filled": 0,
                  "rationales": 0, "challenged": 0, "challenge_holds": 0}
        if not self.llm.available:
            self.log.info("LLM: no Groq key available; skipping")
            return report

        tasks = self.cfg.get("llm.tasks", {}) or {}
        cap = int(self.cfg.get("llm.max_companies_per_task", 400))
        max_conf = float(tasks.get("verify_entity_max_confidence", 0.6))
        min_score = float(tasks.get("rationale_min_score", 55))

        rows = self.store.companies_where("status IN ('approved','review')")
        self.log.info("LLM: %d candidate records", len(rows))

        # -- entity second opinion ----------------------------------------
        if tasks.get("verify_entity", True):
            targets = [
                r for r in rows
                if (r["entity_confidence"] or 0) <= max_conf or r["status"] == "approved"
            ] 
            targets.sort(key=lambda r: -(r["score_total"] or 0))
            for row in targets[:cap]:
                if self.should_stop():
                    break
                key = f"entity:{row['id']}"
                if self.store.is_done(self.run_id, "llm", key):
                    continue
                # Re-fetch the page rather than judge a 1,200-char marketing
                # snippet. The stored description is a truncated blurb; entity
                # type is a judgement about the whole site. The gated subset is
                # small enough that re-crawling is cheaper than persisting the
                # full text of every company.
                text = ""
                if row["website"]:
                    try:
                        crawl = self.enricher.crawl(row["website"])
                        if crawl.reachable:
                            text = crawl.text
                    except Exception:  # noqa: BLE001
                        text = ""
                if len(text.strip()) < 400:
                    text = (row["company_description"] or "") + " " + (row["services"] or "")
                if len(text.strip()) < 120:
                    continue
                try:
                    verdict = llm_tasks.verify_entity(self.llm, self.store, row, text)
                except LLMBudgetExhausted as exc:
                    self.log.warning("%s — stopping LLM entity checks.", exc)
                    break
                if not verdict:
                    continue
                report["entity_checked"] += 1

                # A deterministic verdict of None/unknown is an abstention, not
                # an opinion — the LLM filling that gap is not a disagreement,
                # and counting it as one made the model look 70% at odds with
                # the lexicon when the two genuinely conflict far less often.
                deterministic = row["entity_type"]
                if deterministic in (None, "", "unknown"):
                    report["entity_gap_filled"] += 1
                    agrees = None
                else:
                    agrees = verdict["entity_type"] == deterministic
                    if not agrees:
                        report["entity_disagreed"] += 1
                        self.store.add_learning(
                            self.run_id, "llm_entity_conflict",
                            f"{row['company_name']}: lexicon={deterministic} "
                            f"llm={verdict['entity_type']}",
                            verdict["evidence"],
                        )
                self.store.update_company(
                    row["id"],
                    llm_entity_type=verdict["entity_type"],
                    llm_entity_conf=verdict["confidence"],
                    llm_entity_evidence=verdict["evidence"],
                    llm_agrees=None if agrees is None else (1 if agrees else 0),
                )
                self.store.mark_done(self.run_id, "llm", key)
                self.registry.record("llm_verify", entity_checks=1)

        # -- rationale (SOP Module 63) -------------------------------------
        if tasks.get("write_rationale", True):
            targets = [r for r in rows if (r["score_total"] or 0) >= min_score]
            targets.sort(key=lambda r: -(r["score_total"] or 0))
            for row in targets[:cap]:
                if self.should_stop():
                    break
                key = f"rationale:{row['id']}"
                if self.store.is_done(self.run_id, "llm", key):
                    continue
                try:
                    rationale = llm_tasks.write_rationale(self.llm, self.store, row)
                except LLMBudgetExhausted as exc:
                    self.log.warning("%s — stopping LLM rationales.", exc)
                    break
                if rationale:
                    self.store.update_company(
                        row["id"], fit_reason=rationale, rationale_source="llm"
                    )
                    report["rationales"] += 1
                    self.registry.record("llm_verify", rationales=1)
                self.store.mark_done(self.run_id, "llm", key)

        # -- false-positive challenge (SOP Module 61) ----------------------
        if tasks.get("challenge_tier_a", True):
            targets = [r for r in rows if r["tier"] == "A" and r["status"] == "approved"]
            for row in targets[:cap]:
                if self.should_stop():
                    break
                key = f"challenge:{row['id']}"
                if self.store.is_done(self.run_id, "llm", key):
                    continue
                try:
                    result = llm_tasks.challenge(self.llm, self.store, row)
                except LLMBudgetExhausted as exc:
                    self.log.warning("%s — stopping LLM challenges.", exc)
                    break
                if result:
                    report["challenged"] += 1
                    if result["verdict"] != "approve":
                        report["challenge_holds"] += 1
                    self.store.update_company(
                        row["id"],
                        llm_challenge_verdict=result["verdict"],
                        llm_challenge_notes="; ".join(result["concerns"])[:900],
                    )
                    self.registry.record("llm_verify", challenges=1)
                self.store.mark_done(self.run_id, "llm", key)

        usage = self.llm.usage()
        self.log.info(
            "  entity checks=%d (gaps filled %d, genuine conflicts %d)  "
            "rationales=%d  challenges=%d (holds %d)",
            report["entity_checked"], report["entity_gap_filled"],
            report["entity_disagreed"], report["rationales"],
            report["challenged"], report["challenge_holds"],
        )
        self.log.info(
            "  Groq: %d calls, %d cache hits, %d/%d keys active",
            usage["calls_made"], usage["cache_hits"],
            usage["keys_active"], usage["keys_total"],
        )
        report["usage"] = usage
        return report

    def stage_clean(self) -> dict[str, Any]:
        self.log.info("CLEAN: verifying and repairing records before export")
        return clean_mod.clean_and_verify(
            self.store, self.cfg, self.run_id, self.aggregators,
            enterprise_domains=self.cfg.enterprise_domains,
        )

    def stage_contacts(self) -> dict[str, Any]:
        """Map US decision-makers inside approved Tier A accounts.

        SOP Module 31's work order step 4, and the first stage that spends
        per-item money ($0.50 per search + $0.01 per person returned). Three
        things follow from that:

          * it runs after `clean`, so the gate can require clean_verified — we
            never buy contacts for a company that failed entity verification;
          * companies are batched up to 10 per search by size band, because the
            $0.50 is charged per search, not per company;
          * every response is cached by a hash of its request, so an
            interrupted run resumes for free instead of re-buying.
        """
        summary: dict[str, Any] = {"searched": 0, "accepted": 0, "rejected": 0, "spend_usd": 0.0}
        if not self.salesnav:
            self.log.info("CONTACTS: no Apify token configured; skipping")
            return summary

        node = self.cfg.get("contacts", {}) or {}
        gate = node.get("gate", {}) or {}
        statuses = gate.get("statuses", ["approved"])
        tiers = gate.get("tiers", ["A"])
        cooldown_days = int(node.get("search_cooldown_days", 90))
        depth_map = node.get("depth", {}) or {}
        min_depth = int(node.get("min_depth", 2))
        over_fetch = float(node.get("over_fetch", 1.5))
        max_results = int(node.get("max_results_per_search", 100))
        us_policy = str(node.get("us_policy", "strict"))
        consultant_policy = str(node.get("consultant_policy", "review"))

        status_marks = ", ".join("?" for _ in statuses)
        tier_marks = ", ".join("?" for _ in tiers)
        where = f"status IN ({status_marks}) AND tier IN ({tier_marks})"
        if gate.get("require_clean_verified", True):
            where += " AND clean_verified = 1"
        if gate.get("require_linkedin_url", False):
            # Companies with no LinkedIn page mostly return nobody (6 of 10 in a
            # canary), so each still costs a search fee for ~0 people.
            where += " AND linkedin_url IS NOT NULL AND linkedin_url != ''"
        gated = self.store.companies_where(where + " ORDER BY score_total DESC",
                                           (*statuses, *tiers))

        cutoff = datetime.now().timestamp() - cooldown_days * 86400
        eligible = []
        for row in gated:
            if self.store.is_done(self.run_id, "contacts", f"company:{row['id']}"):
                continue
            last = row["last_contact_search_at"]
            if last:
                try:
                    if datetime.fromisoformat(last).timestamp() > cutoff:
                        continue  # searched recently — do not re-buy
                except ValueError:
                    pass
            eligible.append(row)

        self.log.info(
            "CONTACTS: %d eligible of %d gated accounts (%dd cooldown)",
            len(eligible), len(gated), cooldown_days,
        )
        if not eligible:
            return summary

        us_index = contacts_mod.build_us_index(self.cfg)
        batches = contacts_mod.plan_batches(eligible, self.cfg)
        self.log.info("CONTACTS: %d batched search(es) planned", len(batches))

        nonempty_seen = False
        deferred_empty: list[Any] = []
        for batch in batches:
            if self.should_stop():
                break
            depth = int(depth_map.get(batch.rows[0]["tier"], min_depth))
            limit = min(max_results, max(min_depth, int(depth * len(batch.rows) * over_fetch)))
            try:
                payload = contacts_mod.build_search_input(
                    company_names=None if batch.all_have_linkedin_ids else batch.names,
                    company_ids=batch.linkedin_ids if batch.all_have_linkedin_ids else None,
                    band=batch.band, cfg=self.cfg, limit=limit,
                )
            except ValueError as exc:
                self.log.error("CONTACTS: cannot build search input — %s", exc)
                break

            search_key = _search_key(payload)
            cached = self.store.get_contact_search(search_key)
            if cached:
                self.log.debug("  cache hit, no charge: %s", ", ".join(batch.names)[:70])
                response = json.loads(cached["response_json"] or "{}")
            else:
                try:
                    response = self.salesnav.search(payload)
                except SalesNavBudgetExhausted as exc:
                    self.log.warning("%s — stopping contact mapping.", exc)
                    break
                except Exception as exc:  # noqa: BLE001
                    self.log.warning("CONTACTS: search failed (%s); skipping batch", exc)
                    continue
                rows = response.get("data") or []
                # Persist BEFORE mapping, so a crash between the paid call and
                # the checkpoint replays from cache at zero cost.
                self.store.record_contact_search(
                    search_key, json.dumps(payload, sort_keys=True), json.dumps(response),
                    str(response.get("status") or "success"), len(rows),
                    contacts_mod.INIT_COST_USD + len(rows) * contacts_mod.RESULT_COST_USD,
                    self.run_id, response.get("request_id"),
                )
                self.stats["contact_searches"] += 1
                self.stats["contact_results"] += len(rows)

            got_rows = bool(response.get("data") or [])
            if got_rows:
                nonempty_seen = True
            elif len(batch.rows) >= 3:
                # A 3+ company batch returning nobody is either the actor
                # rejecting the payload (then nothing works and we must not lock
                # anyone out) or ten companies that genuinely are not on
                # LinkedIn (then re-buying them every run wastes the $0.50
                # search fee). Which one it was is only known once the whole run
                # is done: if any other batch returned people, the payload was
                # fine. Defer the decision to after the loop.
                deferred_empty.append(batch)
                self.log.warning(
                    "CONTACTS: empty result for %d-company batch (%s); deciding after run",
                    len(batch.rows), ", ".join(batch.names)[:70],
                )
                continue

            if response.get("status") == "failed":
                # The provider exhausted every token/retry without ever
                # completing the search (e.g. dead token, actor 403). This is
                # not "searched and found nobody" — stamping the cooldown here
                # would burn the 90-day slot for a company that was never
                # actually searched. Leave it eligible for the next attempt.
                self.log.warning(
                    "CONTACTS: search failed for batch (%s); leaving companies eligible",
                    ", ".join(batch.names)[:70],
                )
                continue

            attributed, unmatched = contacts_mod.attribute_people(
                response.get("data") or [], batch, self.cfg
            )
            if unmatched:
                self.stats["contact_match_failures"] += len(unmatched)
                self.store.add_learning(
                    self.run_id, "contact_match_miss",
                    f"{len(unmatched)} person(s) matched no company in batch",
                    ", ".join(sorted({p.get("li_company_name") or "?" for p in unmatched}))[:400],
                )

            for row in batch.rows:
                cid = int(row["id"])
                people = attributed.get(cid, [])
                kept = self._persist_contacts(
                    row, people, us_index, us_policy, consultant_policy,
                    int(depth_map.get(row["tier"], min_depth)),
                )
                summary["accepted"] += len(kept)
                if kept:
                    self.stats["contact_companies_done"] += 1
                    self._write_access_signals(cid, kept)
                    self.store.update_company(
                        cid, last_contact_search_at=_now_iso(),
                        contact_search_status="searched",
                        linkedin_company_id=kept[0].get("li_company_id")
                        or row["linkedin_company_id"],
                    )
                    self.store.mark_done(self.run_id, "contacts", f"company:{cid}")
                else:
                    # Not checkpointed: a company that yielded nothing inside a
                    # shared batch may simply have been crowded out by a larger
                    # one, so it stays eligible for a smaller batch next run.
                    self.stats["contact_companies_empty"] += 1
                    self.store.update_company(
                        cid, last_contact_search_at=_now_iso(),
                        contact_search_status="no_results",
                    )

        if deferred_empty and nonempty_seen:
            for b in deferred_empty:
                for row in b.rows:
                    self.store.update_company(
                        int(row["id"]), last_contact_search_at=_now_iso(),
                        contact_search_status="no_results",
                    )
            self.log.info(
                "CONTACTS: %d empty batch(es) stamped as no-results (other batches "
                "returned people, so the search itself was valid)", len(deferred_empty),
            )
        elif deferred_empty:
            self.log.warning(
                "CONTACTS: every batch was empty; not stamping any cooldown "
                "(likely a payload/actor problem)"
            )

        usage = self.salesnav.token_usage()
        self.stats["contact_spend_usd"] = usage["spend_usd"]
        summary.update(
            searched=usage["searches_made"],
            rejected=self.stats["contacts_rejected"],
            spend_usd=usage["spend_usd"],
        )
        self.log.info(
            "  %d accepted, %d rejected (%d non-US), %d unmatched — $%.2f spent",
            summary["accepted"], self.stats["contacts_rejected"],
            self.stats["contacts_non_us"], self.stats["contact_match_failures"],
            usage["spend_usd"],
        )
        return summary

    def _persist_contacts(
        self, row: Any, people: list[dict[str, Any]], us_index: Any,
        us_policy: str, consultant_policy: str, depth: int,
    ) -> list[dict[str, Any]]:
        """Classify, filter and store one company's people. Returns the accepted."""
        cid = int(row["id"])
        candidates: list[dict[str, Any]] = []

        for person in people:
            if not person.get("profile_key"):
                continue
            title = person.get("job_title")

            # US-only, layer 2: re-check the location the actor returned rather
            # than trusting its geo filter as the sole control.
            geo = contacts_mod.classify_location(person.get("location"), us_index)
            person["is_us"] = 1 if geo["is_us"] else (0 if geo["is_us"] is False else None)
            person["us_state"] = geo["state"]

            reason = contacts_mod.reject_reason(title, person.get("about"))
            role = contacts_mod.classify_role(title)

            if geo["is_us"] is not True and us_policy == "strict":
                person["status"] = "rejected"
                person["reject_reason"] = (
                    "non_us_location" if geo["is_us"] is False else "location_unresolved"
                )
                self.stats["contacts_non_us"] += 1
                if geo["is_us"] is None:
                    self.store.add_learning(
                        self.run_id, "contact_location_unresolved",
                        person.get("location") or "", row["company_name"],
                    )
            elif reason:
                person["status"] = "review" if (
                    reason == "consultant" and consultant_policy == "review"
                ) else "rejected"
                person["reject_reason"] = reason
            elif not role:
                person["status"] = "rejected"
                person["reject_reason"] = "role_unmapped"
            else:
                person["status"] = "accepted"

            person["role_type"] = role
            person["rank_score"] = contacts_mod.rank_score(title, role)
            person["email_status"] = "not found — use LinkedIn"
            candidates.append(person)

        accepted = [p for p in candidates if p["status"] == "accepted"]
        keep = contacts_mod.rank_contacts(accepted, depth, self.cfg)
        keep_keys = {p["profile_key"] for p in keep}
        # Anything good but beyond the Module 31 depth target is held as review
        # rather than discarded — it was already paid for.
        for person in accepted:
            if person["profile_key"] not in keep_keys:
                person["status"] = "review"
                person["reject_reason"] = "beyond_tier_depth_target"

        for person in candidates:
            _, created = self.store.upsert_contact(cid, person, self.run_id)
            if created:
                self.stats["contacts_created"] += 1
            else:
                self.stats["contacts_updated"] += 1
            if person["status"] in ("rejected", "review"):
                self.stats["contacts_rejected"] += 1

        return keep

    def _write_access_signals(self, cid: int, kept: list[dict[str, Any]]) -> None:
        """Feed real decision-makers back into the Module 30 access score.

        Deliberately one run behind: this stage runs after `score`, so these
        signals are read by the NEXT run. The effect is monotone — contacts only
        add points — so a company can never be de-gated by the very mechanism
        that gated it.
        """
        best = kept[0]
        self.store.add_signal(
            cid, "access", "named_decision_maker",
            detail=f"{len(kept)} verified US decision-maker(s) mapped",
            source_url=best.get("linkedin_url"),
        )
        if any(p.get("role_type") == "economic" for p in kept):
            self.store.add_signal(
                cid, "access", "economic_buyer_identified",
                detail="Founder/CEO/COO-level sponsor identified (SOP Module 41)",
                source_url=best.get("linkedin_url"),
            )
        for person in kept:
            if person.get("linkedin_url"):
                self.store.add_evidence(
                    cid, "LinkedIn profile", person["linkedin_url"],
                    f"{person.get('full_name')} — {person.get('job_title')}",
                    evidence_level=2,
                )

    def stage_quality(self) -> dict[str, Any]:
        self.log.info("QUALITY: running end-of-run checks")
        report = quality_mod.run_quality_checks(self.store, self.cfg, self.run_id)
        self.stats["quality"] = {
            "passed": report.get("passed"),
            "avg_completeness_pct": report.get("avg_completeness_pct"),
            "duplicate_rate_pct": report.get("duplicate_rate_pct"),
            "issues": report.get("issues"),
        }
        return report

    def stage_export(self, report: dict[str, Any]) -> Any:
        """Write the deliverable.

        `master` (default) is the accumulating two-sheet tracker. `legacy` is
        the old multi-sheet per-run workbook, kept as a one-line rollback and
        for the diagnostic sheets (Evidence, Signals, Excluded, Run_Report)
        that the master deliberately omits — that data all stays in SQLite.
        """
        mode = str(self.cfg.get("export.mode", "master")).lower()
        path = None
        if mode in ("master", "both"):
            path = master_export.build_master_workbook(self.store, self.cfg, self.run_id)
            self.stats["master_path"] = str(path)
        if mode in ("legacy", "both"):
            legacy = exporter.export_workbook(self.store, self.cfg, self.run_id, report)
            self.stats["export_path"] = str(legacy)
            path = path or legacy
        return path

    # ------------------------------------------------------------------

    def run(self) -> int:
        self.store.start_run(self.run_id, self.cfg.get("run.label", "run"))
        self.log.info("=" * 72)
        self.log.info("RUN %s  label=%s", self.run_id, self.cfg.get("run.label"))
        self.log.info("=" * 72)

        report: dict[str, Any] = {"run_id": self.run_id, "issues": [], "passed": True}
        status = "completed"

        try:
            if self.stage_enabled("discover"):
                self.stage_discover()
            if self.stage_enabled("apify_discover") and not self.should_stop():
                self.stage_apify_discover()
            if self.stage_enabled("prescreen") and not self.should_stop():
                self.stage_prescreen()
            if self.stage_enabled("enrich") and not self.should_stop():
                self.stage_enrich()
            if self.stage_enabled("intent") and not self.should_stop():
                self.stage_intent()
            if self.stage_enabled("qualify"):
                self.stage_qualify()
            if self.stage_enabled("score"):
                self.stage_score()
            if self.stage_enabled("llm") and not self.should_stop():
                self.stats["llm"] = self.stage_llm()
            if self.stage_enabled("clean"):
                clean_report = self.stage_clean()
                self.stats["clean"] = clean_report
            if self.stage_enabled("contacts") and not self.should_stop():
                self.stats["contacts"] = self.stage_contacts()
            if self.stage_enabled("quality"):
                report = self.stage_quality()
                report["clean"] = self.stats.get("clean")
            if self.stage_enabled("export"):
                self.stage_export(report)
            if _interrupted:
                status = "interrupted"
        except Exception as exc:  # noqa: BLE001
            self.log.exception("Run failed: %s", exc)
            status = "failed"
            self.stats["error"] = str(exc)

        self.stats["elapsed_seconds"] = round(time.time() - self.started, 1)
        self.stats["counts"] = self.store.counts()
        self.stats["pipelines"] = self.registry.summary()
        self.stats["pipeline_yield"] = yield_by_pipeline(self.store)
        self.stats["query_group_yield"] = yield_by_query_group(self.store)
        self.store.finish_run(self.run_id, status, self.stats)
        self._print_summary(status, report)
        self.store.close()
        if self.tavily:
            self.tavily.close()
        if self.apify:
            self.apify.close()
        if self.salesnav:
            self.salesnav.close()
        return 0 if status == "completed" else 1

    def _print_summary(self, status: str, report: dict[str, Any]) -> None:
        counts = self.stats["counts"]
        elapsed = self.stats["elapsed_seconds"]
        lines = [
            "",
            "=" * 72,
            f"RUN {self.run_id} — {status.upper()}  ({elapsed / 60:.1f} min)",
            "=" * 72,
            f"  Candidates seen        {self.stats['candidates_seen']}",
            f"  New companies          {self.stats['companies_created']}",
            f"  Merged into existing   {self.stats['companies_merged']}   <- dedup working",
            f"  Aggregators skipped    {self.stats['aggregators_skipped']}",
            f"  Junk names skipped     {self.stats['junk_names_skipped']}",
            f"  Prior exclusions hit   {self.stats['prior_exclusions_skipped']}",
            f"  Screened before crawl  {self.stats['prescreened_out']}   <- crawl saved",
            f"  Websites crawled       {self.stats['enriched']} "
            f"({self.stats['crawl_failures']} failed)",
            f"  News searches          {self.stats['news_searched']}"
            f"  ({self.stats['news_search_cooldown_skipped']} skipped, searched recently)",
            f"  Tavily calls used      {self.stats['tavily_calls']}",
            f"  Apify calls used       {self.stats['apify_calls']}",
            f"  Contacts mapped        {self.stats['contacts_created']} new / "
            f"{self.stats['contacts_updated']} refreshed across "
            f"{self.stats['contact_companies_done']} accounts",
            f"  Contacts filtered out  {self.stats['contacts_rejected']} "
            f"({self.stats['contacts_non_us']} non-US, "
            f"{self.stats['contact_match_failures']} unmatched)",
            "",
            f"  DATABASE  total={counts['total_companies']}  approved={counts['approved']}"
            f"  review={counts['review']}  excluded={counts['excluded']}",
            f"  TIERS     A={counts['tier_a']}  B={counts['tier_b']}  C={counts['tier_c']}",
            f"  EVIDENCE  {counts['evidence_rows']} rows   SIGNALS {counts['signal_rows']} rows",
        ]
        if report.get("avg_completeness_pct") is not None:
            lines.append(
                f"  QUALITY   completeness={report['avg_completeness_pct']}%  "
                f"duplicate_rate={report.get('duplicate_rate_pct')}%  "
                f"passed={report.get('passed')}"
            )
        for issue in report.get("issues", []):
            lines.append(f"    ! {issue}")
        rows = yield_by_pipeline(self.store)
        if rows:
            lines.append("")
            lines.append("  PIPELINE YIELD (qualified-account rate by source)")
            lines.append(f"    {'pipeline':<16}{'found':>7}{'appr':>6}{'tierA':>6}"
                         f"{'rate':>7}{'score':>7}{'intent':>7}")
            for row in rows:
                lines.append(
                    f"    {row['pipeline'][:15]:<16}{row['discovered']:>7}{row['approved']:>6}"
                    f"{row['tier_a']:>6}{row['qualified_rate_pct']:>6.1f}%"
                    f"{row['avg_score']:>7.1f}{row['avg_intent']:>7.1f}"
                )
        group_rows = yield_by_query_group(self.store)
        if group_rows:
            lines.append("")
            lines.append("  QUERY GROUP YIELD (approved accounts per API credit spent — SOP Module 34/69)")
            lines.append(f"    {'group':<28}{'found':>7}{'appr':>6}{'tierA':>6}"
                         f"{'rate':>7}{'calls':>7}{'appr/call':>10}")
            for row in group_rows:
                lines.append(
                    f"    {(row['group'] or '?')[:27]:<28}{row['discovered']:>7}{row['approved']:>6}"
                    f"{row['tier_a']:>6}{row['qualified_rate_pct']:>6.1f}%{row['calls_spent']:>7}"
                    f"{row['approved_per_call']:>10.3f}"
                )
        llm_stats = self.stats.get("llm") or {}
        if llm_stats.get("usage"):
            u = llm_stats["usage"]
            lines.append(
                f"  GROQ      {u['calls_made']} calls, {u['cache_hits']} cached, "
                f"{u['keys_active']}/{u['keys_total']} keys | "
                f"entity={llm_stats.get('entity_checked', 0)} "
                f"(gaps {llm_stats.get('entity_gap_filled', 0)}, "
                f"conflicts {llm_stats.get('entity_disagreed', 0)}) "
                f"rationales={llm_stats.get('rationales', 0)} "
                f"challenges={llm_stats.get('challenged', 0)}"
            )
        if self.tavily:
            usage = self.tavily.key_usage()
            lines.append(
                f"  TAVILY    {usage['calls_made']} calls across "
                f"{usage['keys_active']}/{usage['keys_total']} active keys"
            )
        if self.apify:
            usage = self.apify.token_usage()
            lines.append(
                f"  APIFY     {usage['calls_made']} calls across "
                f"{usage['keys_active']}/{usage['keys_total']} active tokens"
            )
        if self.salesnav:
            usage = self.salesnav.token_usage()
            lines.append(
                f"  SALESNAV  {usage['searches_made']} searches / "
                f"{usage['results_returned']} results = ${usage['spend_usd']:.2f} across "
                f"{usage['keys_active']}/{usage['keys_total']} active tokens"
            )
        if self.stats.get("master_path"):
            lines += ["", f"  MASTER {self.stats['master_path']}"]
        if self.stats.get("export_path"):
            lines += ["", f"  EXCEL  {self.stats['export_path']}"]
        lines.append("=" * 72)
        print("\n".join(lines))


# --------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Company research pipeline — discovery to Excel tracker.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--states", nargs="+", metavar="XX", help="limit to these state codes")
    p.add_argument("--only", nargs="+", choices=STAGES, help="run only these stages")
    p.add_argument("--skip", nargs="+", choices=STAGES, help="skip these stages")
    p.add_argument("--resume", metavar="RUN_ID", help="resume an interrupted run")
    p.add_argument("--export-only", action="store_true", help="rebuild the workbook only")
    p.add_argument("--dry-run", action="store_true", help="print the plan, change nothing")
    p.add_argument("--stats", action="store_true", help="print database totals and exit")
    p.add_argument("-v", "--verbose", action="store_true", help="debug logging")
    return p.parse_args()


def main() -> int:
    os_signal.signal(os_signal.SIGINT, _handle_sigint)
    ensure_dirs()
    cfg = Config()
    args = parse_args()

    if args.export_only:
        args.only = ["quality", "export"]

    if args.stats:
        store = Store(DB_PATH)
        print(json.dumps(store.counts(), indent=2))
        recent = store.conn.execute(
            "SELECT run_id, label, started_at, finished_at, status FROM runs"
            " ORDER BY started_at DESC LIMIT 10"
        ).fetchall()
        print("\nRecent runs:")
        for row in recent:
            print(f"  {row['run_id']}  {row['status']:<12} {row['started_at']}")
        store.close()
        return 0

    if args.dry_run:
        states = cfg.selected_states()
        if args.states:
            wanted = {s.upper() for s in args.states}
            states = [s for s in states if s["code"].upper() in wanted]
        groups = cfg.query_groups
        cities_per_state = int(cfg.get("discovery.tavily.cities_per_state", 4))
        city_queries = sum(
            len(g.get("queries", [])) for g in groups.values() if g.get("scope") == "city"
        )
        state_queries = sum(
            len(g.get("queries", [])) for g in groups.values() if g.get("scope") != "city"
        )
        est = len(states) * (state_queries + city_queries * cities_per_state)
        print(f"States           : {len(states)}")
        print(f"Query groups     : {len(groups)}")
        print(f"Est. Tavily calls: {est}  (budget "
              f"{cfg.get('discovery.tavily.max_calls_per_run')})")
        print(f"Overpass calls   : {len(states)}")
        print(f"Tavily keys      : {len(cfg.tavily_api_keys)} in rotation pool")
        # Both gates, exactly as the constructor applies them (see the
        # `self.apify` block): the stage flag alone reported phantom spend
        # while `discovery.apify.enabled: false` made the stage a no-op.
        if cfg.get("stages.apify_discover", True) and cfg.get("discovery.apify.enabled", True):
            apify_est = int(cfg.get("discovery.apify.queries_per_run", 15))
            print(f"Est. Apify calls : {apify_est}  (budget "
                  f"{cfg.get('discovery.apify.max_calls_per_run')}), "
                  f"{len(cfg.apify_tokens)} token(s) in rotation pool")
        if cfg.get("stages.contacts", True):
            # The only stage billed per item, so this estimate is in dollars.
            # Read-only DB access; a locked or missing database degrades to a
            # config-only line rather than breaking --dry-run.
            try:
                store = Store(DB_PATH)
                node = cfg.get("contacts", {}) or {}
                gate = node.get("gate", {}) or {}
                statuses = gate.get("statuses", ["approved"])
                tiers = gate.get("tiers", ["A"])
                where = (f"status IN ({', '.join('?' for _ in statuses)})"
                         f" AND tier IN ({', '.join('?' for _ in tiers)})")
                if gate.get("require_clean_verified", True):
                    where += " AND clean_verified = 1"
                if gate.get("require_linkedin_url", False):
                    where += " AND linkedin_url IS NOT NULL AND linkedin_url != ''"
                gated = store.companies_where(where, (*statuses, *tiers))
                cooldown = int(node.get("search_cooldown_days", 90))
                cutoff = datetime.now().timestamp() - cooldown * 86400
                eligible = [
                    r for r in gated
                    if not r["last_contact_search_at"]
                    or _before(r["last_contact_search_at"], cutoff)
                ]
                batch = max(1, int(node.get("batch_size", 10)))
                depth = int((node.get("depth", {}) or {}).get("A", 4))
                over = float(node.get("over_fetch", 1.5))
                per_search = min(int(node.get("max_results_per_search", 100)),
                                 max(1, int(depth * batch * over)))
                searches = -(-len(eligible) // batch) if eligible else 0
                est_usd = searches * 0.50 + searches * per_search * 0.01
                print(f"Contact accounts : {len(eligible)} eligible of {len(gated)} gated "
                      f"({cooldown}d cooldown)")
                print(f"Est. SalesNav    : ~{searches} searches x $0.50 + up to "
                      f"~{searches * per_search} results x $0.01 = ~${est_usd:.2f}  "
                      f"(cap ${node.get('max_spend_usd_per_run', 30.0)})")
                store.close()
            except Exception as exc:  # noqa: BLE001
                print(f"Contact accounts : (database unavailable: {exc})")
        print(f"Stages enabled   : "
              f"{[s for s in STAGES if cfg.get(f'stages.{s}', True)]}")
        print(f"Database         : {DB_PATH}")
        return 0

    return Pipeline(cfg, args).run()


if __name__ == "__main__":
    sys.exit(main())
