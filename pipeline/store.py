"""SQLite persistence, deduplication ledger and run checkpointing.

The database is the memory that makes repeat runs safe:

  companies      one row per real company, UNIQUE on domain_key
  aliases        alternate names/domains pointing at a company
  evidence       source trail (SOP Module 29/63)
  signals        detected fit / operational / timing signals
  exclusions     permanent "do not research again" ledger (SOP Module 28)
  query_log      which discovery queries already ran, and when
  url_seen       URLs already fetched, so crawls are not repeated
  checkpoints    stage/key completion so an interrupted run resumes
  runs           run history and stats
"""

from __future__ import annotations

import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

from . import dedup

log = logging.getLogger("pipeline.store")

SCHEMA_VERSION = 1

SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key   TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS companies (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    domain_key           TEXT UNIQUE,
    name_key             TEXT,
    phone_key            TEXT,

    company_name         TEXT NOT NULL,
    website              TEXT,
    linkedin_url         TEXT,
    headquarters_city    TEXT,
    headquarters_state   TEXT,
    hq_confirmed         INTEGER DEFAULT 0,
    discovery_region     TEXT,
    source_pipeline      TEXT,
    discovery_route      TEXT DEFAULT 'direct',  -- SOP Module 13: direct | reverse_engineered
    discovery_group      TEXT,  -- queries.yaml group name, for per-group yield reporting
    ats_provider         TEXT,
    open_roles           INTEGER DEFAULT 0,
    entity_type          TEXT,
    llm_entity_type      TEXT,
    llm_entity_conf      REAL DEFAULT 0,
    llm_entity_evidence  TEXT,
    llm_agrees           INTEGER,
    llm_challenge_verdict TEXT,
    llm_challenge_notes  TEXT,
    rationale_source     TEXT,
    entity_confidence    REAL DEFAULT 0,
    entity_evidence      TEXT,
    is_us                INTEGER,
    geo_reason           TEXT,
    name_source          TEXT,
    intent_score         REAL DEFAULT 0,
    intent_summary       TEXT,
    tool_categories      INTEGER DEFAULT 0,
    tools_found          TEXT,
    country              TEXT DEFAULT 'United States',
    phone                TEXT,
    employee_range       TEXT,
    employee_estimate    INTEGER,
    employee_range_source TEXT DEFAULT 'unknown',  -- 'stated' | 'unknown' (never fabricated)

    segment              TEXT,
    services             TEXT,
    recurring_evidence   TEXT,
    customer_industries  TEXT,
    company_description  TEXT,

    fit_reason           TEXT,
    operational_signals  TEXT,
    timing_signal        TEXT,
    exclusions_checked   TEXT,

    score_fit            REAL DEFAULT 0,
    score_need           REAL DEFAULT 0,
    score_timing         REAL DEFAULT 0,
    score_access         REAL DEFAULT 0,
    score_total          REAL DEFAULT 0,
    tier                 TEXT,
    status               TEXT DEFAULT 'raw',
    status_reason        TEXT,

    discovery_source     TEXT,
    discovery_query      TEXT,
    owner                TEXT DEFAULT 'pipeline',
    next_action          TEXT,
    review_notes         TEXT,
    duplicate_status     TEXT DEFAULT 'unique',

    clean_verified       INTEGER DEFAULT 0,
    cleaned_at           TEXT,
    completeness_pct     REAL DEFAULT 0,
    website_reachable    INTEGER,
    crawl_pages          INTEGER DEFAULT 0,
    raw_text_len         INTEGER DEFAULT 0,

    first_seen_run       TEXT,
    first_seen_at        TEXT,
    last_updated_at      TEXT,
    last_enriched_at     TEXT,
    last_news_search_at  TEXT,
    times_seen           INTEGER DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_companies_name_key ON companies(name_key);
CREATE INDEX IF NOT EXISTS idx_companies_phone_key ON companies(phone_key);
CREATE INDEX IF NOT EXISTS idx_companies_state ON companies(headquarters_state);
CREATE INDEX IF NOT EXISTS idx_companies_status ON companies(status);
CREATE INDEX IF NOT EXISTS idx_companies_tier ON companies(tier);

CREATE TABLE IF NOT EXISTS aliases (
    alias_type TEXT NOT NULL,          -- 'domain' | 'name' | 'phone'
    alias_key  TEXT NOT NULL,
    company_id INTEGER NOT NULL,
    created_at TEXT,
    PRIMARY KEY (alias_type, alias_key),
    FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS evidence (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id     INTEGER NOT NULL,
    source_type    TEXT,               -- SOP Module 29 evidence level
    evidence_level INTEGER,
    source_url     TEXT,
    supported_fact TEXT,
    date_checked   TEXT,
    UNIQUE (company_id, source_url, supported_fact),
    FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS signals (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id   INTEGER NOT NULL,
    category     TEXT,                 -- recurring|operational|complexity|vertical|timing|access
    signal       TEXT,
    detail       TEXT,
    source_url   TEXT,
    detected_at  TEXT,
    UNIQUE (company_id, category, signal),
    FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
);

-- Dated, discrete triggers (SOP Module 55: "record the date and source
-- because timing information changes"). Separate from `signals`, which holds
-- ongoing conditions with no natural date. UNIQUE on source_url (not on
-- event_type alone) so a company that makes three acquisitions can carry
-- three rows, each independently decaying by its own event_date.
CREATE TABLE IF NOT EXISTS intent_events (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id     INTEGER NOT NULL,
    event_type     TEXT NOT NULL,
    event_date     TEXT,               -- when the underlying event happened; NULL if unknown
    detected_at    TEXT NOT NULL,      -- when the pipeline found it; always set
    source_url     TEXT,
    source_type    TEXT,
    evidence_level INTEGER,
    detail         TEXT,
    run_id         TEXT,
    UNIQUE (company_id, event_type, source_url),
    FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_intent_events_company ON intent_events(company_id);

CREATE TABLE IF NOT EXISTS exclusions (
    domain_key   TEXT PRIMARY KEY,
    company_name TEXT,
    name_key     TEXT,
    reason       TEXT,
    excluded_at  TEXT,
    run_id       TEXT
);

CREATE TABLE IF NOT EXISTS query_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    source       TEXT,
    query        TEXT,
    scope_key    TEXT,
    result_count INTEGER,
    new_count    INTEGER,
    run_id       TEXT,
    executed_at  TEXT,
    group_name   TEXT,  -- queries.yaml group, for the qualified-per-credit scorecard
    UNIQUE (source, query, scope_key)
);

CREATE TABLE IF NOT EXISTS url_seen (
    url         TEXT PRIMARY KEY,
    status_code INTEGER,
    fetched_at  TEXT,
    company_id  INTEGER
);

CREATE TABLE IF NOT EXISTS checkpoints (
    run_id  TEXT NOT NULL,
    stage   TEXT NOT NULL,
    key     TEXT NOT NULL,
    done_at TEXT,
    PRIMARY KEY (run_id, stage, key)
);

CREATE TABLE IF NOT EXISTS runs (
    run_id      TEXT PRIMARY KEY,
    label       TEXT,
    started_at  TEXT,
    finished_at TEXT,
    status      TEXT,
    stats_json  TEXT
);

CREATE TABLE IF NOT EXISTS jobs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id  INTEGER NOT NULL,
    title       TEXT,
    location    TEXT,
    posted_at   TEXT,
    url         TEXT,
    provider    TEXT,
    is_operational INTEGER DEFAULT 0,
    tools_named TEXT,
    fetched_at  TEXT,
    UNIQUE (company_id, title, url),
    FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS learning_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id      TEXT,
    kind        TEXT,
    observation TEXT,
    detail      TEXT,
    created_at  TEXT
);

-- Decision-makers inside approved accounts (SOP Modules 41-44, 63).
-- UNIQUE is (company_id, profile_key), deliberately NOT profile_key alone: a
-- global unique would make "the same LinkedIn person attributed to two of our
-- companies" a silent no-op on insert, and that event is precisely the symptom
-- of a mis-batched search (see pipeline/contacts/mapping.py). It has to stay
-- visible. The non-unique index on profile_key keeps the audit query cheap.
CREATE TABLE IF NOT EXISTS contacts (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id       INTEGER NOT NULL,
    profile_key      TEXT NOT NULL,      -- dedup key, see dedup.profile_key()

    full_name        TEXT,
    first_name       TEXT,
    last_name        TEXT,
    job_title        TEXT,
    role_type        TEXT,               -- economic|technical|operational|champion|risk
    seniority        TEXT,
    linkedin_url     TEXT,
    profile_id       TEXT,

    -- Which LinkedIn company the actor said this person works at, and how we
    -- tied that back to our own company row.
    li_company_id    TEXT,
    li_company_name  TEXT,
    match_method     TEXT,               -- li_company_id|exact_name|fuzzy_name
    match_confidence REAL DEFAULT 0,

    location         TEXT,
    us_state         TEXT,
    is_us            INTEGER,

    email            TEXT,
    email_status     TEXT,
    about            TEXT,
    relevance        TEXT,               -- Module 44 "reason selected"
    rank_score       REAL DEFAULT 0,

    status           TEXT DEFAULT 'candidate',  -- candidate|accepted|review|rejected
    reject_reason    TEXT,

    first_seen_run   TEXT,
    first_seen_at    TEXT,
    last_seen_run    TEXT,
    last_updated_at  TEXT,
    date_checked     TEXT,
    UNIQUE (company_id, profile_key),
    FOREIGN KEY (company_id) REFERENCES companies(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_contacts_company ON contacts(company_id);
CREATE INDEX IF NOT EXISTS idx_contacts_profile ON contacts(profile_key);
CREATE INDEX IF NOT EXISTS idx_contacts_status  ON contacts(status);

-- Spend audit and free resume cache for the paid Sales Navigator actor.
-- search_key is a content hash of the canonicalised request payload, so an
-- identical batch is never re-bought — not even across run ids. Same
-- philosophy as GroqClient's content-hash cache and the url_seen table.
CREATE TABLE IF NOT EXISTS contact_searches (
    search_key    TEXT PRIMARY KEY,
    request_id    TEXT,
    input_json    TEXT,
    response_json TEXT,
    status        TEXT,
    result_count  INTEGER DEFAULT 0,
    cost_usd      REAL DEFAULT 0,
    run_id        TEXT,
    created_at    TEXT
);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Store:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path, timeout=60, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.executescript(SCHEMA)
        self._migrate()
        self.conn.execute(
            "INSERT OR REPLACE INTO schema_meta(key, value) VALUES ('version', ?)",
            (str(SCHEMA_VERSION),),
        )
        self.conn.commit()

    def _add_columns(self, table: str, columns: dict[str, str]) -> None:
        """ALTER in any of `columns` the table does not already have.

        CREATE TABLE IF NOT EXISTS is a no-op on an existing table, so columns
        introduced after a database was first created have to be added
        explicitly for older databases to keep working. Idempotent.
        """
        existing = {
            row["name"] for row in self.conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
        for column, ddl in columns.items():
            if column not in existing:
                self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")

    def _migrate(self) -> None:
        """Bring an older database up to the current schema."""
        added = {
            "hq_confirmed": "INTEGER DEFAULT 0",
            "discovery_region": "TEXT",
            "source_pipeline": "TEXT",
            "ats_provider": "TEXT",
            "open_roles": "INTEGER DEFAULT 0",
            "entity_type": "TEXT",
            "llm_entity_type": "TEXT",
            "llm_entity_conf": "REAL DEFAULT 0",
            "llm_entity_evidence": "TEXT",
            "llm_agrees": "INTEGER",
            "llm_challenge_verdict": "TEXT",
            "llm_challenge_notes": "TEXT",
            "rationale_source": "TEXT",
            "clean_verified": "INTEGER DEFAULT 0",
            "cleaned_at": "TEXT",
            "entity_confidence": "REAL DEFAULT 0",
            "entity_evidence": "TEXT",
            "is_us": "INTEGER",
            "geo_reason": "TEXT",
            "name_source": "TEXT",
            "intent_score": "REAL DEFAULT 0",
            "intent_summary": "TEXT",
            "tool_categories": "INTEGER DEFAULT 0",
            "tools_found": "TEXT",
            # Cross-run spend guard: without this a company that already had a
            # paid Tavily news search gets searched again on every future run,
            # forever, since the checkpoints table resets per run_id.
            "last_news_search_at": "TEXT",
            "discovery_route": "TEXT DEFAULT 'direct'",
            "employee_range_source": "TEXT DEFAULT 'unknown'",
            "discovery_group": "TEXT",
            # Cross-run spend guard for the paid Sales Navigator actor, exactly
            # like last_news_search_at above: without it, every run re-buys a
            # $0.50 search for the same company forever, since `checkpoints`
            # resets per run_id.
            "last_contact_search_at": "TEXT",
            # Learned LinkedIn organisation id. Once a person has been matched
            # to this company with high confidence, later runs can filter by
            # exact org id instead of company name — which removes the
            # wrong-company risk in batched searches entirely.
            "linkedin_company_id": "TEXT",
            "contact_search_status": "TEXT",  # searched|no_results|match_failed
        }
        self._add_columns("companies", added)
        self._add_columns("query_log", {"group_name": "TEXT"})

        self._backfill_intent_events()

    def _backfill_intent_events(self) -> None:
        """One-time move of pre-Change-1 timing signals into intent_events.

        Earlier runs stored timing signals in `signals` (category='timing'),
        which score_intent no longer reads — timing now lives entirely in
        `intent_events`. Without this backfill, a database that already has
        scored companies would show zero timing/intent score until every
        company is re-crawled. We don't know the true event date for these
        old rows, only when they were crawled, so they migrate as undated
        events: recency_multiplier's confidence penalty applies, same as any
        other undated event today. Self-idempotent — once migrated, the
        source rows are deleted, so later runs find nothing left to do.
        """
        legacy = self.conn.execute(
            "SELECT company_id, signal, detail, source_url, detected_at"
            " FROM signals WHERE category='timing'"
        ).fetchall()
        if not legacy:
            return
        for row in legacy:
            self.conn.execute(
                "INSERT OR IGNORE INTO intent_events"
                "(company_id, event_type, event_date, detected_at, source_url,"
                " source_type, evidence_level, detail, run_id)"
                " VALUES (?,?,NULL,?,?,?,?,?,NULL)",
                (row["company_id"], row["signal"], row["detected_at"], row["source_url"],
                 "Legacy signal (pre-Change 1)", 2, row["detail"]),
            )
        self.conn.execute("DELETE FROM signals WHERE category='timing'")
        self.conn.commit()
        log.info("Backfilled %d legacy timing signal(s) into intent_events", len(legacy))

    # -- plumbing ------------------------------------------------------------

    @contextmanager
    def tx(self) -> Iterator[sqlite3.Connection]:
        try:
            yield self.conn
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def close(self) -> None:
        self.conn.commit()
        self.conn.close()

    # -- runs ----------------------------------------------------------------

    def start_run(self, run_id: str, label: str) -> None:
        with self.tx() as c:
            c.execute(
                "INSERT OR REPLACE INTO runs(run_id, label, started_at, status) VALUES (?,?,?,?)",
                (run_id, label, _now(), "running"),
            )

    def finish_run(self, run_id: str, status: str, stats: dict[str, Any]) -> None:
        with self.tx() as c:
            c.execute(
                "UPDATE runs SET finished_at=?, status=?, stats_json=? WHERE run_id=?",
                (_now(), status, json.dumps(stats, default=str), run_id),
            )

    # -- checkpoints ---------------------------------------------------------

    def is_done(self, run_id: str, stage: str, key: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM checkpoints WHERE run_id=? AND stage=? AND key=?",
            (run_id, stage, key),
        ).fetchone()
        return row is not None

    def mark_done(self, run_id: str, stage: str, key: str) -> None:
        with self.tx() as c:
            c.execute(
                "INSERT OR REPLACE INTO checkpoints(run_id, stage, key, done_at) VALUES (?,?,?,?)",
                (run_id, stage, key, _now()),
            )

    # -- exclusions ----------------------------------------------------------

    def is_excluded(self, domain_key: str | None, name_key: str | None = None) -> str | None:
        """Return the exclusion reason if this company was already ruled out."""
        if domain_key:
            row = self.conn.execute(
                "SELECT reason FROM exclusions WHERE domain_key=?", (domain_key,)
            ).fetchone()
            if row:
                return row["reason"]
        if name_key:
            row = self.conn.execute(
                "SELECT reason FROM exclusions WHERE name_key=?", (name_key,)
            ).fetchone()
            if row:
                return row["reason"]
        return None

    def add_exclusion(
        self, domain_key: str | None, name: str, name_key: str | None, reason: str, run_id: str
    ) -> None:
        if not domain_key and not name_key:
            return
        with self.tx() as c:
            c.execute(
                "INSERT OR REPLACE INTO exclusions"
                "(domain_key, company_name, name_key, reason, excluded_at, run_id)"
                " VALUES (?,?,?,?,?,?)",
                (domain_key or f"name:{name_key}", name, name_key, reason, _now(), run_id),
            )

    # -- dedup lookup --------------------------------------------------------

    def find_existing(
        self,
        domain_key: str | None,
        name_key: str | None,
        phone_key: str | None,
        state: str | None = None,
        fuzzy_threshold: float = 92.0,
    ) -> tuple[int | None, str | None]:
        """Locate an existing company by any dedup key.

        Returns (company_id, match_method) or (None, None).
        """
        # 1. Exact registrable domain — strongest signal.
        if domain_key:
            row = self.conn.execute(
                "SELECT id FROM companies WHERE domain_key=?", (domain_key,)
            ).fetchone()
            if row:
                return row["id"], "domain"
            row = self.conn.execute(
                "SELECT company_id FROM aliases WHERE alias_type='domain' AND alias_key=?",
                (domain_key,),
            ).fetchone()
            if row:
                return row["company_id"], "domain_alias"

        # 2. Exact normalised name within the same state.
        if name_key:
            if state:
                row = self.conn.execute(
                    "SELECT id FROM companies WHERE name_key=? AND headquarters_state=?",
                    (name_key, state),
                ).fetchone()
            else:
                row = self.conn.execute(
                    "SELECT id FROM companies WHERE name_key=?", (name_key,)
                ).fetchone()
            if row:
                return row["id"], "name"
            row = self.conn.execute(
                "SELECT company_id FROM aliases WHERE alias_type='name' AND alias_key=?",
                (name_key,),
            ).fetchone()
            if row:
                return row["company_id"], "name_alias"

        # 3. Same phone number.
        if phone_key:
            row = self.conn.execute(
                "SELECT id FROM companies WHERE phone_key=?", (phone_key,)
            ).fetchone()
            if row:
                return row["id"], "phone"

        # 4. Fuzzy name match, scoped to the state to keep it cheap and safe.
        if name_key and state:
            candidates = self.conn.execute(
                "SELECT id, name_key FROM companies WHERE headquarters_state=? AND name_key IS NOT NULL",
                (state,),
            ).fetchall()
            for cand in candidates:
                if dedup.is_near_duplicate(name_key, cand["name_key"], fuzzy_threshold):
                    return cand["id"], "fuzzy_name"

        return None, None

    # -- company upsert ------------------------------------------------------

    def upsert_company(
        self,
        record: dict[str, Any],
        run_id: str,
        fuzzy_threshold: float = 92.0,
    ) -> tuple[int, bool]:
        """Insert a new company or merge into the existing one.

        Returns (company_id, created). `created` is False when the candidate was
        recognised as a company already in the database.
        """
        name = (record.get("company_name") or "").strip()
        website = record.get("website")
        phone = record.get("phone")
        keys = dedup.build_keys(name, website, phone)
        state = record.get("headquarters_state")

        existing_id, method = self.find_existing(
            keys["domain_key"], keys["name_key"], keys["phone_key"], state, fuzzy_threshold
        )

        if existing_id is not None:
            self._merge_into(existing_id, record, keys, method or "unknown")
            return existing_id, False

        now = _now()
        payload = {
            "domain_key": keys["domain_key"],
            "name_key": keys["name_key"],
            "phone_key": keys["phone_key"],
            "company_name": name,
            "website": keys["canonical_website"] or website,
            "first_seen_run": run_id,
            "first_seen_at": now,
            "last_updated_at": now,
            "times_seen": 1,
        }
        for field in (
            "linkedin_url", "headquarters_city", "headquarters_state", "country", "phone",
            "employee_range", "employee_estimate", "segment", "services",
            "customer_industries", "company_description", "discovery_source",
            "discovery_query", "status", "discovery_region", "source_pipeline",
            "discovery_route", "discovery_group",
        ):
            if record.get(field) is not None:
                payload[field] = record[field]
        payload.setdefault("status", "raw")

        cols = ", ".join(payload)
        marks = ", ".join("?" for _ in payload)
        with self.tx() as c:
            cur = c.execute(
                f"INSERT INTO companies ({cols}) VALUES ({marks})", list(payload.values())
            )
            company_id = int(cur.lastrowid)
            self._register_aliases(c, company_id, keys)
        return company_id, True

    def _merge_into(
        self, company_id: int, record: dict[str, Any], keys: dict[str, Any], method: str
    ) -> None:
        """Fill blank fields on an existing record; never overwrite good data."""
        current = self.get_company(company_id)
        if current is None:
            return
        updates: dict[str, Any] = {}
        mergeable = (
            "website", "linkedin_url", "headquarters_city", "headquarters_state",
            "phone", "employee_range", "employee_estimate", "segment", "services",
            "customer_industries", "company_description",
        )
        for field in mergeable:
            new_val = record.get(field)
            if new_val in (None, "", 0):
                continue
            if not current[field]:
                updates[field] = new_val

        # Keep a trail of every query/source that surfaced this company.
        for field in ("discovery_source", "discovery_query"):
            new_val = record.get(field)
            if not new_val:
                continue
            existing_val = current[field] or ""
            if new_val not in existing_val:
                merged = f"{existing_val}; {new_val}".strip("; ")
                updates[field] = merged[:2000]

        updates["times_seen"] = (current["times_seen"] or 1) + 1
        updates["last_updated_at"] = _now()
        updates["duplicate_status"] = f"merged_via_{method}"

        # A better website may arrive later than the first sighting.
        if not current["domain_key"] and keys["domain_key"]:
            updates["domain_key"] = keys["domain_key"]
        if not current["phone_key"] and keys["phone_key"]:
            updates["phone_key"] = keys["phone_key"]

        sets = ", ".join(f"{k}=?" for k in updates)
        with self.tx() as c:
            c.execute(
                f"UPDATE companies SET {sets} WHERE id=?", [*updates.values(), company_id]
            )
            self._register_aliases(c, company_id, keys)

    @staticmethod
    def _register_aliases(conn: sqlite3.Connection, company_id: int, keys: dict[str, Any]) -> None:
        now = _now()
        for alias_type, key in (
            ("domain", keys.get("domain_key")),
            ("name", keys.get("name_key")),
            ("phone", keys.get("phone_key")),
        ):
            if not key:
                continue
            conn.execute(
                "INSERT OR IGNORE INTO aliases(alias_type, alias_key, company_id, created_at)"
                " VALUES (?,?,?,?)",
                (alias_type, key, company_id, now),
            )

    def update_company(self, company_id: int, **fields: Any) -> None:
        if not fields:
            return
        fields["last_updated_at"] = _now()
        sets = ", ".join(f"{k}=?" for k in fields)
        with self.tx() as c:
            c.execute(f"UPDATE companies SET {sets} WHERE id=?", [*fields.values(), company_id])

    def get_company(self, company_id: int) -> sqlite3.Row | None:
        return self.conn.execute("SELECT * FROM companies WHERE id=?", (company_id,)).fetchone()

    def companies_where(self, where: str = "1=1", params: tuple = ()) -> list[sqlite3.Row]:
        return self.conn.execute(f"SELECT * FROM companies WHERE {where}", params).fetchall()

    # -- evidence & signals --------------------------------------------------

    def add_evidence(
        self,
        company_id: int,
        source_type: str,
        source_url: str | None,
        supported_fact: str,
        evidence_level: int = 1,
    ) -> None:
        with self.tx() as c:
            c.execute(
                "INSERT OR IGNORE INTO evidence"
                "(company_id, source_type, evidence_level, source_url, supported_fact, date_checked)"
                " VALUES (?,?,?,?,?,?)",
                (company_id, source_type, evidence_level, source_url, supported_fact, _now()[:10]),
            )

    def add_signal(
        self,
        company_id: int,
        category: str,
        signal: str,
        detail: str | None = None,
        source_url: str | None = None,
    ) -> None:
        with self.tx() as c:
            c.execute(
                "INSERT OR IGNORE INTO signals"
                "(company_id, category, signal, detail, source_url, detected_at)"
                " VALUES (?,?,?,?,?,?)",
                (company_id, category, signal, detail, source_url, _now()),
            )

    def get_signals(self, company_id: int) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM signals WHERE company_id=?", (company_id,)
        ).fetchall()

    def add_intent_event(
        self,
        company_id: int,
        event_type: str,
        detail: str | None = None,
        source_url: str | None = None,
        source_type: str | None = None,
        evidence_level: int = 2,
        event_date: str | None = None,
        run_id: str | None = None,
    ) -> None:
        """Record a dated, discrete timing trigger (SOP Module 55).

        `event_date` is when the underlying thing happened (an article's
        publish date, a job's posted_at) — pass None when it genuinely cannot
        be found; score_intent applies a confidence penalty rather than
        treating an undated event as perpetually fresh.
        """
        with self.tx() as c:
            c.execute(
                "INSERT OR IGNORE INTO intent_events"
                "(company_id, event_type, event_date, detected_at, source_url,"
                " source_type, evidence_level, detail, run_id)"
                " VALUES (?,?,?,?,?,?,?,?,?)",
                (company_id, event_type, event_date, _now(), source_url,
                 source_type, evidence_level, detail, run_id),
            )

    def get_intent_events(self, company_id: int) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM intent_events WHERE company_id=?", (company_id,)
        ).fetchall()

    def get_evidence(self, company_id: int) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM evidence WHERE company_id=? ORDER BY evidence_level", (company_id,)
        ).fetchall()

    # -- query log -----------------------------------------------------------

    def query_recently_run(self, source: str, query: str, scope_key: str, cooldown_days: int) -> bool:
        row = self.conn.execute(
            "SELECT executed_at FROM query_log WHERE source=? AND query=? AND scope_key=?",
            (source, query, scope_key),
        ).fetchone()
        if not row or not row["executed_at"]:
            return False
        try:
            last = datetime.fromisoformat(row["executed_at"])
        except ValueError:
            return False
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) - last < timedelta(days=cooldown_days)

    def log_query(
        self,
        source: str,
        query: str,
        scope_key: str,
        result_count: int,
        new_count: int,
        run_id: str,
        group_name: str | None = None,
    ) -> None:
        with self.tx() as c:
            c.execute(
                "INSERT OR REPLACE INTO query_log"
                "(source, query, scope_key, result_count, new_count, run_id, executed_at, group_name)"
                " VALUES (?,?,?,?,?,?,?,?)",
                (source, query, scope_key, result_count, new_count, run_id, _now(), group_name),
            )

    def recent_queries(self, source: str, limit: int = 50) -> list[str]:
        """Distinct recent query text for a source — feeds
        llm_tasks.generate_fresh_queries' "don't repeat these" negative list."""
        rows = self.conn.execute(
            "SELECT DISTINCT query FROM query_log WHERE source=? ORDER BY executed_at DESC LIMIT ?",
            (source, limit),
        ).fetchall()
        return [r["query"] for r in rows]

    # -- url ledger ----------------------------------------------------------

    def url_was_seen(self, url: str) -> bool:
        return (
            self.conn.execute("SELECT 1 FROM url_seen WHERE url=?", (url,)).fetchone() is not None
        )

    def mark_url_seen(self, url: str, status_code: int | None, company_id: int | None = None) -> None:
        with self.tx() as c:
            c.execute(
                "INSERT OR REPLACE INTO url_seen(url, status_code, fetched_at, company_id)"
                " VALUES (?,?,?,?)",
                (url, status_code, _now(), company_id),
            )

    # -- jobs ----------------------------------------------------------------

    def add_job(self, company_id: int, job: dict[str, Any], provider: str,
                is_operational: bool, tools_named: str = "") -> None:
        with self.tx() as c:
            c.execute(
                "INSERT OR IGNORE INTO jobs"
                "(company_id, title, location, posted_at, url, provider,"
                " is_operational, tools_named, fetched_at)"
                " VALUES (?,?,?,?,?,?,?,?,?)",
                (company_id, job.get("title"), job.get("location"),
                 job.get("posted_at"), job.get("url"), provider,
                 1 if is_operational else 0, tools_named, _now()),
            )

    def get_jobs(self, company_id: int) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM jobs WHERE company_id=? ORDER BY posted_at DESC", (company_id,)
        ).fetchall()

    # -- decision-makers (SOP Modules 41-44, 63) -----------------------------

    CONTACT_FIELDS = (
        "full_name", "first_name", "last_name", "job_title", "role_type", "seniority",
        "linkedin_url", "profile_id", "li_company_id", "li_company_name",
        "match_method", "match_confidence", "location", "us_state", "is_us",
        "email", "email_status", "about", "relevance", "rank_score",
        "status", "reject_reason",
    )

    def upsert_contact(
        self, company_id: int, record: dict[str, Any], run_id: str
    ) -> tuple[int, bool]:
        """Insert a person, or refresh one already known for this company.

        Returns (contact_id, created). `first_seen_run` and `first_seen_at` are
        stamped once and never touched again — that is what makes the master
        workbook's "Date Added" column trustworthy across runs.
        """
        key = record.get("profile_key")
        if not key:
            raise ValueError("upsert_contact requires a profile_key")
        now = _now()
        values = {f: record.get(f) for f in self.CONTACT_FIELDS}

        with self.tx() as c:
            existing = c.execute(
                "SELECT id FROM contacts WHERE company_id=? AND profile_key=?",
                (company_id, key),
            ).fetchone()
            if existing:
                sets = ", ".join(f"{f}=?" for f in self.CONTACT_FIELDS)
                c.execute(
                    f"UPDATE contacts SET {sets}, last_seen_run=?, last_updated_at=?,"
                    " date_checked=? WHERE id=?",
                    [*values.values(), run_id, now, now[:10], existing["id"]],
                )
                return int(existing["id"]), False

            columns = ", ".join(self.CONTACT_FIELDS)
            marks = ", ".join("?" for _ in self.CONTACT_FIELDS)
            cur = c.execute(
                f"INSERT INTO contacts(company_id, profile_key, {columns},"
                " first_seen_run, first_seen_at, last_seen_run, last_updated_at, date_checked)"
                f" VALUES (?,?,{marks},?,?,?,?,?)",
                [company_id, key, *values.values(), run_id, now, run_id, now, now[:10]],
            )
            return int(cur.lastrowid), True

    def get_contacts(
        self, company_id: int, status: str | None = "accepted"
    ) -> list[sqlite3.Row]:
        if status is None:
            return self.conn.execute(
                "SELECT * FROM contacts WHERE company_id=? ORDER BY rank_score DESC",
                (company_id,),
            ).fetchall()
        return self.conn.execute(
            "SELECT * FROM contacts WHERE company_id=? AND status=? ORDER BY rank_score DESC",
            (company_id, status),
        ).fetchall()

    def contacts_where(self, where: str = "1=1", params: tuple = ()) -> list[sqlite3.Row]:
        return self.conn.execute(f"SELECT * FROM contacts WHERE {where}", params).fetchall()

    def contact_counts(self, status: str = "accepted") -> dict[int, int]:
        """{company_id: n} for companies that have at least one contact."""
        rows = self.conn.execute(
            "SELECT company_id, COUNT(*) n FROM contacts WHERE status=? GROUP BY company_id",
            (status,),
        ).fetchall()
        return {int(r["company_id"]): int(r["n"]) for r in rows}

    def record_contact_search(
        self,
        search_key: str,
        input_json: str,
        response_json: str,
        status: str,
        result_count: int,
        cost_usd: float,
        run_id: str,
        request_id: str | None = None,
    ) -> None:
        with self.tx() as c:
            c.execute(
                "INSERT OR REPLACE INTO contact_searches"
                "(search_key, request_id, input_json, response_json, status,"
                " result_count, cost_usd, run_id, created_at)"
                " VALUES (?,?,?,?,?,?,?,?,?)",
                (search_key, request_id, input_json, response_json, status,
                 result_count, cost_usd, run_id, _now()),
            )

    def get_contact_search(self, search_key: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM contact_searches WHERE search_key=?", (search_key,)
        ).fetchone()

    # -- learning log (SOP Module 15) ---------------------------------------

    def add_learning(self, run_id: str, kind: str, observation: str, detail: str = "") -> None:
        with self.tx() as c:
            c.execute(
                "INSERT INTO learning_log(run_id, kind, observation, detail, created_at)"
                " VALUES (?,?,?,?,?)",
                (run_id, kind, observation, detail, _now()),
            )

    # -- stats ---------------------------------------------------------------

    def counts(self) -> dict[str, int]:
        def one(sql: str, params: tuple = ()) -> int:
            row = self.conn.execute(sql, params).fetchone()
            return int(row[0]) if row else 0

        return {
            "total_companies": one("SELECT COUNT(*) FROM companies"),
            "approved": one("SELECT COUNT(*) FROM companies WHERE status='approved'"),
            "review": one("SELECT COUNT(*) FROM companies WHERE status='review'"),
            "excluded": one("SELECT COUNT(*) FROM companies WHERE status='excluded'"),
            "raw": one("SELECT COUNT(*) FROM companies WHERE status='raw'"),
            "tier_a": one("SELECT COUNT(*) FROM companies WHERE tier='A'"),
            "tier_b": one("SELECT COUNT(*) FROM companies WHERE tier='B'"),
            "tier_c": one("SELECT COUNT(*) FROM companies WHERE tier='C'"),
            "msp_operators": one("SELECT COUNT(*) FROM companies WHERE entity_type='msp_operator'"),
            "software_vendors": one("SELECT COUNT(*) FROM companies WHERE entity_type='software_vendor'"),
            "directories": one("SELECT COUNT(*) FROM companies WHERE entity_type='directory_media'"),
            "non_us": one("SELECT COUNT(*) FROM companies WHERE is_us=0"),
            "exclusion_ledger": one("SELECT COUNT(*) FROM exclusions"),
            "evidence_rows": one("SELECT COUNT(*) FROM evidence"),
            "signal_rows": one("SELECT COUNT(*) FROM signals"),
            "contacts_total": one("SELECT COUNT(*) FROM contacts"),
            "contacts_accepted": one("SELECT COUNT(*) FROM contacts WHERE status='accepted'"),
            "companies_with_contacts": one(
                "SELECT COUNT(DISTINCT company_id) FROM contacts WHERE status='accepted'"
            ),
            "tier_a_without_contacts": one(
                "SELECT COUNT(*) FROM companies WHERE tier='A' AND status='approved'"
                " AND clean_verified=1 AND id NOT IN"
                " (SELECT company_id FROM contacts WHERE status='accepted')"
            ),
        }
