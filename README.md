# Company Research Pipeline

Automated company discovery and qualification for the US market, implementing
`Company_and_Decision_Maker_Research_SOP_Complete.md`. It discovers companies,
verifies them against their own websites, scores them with the SOP's weighted
model, and produces a structured Excel tracker.

**Current scope: companies.** Decision-maker mapping is scaffolded (schema and
worksheet exist) but not yet populated — that is phase 2.

---

## Quick start

```bash
cp .env.example .env      # add TAVILY_API_KEYS and (optional) GROQ_API_KEYS
./run.sh --dry-run        # show the plan and the API budget
./run.sh --states DE      # try one small state first
./run.sh                  # full 50-state run
```

`run.sh` creates the `.venv`, installs dependencies, and starts the pipeline.
If you prefer to drive Python directly: `.venv/bin/python run.py [flags]`.

---

## The run switch

`run.py` is the single entry point. It runs nine stages in order:

```
                overpass ─┐
                tavily   ─┼─→ [ dedup store ] ─→ enrich ─→ intent ─→ qualify
                ats_jobs ─┘                                              │
                                                                        ▼
        export ←─ quality ←─ clean ←─ llm ←─ score ←──────────────────────┘
```

Discovery and signal pipelines converge on one funnel (SOP Module 13: both
routes feed the same validation "to prevent inconsistent standards across
different sources"). Convergence is safe because the dedup store's four keys
merge the same company arriving from several pipelines, checkpoints are
per-`(run_id, stage, key)` so each resumes independently, and every provider
carries its own budget.

Each company records `source_pipeline`, so the run report prints
qualified-account rate **by pipeline** — the SOP Module 69 weekly question,
"which discovery sources produced the best qualified-account rate":

```
  PIPELINE YIELD (qualified-account rate by source)
    pipeline          found  appr tierA   rate  score intent
    tavily              291    45    17  15.5%   28.4    3.6
    overpass              5     0     0   0.0%   32.0    3.6
```

| Flag | Effect |
|---|---|
| *(none)* | Full run using `config/settings.yaml` |
| `--states TX CA NY` | Limit the geography for this run |
| `--only discover` | Run a single stage |
| `--skip intent` | Run everything except one stage |
| `--resume RUN_ID` | Continue an interrupted run from its checkpoints |
| `--export-only` | Rebuild the workbook from existing data — no network |
| `--dry-run` | Print the plan and API budget, change nothing |
| `--stats` | Print database totals and recent runs |
| `-v` | Debug logging |

Stages can also be toggled permanently in `config/settings.yaml` under `stages:`.

`Ctrl-C` stops cleanly after the current item and tells you the run ID to resume.

---

## What each stage does

**1. discover** — builds the raw account universe from two free sources:
- **OpenStreetMap Overpass** — geo-located `office=it` / `office=telecommunication`
  records per state. No API key, no cost.
- **Tavily search** — runs the SOP query library (`config/queries.yaml`,
  Modules 36/37/58) per state and per city.

**2. enrich** — the verification step the SOP insists on (Module 38). Crawls the
homepage plus services, about, industries, careers, contact and case-study
pages. Extracts services, customer industries, headcount, phone, LinkedIn URL,
and records every detected signal with the page URL it came from as
**Level 1 (direct) evidence**.

**3. intent** — see [the intent model](#the-intent-model) below.

**4. qualify** — exclusion rules and the minimum acceptance rule (Modules 26/28).
A company must show a recurring service model, operational responsibility, and
buyer access to be `approved`. Relevant-but-unproven companies become `review`
rather than being forced into the approved list.

**5. score** — the Module 30 weighted model:

| Category | Weight |
|---|---|
| Ideal company fit | 40 |
| Operational need | 25 |
| Timing signals | 20 |
| Decision-maker access | 15 |

75–100 = Tier A, 55–74 = Tier B, 35–54 = Tier C, below 35 = Exclude.
A fit rationale is generated in the Module 63 format:
*[business model] + [operational complexity] + [problem relevance] + [timing] + [buyer access]*.

**6. llm** — Groq assistance, gated hard (see [LLM usage](#llm-usage)).

**7. clean** — the final verification pass. Re-checks entity type, geography,
name quality and evidence; repairs what it can, demotes what it cannot, and
reports a defect rate by class. Demotion is never deletion: a rejected record
moves to `excluded` with a reason and stays in the database.

**8. quality** — end-of-run gates: field completeness, residual duplicates,
live link verification, Level 1 evidence coverage, and score/tier consistency.
Issues are reported in the log and in the workbook rather than shipped silently.

**9. export** — the Excel tracker, written to a unique dated filename:
`companies_<label>_<YYYY-MM-DD>_<HHMMSS>_<runid>.xlsx`, so two runs on the same
day never collide and every file traces back to its run log.

---

## The intent model

The SOP gives timing 20 of 100 points and warns against awarding them "merely
because the company posted recently". So intent here is a composite of signals
that connect to operational need, each decaying with age
(`pipeline/intent_signals.py`).

### Pipeline 1 — ATS job boards (`pipeline/discovery/ats.py`)

Hiring is the strongest first-party signal, and a text scan of a careers page
only answers "is a NOC Manager mentioned". Structured postings give titles,
**real posting dates**, locations, counts and full descriptions — and job
descriptions name the actual RMM/PSA/monitoring stack far more explicitly than
marketing copy.

Three routes, tried in order:

1. **Public ATS JSON APIs** — Greenhouse, Lever, Ashby, Workable,
   SmartRecruiters, Recruitee. Unauthenticated endpoints intended to be consumed.
2. **SMB HR-payroll platforms** — Paylocity, ADP Workforce Now, isolved,
   BambooHR, JazzHR, ApplicantPro, CareerPlug, Breezy and others. *Measured on
   real MSP careers pages, these — not Greenhouse/Lever — are what this ICP
   actually runs.* Greenhouse/Lever/Ashby scored **0 hits across 30 MSPs**.
3. **schema.org `JobPosting`**, then a heuristic title extractor for the common
   case: a mid-market MSP listing openings as plain links on its own careers
   page, with no ATS at all.

Together these lifted job coverage from 2 companies to **73 of 296**, finding
real openings like "Escalations Engineer", "Service Desk Specialist" and
"Support Engineer Tier 2".

**Deliberately not used:** Indeed, Glassdoor, ZipRecruiter, LinkedIn. Their
terms prohibit scraping, they sit behind aggressive bot protection, and they
are already on the aggregator blocklist because job boards kept being saved as
companies.

Roles are split by seniority: a **leadership** hire (Service Delivery Manager,
NOC Manager) means the company is building the function that owns the problem
and scores `hiring_operational`; **front-line** hiring (Service Desk Specialist,
Escalations Engineer) only shows the function is growing and scores the weaker
`hiring_context`.

### Signal tiers

**Tier 1 — first-party, from the company's own site. Free and strongest.**

| Signal | Why it means *now* | Pts |
|---|---|---|
| `hiring_operational` | Hiring a Service Delivery / NOC / Incident / Change manager — the roles that own the problem | 9 |
| `tool_fragmentation` | Runs tools across 4+ categories (RMM, PSA, monitoring, ticketing, docs, backup, security, cloud, identity, network). **This is the pain itself**, not a proxy for it | 7 |
| `compliance_pursuit` | SOC 2 Type II / HITRUST / CMMC underway — creates an evidence, ownership and change-control burden | 6 |
| `incident_history` | Publishes postmortems / RCAs — acute, admitted pain | 5 |
| `hiring_volume` | Two or more operational roles open at once — building a function, not backfilling | 4 |
| `status_page` | Public status page — has committed to service availability | 4 |
| `client_scale_claim` | "500+ clients managed" — operational load | 3 |

**Tier 2 — ecosystem participation:** `vendor_partner` (RMM/PSA/cloud partner
programmes), `association_member` (CompTIA, ASCII, technology councils),
`recent_award` (MSP 501, CRN, Inc. 5000) — 2–3 pts each.

**Tier 3 — business change**, via Tavily news: `acquisition` (8),
`transformation` (6), `expansion` (5), `new_service` (4),
`leadership_change` (4), `funding` (4), `growth` (3).

**Tier 4 — voice:** `operational_content` (3) — leaders writing about MTTR,
alert fatigue, tool sprawl, tribal knowledge.

**Recency decay.** Every signal is multiplied by `0.5 ^ (age / 180 days)`,
floored at 0.25. A three-year-old acquisition scores a quarter of a fresh one.
The total is capped at the SOP's 20 points and written to `Intent Score` with a
plain-English `Intent Summary`.

Why tool fragmentation carries so much weight: it is the only signal that
measures the *actual problem* — an operator running nine disconnected tools has
the fragmented operational context the platform exists to unify. Everything
else is a proxy for readiness to buy.

---

## Deduplication safeguard

The main protection against repeated runs producing repeated data. Four
independent keys are computed for every candidate; a match on **any** of them
merges instead of inserting:

| Key | Derivation |
|---|---|
| `domain_key` | Registrable domain — `https://www.Acme.com/services` → `acme.com` |
| `name_key` | Legal suffixes, punctuation and plurals stripped — `Acme Technologies, LLC` → `acmetechnology` |
| `phone_key` | Last 10 digits of the phone number |
| fuzzy | rapidfuzz ratio ≥ 92 on `name_key`, scoped to the same state |

On top of that:

- **Aggregator blocklist** — Clutch, Yelp, LinkedIn, Indeed, Crunchbase and
  similar directories can never become company records.
- **Exclusion ledger** — every excluded company is written to a permanent table
  with its reason. Later runs skip it before spending any API credit
  (Module 28 exclusion discipline).
- **Query cooldown** — a query that succeeded within `query_cooldown_days` is
  not re-run, so repeat runs don't re-spend credits on the same search.
- **URL ledger** — fetched URLs are recorded and not re-fetched.
- **Merges never overwrite good data** — a merge fills blank fields only, and
  appends the new discovery source so the full source trail is kept.

Merging is visible in the run summary (`Merged into existing`) and in the
`Times Seen` and `Duplicate Status` columns of the export.

---

## Excel output

Written to `data/exports/companies_<label>_<timestamp>.xlsx`:

| Sheet | Contents |
|---|---|
| **Companies** | One row per approved/review account, in SOP Module 63 tracker field order, tier-colour-coded and filterable |
| **Decision_Makers** | Module 44 columns, pre-seeded with blank rows for each Tier A/B account — ready for phase 2 |
| **Evidence** | Source trail: evidence level, source type, URL, supported fact, date checked |
| **Signals** | Every detected signal with the page it was found on |
| **Excluded** | The exclusion ledger, so nobody re-researches these |
| **Run_Report** | Quality metrics, tier/segment/state distributions, issues found |
| **Learning_Log** | Observations captured during the run (Module 15) |

---

## Configuration

| File | Purpose |
|---|---|
| `config/settings.yaml` | Run switches, stage toggles, API budgets, thresholds, quality gates |
| `config/geography.yaml` | 51 states/DC with ranked cities |
| `config/queries.yaml` | The SOP query library and the aggregator blocklist |
| `.env` | API keys (gitignored) |

Notable knobs:

```yaml
discovery.max_new_companies_per_state   # cap per state; 0 = uncapped
discovery.max_new_companies_per_run     # cap across the full run; 0 = uncapped
discovery.tavily.max_calls_per_run      # hard credit budget for the run
discovery.tavily.cities_per_state       # how deep to go city-by-city
qualify.headcount_min                   # SOP Module 25: 1-10 excluded
scoring.tiers                           # A/B/C score thresholds
quality.min_completeness_pct            # run fails the gate below this
run.refresh_after_days                  # re-crawl companies older than this
```

### Tavily key rotation

`TAVILY_API_KEYS` accepts a comma-separated list. The pipeline rotates
automatically: when a key returns an auth or credit error (HTTP 401/402/403/432),
it is retired for the rest of the run and the next key takes over. Per-key
usage is printed in the run summary.

---

## Data sources and conduct

- **OpenStreetMap Overpass** — free and keyless; the pipeline sleeps between
  state queries and retries across mirrors rather than hammering one endpoint.
- **Tavily** — search API, used within a configurable per-run credit budget.
- **Company websites** — `robots.txt` is honoured, requests are rate-limited,
  and the crawler identifies itself in the User-Agent. Only public pages are
  fetched. Set a real contact address in `enrich.user_agent`.

LinkedIn is not scraped. LinkedIn company URLs are recorded only when a company
publishes them on its own site.

---

## Data-quality guarantees

Every defence below exists because the failure was observed in a real export.

**The organisation must be the right *kind* of organisation.** The single
biggest quality failure was validating *language* without asking *what kind of
organisation* published it. Splunk, Arctic Wolf and Mimecast write fluently
about 24/7 operations, SLAs and incident response — because they sell to MSPs.
ChannelPro ranks MSPs. IMC Construction says "our clients" and "we manage" —
about buildings. All of them reached a real export. `pipeline/classify.py` now
classifies each site as `msp_operator`, `software_vendor`, `directory_media`,
`association`, `end_user_org`, `consultancy` or `reseller`, and only operators
(plus end-user orgs in the end-user segments) become accounts.

Two details make it work:

- **Frequency, not presence.** Trade press quotes "our clients" once inside an
  article; a product company says "platform" fifty times. Counting occurrences
  (capped per marker, normalised for page length) separates what a site *is*
  from what it merely *mentions*.
- **IT vocabulary is required.** "Our clients", "we manage" and "we support"
  are industry-neutral. An operator must also be speaking about technology, so
  three distinct unambiguous IT terms are required before the operator reading
  is available at all — deliberately excluding words a construction firm uses
  in its own domain (`network`, `infrastructure`, `data center`).

**Names come from structured markup, not page titles.** A `<title>` is the
worst place to learn a company's name and was the first place we looked,
producing "IT Managed Services", "Transforming Government with Tech, Speed and
Scale" and "www.cognizant.com". `pipeline/nameres.py` tries schema.org
JSON-LD `Organization.name`, then `og:site_name`, then the footer copyright
line, then logo alt text, then the title, then the domain. A name whose tokens
are *all* generic industry or place words is a description, not a name — unless
it matches the domain, because `itsolutions-inc.com` really is called
"IT Solutions".

**US-only is enforced on evidence.** Country-code TLDs, country-specific URL
paths, non-US phone codes, non-USD currency and foreign legal forms (GmbH,
Pvt Ltd, "Registered in England") demote a record.

**Block pages never become companies.** A WAF returning HTTP 403 still serves an
HTML body. Left unchecked, its title becomes the company name and its empty body
becomes the description — producing dozens of records called "403" or "Just a
moment...". Non-2xx responses are rejected outright, bot-challenge interstitials
are detected even when they return 200, and a junk-name filter guards both the
ingest path and the site-name extractor.

**Search titles never outrank the company's own site.** Discovery gives names
like "Top MSP Wilmington | Managed IT Services by Sourcepass". Once the site is
crawled, the name from the site itself replaces it — Level 1 evidence beating
Level 3, per Module 29.

**The searched region is not the headquarters.** A national provider with a
Wilmington landing page is not a Delaware company. `Searched Region` records
where the query looked; `HQ State` is overwritten by the address found on the
company's own site, and `HQ Confirmed` tells you which of the two you're
looking at. Never filter on `HQ State` without checking `HQ Confirmed`.

**Unreachable is not excluded.** Several legitimate MSPs return 403 to any
non-browser client. Those go to `review`, never to the permanent exclusion
ledger — that ledger is reserved for business-model mismatches.

---

## LLM usage

Groq (`pipeline/llm.py`, `pipeline/llm_tasks.py`) is used **only** where it
genuinely beats deterministic code:

| Task | Why a model helps |
|---|---|
| Entity second opinion | The lexicon is accurate but brittle — every new phrasing needs a marker. A model generalises. |
| Fit rationale | SOP Module 63 prose, which is what models are actually good at. |
| False-positive challenge | SOP Module 61 specifies this prompt almost verbatim. |

**Deliberately not used** for dedup keys, the exclusion ledger, scoring
arithmetic, tier thresholds, or geography. Those must be reproducible across
runs — the SOP requires another researcher to reach the same conclusion from
the record, and a nondeterministic classifier breaks that.

Two SOP rules are enforced in code, not just in the prompt:

- **Module 29** — *"AI summaries are research assistance, not proof."* The model
  never overrides the deterministic verdict. Where the two conflict and the
  model is confident (≥0.7), the record drops to `review` with both opinions
  recorded, rather than either being silently trusted.
- **Module 59** — *"Never ask AI for a list of companies."* Every call is
  grounded in already-crawled page text and instructed to answer only from it.
  The model never generates companies, headcounts, customers or tools, and must
  quote the phrase that decided its answer.

Cost control mirrors Tavily: pooled key rotation (`GROQ_API_KEYS`, comma-separated),
a per-run call budget, and a **content-hash cache** so re-runs never re-spend on
unchanged pages. A free-tier key is worth roughly 300 calls/day, so the entity
check is gated to records where the lexicon was unsure or where a wrong call
would let a non-account through, and rationales only run at Tier B and above.

One measurement note worth knowing: an early version reported a 70% model-vs-rules
disagreement rate. That was a bug in the metric — a deterministic verdict of
`None`/`unknown` is an *abstention*, not an opinion, so the model filling that
gap was being counted as a conflict. Genuine conflicts are rare, and the run
report now separates `gaps filled` from `conflicts`.

---

## Where judgement is still required

The SOP is explicit that AI output is research assistance, not proof
(Module 29, Module 59). This pipeline is a discovery and triage system:

- Everything it approves is backed by a source URL you can open, but a human
  should confirm the fit reason before outreach.
- Headcount is best-effort from website copy; where unstated it is left blank
  and flagged, not guessed.
- `review` status means exactly that — the evidence was insufficient to decide,
  and the `Status Reason` column says what is missing.

---

## Tests

```bash
.venv/bin/python tests/test_dedup.py
```

Covers key normalisation, the aggregator guard, cross-run store dedup, the
exclusion ledger, query cooldown, checkpointing and signal detection.

---

## Project layout

```
run.py                     the run switch
run.sh                     venv wrapper
config/                    settings, geography, query library
pipeline/
  config.py                config loading, logging
  store.py                 SQLite persistence, dedup ledger, checkpoints
  dedup.py                 key derivation and matching
  signals.py               SOP signal lexicons and detection
  discovery/
    overpass.py            OpenStreetMap discovery
    tavily.py              Tavily search + key rotation
  enrich.py                website crawler and extraction
  intent.py                hiring and news timing signals
  qualify.py               exclusion rules, acceptance, fit rationale
  score.py                 Module 30 weighted scoring
  quality.py               end-of-run quality gates
  export.py                Excel workbook builder
data/companies.db          SQLite database (the dedup memory)
data/exports/              generated workbooks
logs/                      per-run logs
```
