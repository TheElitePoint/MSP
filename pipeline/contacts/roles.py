"""Decision-maker role classification and Module 44 filtering.

Deterministic on purpose. pipeline/llm.py's module docstring already draws this
line — the LLM is "deliberately NOT used for ... scoring arithmetic, tier
thresholds or geography [because] the SOP requires another researcher to reach
the same conclusion from the record". `role_type` decides whether a person is
accepted and feeds the decision-maker-access score, so it sits on the
deterministic side of that line.

The input is also a short job-title string rather than prose, and SOP Modules
41/42/43 are literally a title-to-role lookup table, so this is a transcription
of the manual rather than a heuristic worth a model call.

The LLM does get one narrow job (see llm_tasks.write_contact_relevance): the
Module 44 "reason selected" sentence, one call per company, which never changes
role_type or acceptance.
"""

from __future__ import annotations

import re
from typing import Any

# -- role classification (SOP Module 41) ------------------------------------
#
# ORDER MATTERS: first match wins, most specific first. Module 41 warns that
# "the CIO is not always the best first person. In managed technology
# providers, service-delivery or managed-services leaders may be closer to the
# pain", and Module 43 says "title is senior but responsibilities are unrelated
# -> do not select only for seniority". So a "Director of Managed Services"
# must land in `operational`, not get swept into a generic C-suite bucket by an
# earlier, broader pattern.
ROLE_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("risk", re.compile(
        r"operational resilience|technology risk|\bciso\b|chief information security"
        r"|chief risk|compliance|governance|\bgrc\b|information security officer",
        re.I)),
    # Champion sits ABOVE operational because Module 41 names these exact
    # titles as champions -- "Service Manager, Incident Manager, Change
    # Manager, NOC Manager, Infrastructure Manager" -- and several of them
    # would otherwise be swallowed by the broader operational patterns below
    # ("noc", "infrastructure"). Deliberately kept to that literal list: a
    # looser "operations manager" here would mis-file an IT Operations
    # Manager, who is the operational buyer rather than the champion.
    ("champion", re.compile(
        r"service manager|incident manager|problem manager|change manager"
        r"|service management|infrastructure manager|\bnoc manager\b"
        r"|network operations manager|escalation manager",
        re.I)),
    ("operational", re.compile(
        r"service delivery|managed services|managed it|it operations|service operations"
        r"|network operations|\bnoc\b|service desk|help ?desk|support operations"
        r"|customer escalation|technical operations|delivery manager",
        re.I)),
    ("technical", re.compile(
        # CIO is listed under BOTH economic and technical in Module 41. We
        # classify it as technical: at a managed technology provider the CIO is
        # the architecture/feasibility evaluator, which is the role that
        # actually engages with a first pilot. Documented choice, has a test.
        r"\bcto\b|chief technology|\bcio\b|chief information officer|chief digital"
        r"|vp of technology|vp technology|vice president of technology"
        r"|head of infrastructure|head of engineering|head of platform|head of cloud"
        r"|enterprise architect|chief architect|director of engineering"
        r"|director of technology|director of infrastructure|vp engineering",
        re.I)),
    ("economic", re.compile(
        r"founder|co-?founder|\bowner\b|\bceo\b|chief executive|president"
        r"|managing director|managing partner|\bcoo\b|chief operating"
        r"|\bprincipal\b|proprietor|chairman|chairwoman|\bcfo\b|chief financial",
        re.I)),
]

# -- Module 44 "people to avoid" --------------------------------------------
#
# Applied BEFORE role assignment. Each pattern below has a companion test for
# the near-miss it must NOT catch; those near-misses are the whole reason these
# are hand-written rather than a naive keyword list.
REJECT_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("recruiter", re.compile(
        r"recruit|talent acquisition|\bstaffing\b|headhunt|sourcer", re.I)),
    ("vendor_or_bd", re.compile(
        # \b on "sales" so "Presales Solutions Engineer" survives. The
        # partnerships alternative is written to not swallow "Managing Partner".
        r"\bsales\b|business development|account executive|\bbdr\b|\bsdr\b"
        r"|partnerships? (manager|director|lead)|channel manager|reseller",
        re.I)),
    ("junior", re.compile(
        # (?! director) keeps "Associate Director of IT Operations", a real
        # operational buyer, out of the junior bucket.
        r"\bintern\b|\bstudent\b|\bjunior\b|\btrainee\b|\bapprentice\b"
        r"|\bassociate\b(?!\s+(director|vice|vp|partner))"
        r"|\bassistant\b(?!\s+(director|vice|vp))|entry.level",
        re.I)),
    ("former_or_unclear", re.compile(
        r"\bformer\b|\bex-|\bretired\b|\bseeking\b|open to work|\bad ?interim\b", re.I)),
    ("non_operational", re.compile(
        r"\bmarketing\b|\bcontent\b|\bsocial media\b|\brealtor\b|real estate"
        r"|\bteacher\b|\bprofessor\b|\bnurse\b|\bparalegal\b|\brecruiting\b",
        re.I)),
]

# Soft reject. An in-house "Principal Consultant, Managed Services" at an MSP is
# an employee, not the outside adviser Module 44 warns about — so a consultant
# title that ALSO matches an operational/technical pattern goes to review for a
# human rather than being dropped.
CONSULTANT_PATTERN = re.compile(r"\bconsultant\b|\badvisor[y]?\b|\bcontractor\b", re.I)

# Module 42 priority weighting. First-priority titles for a band score highest;
# this is what rank_contacts uses to choose 2-4 people out of a larger set.
FIRST_PRIORITY = re.compile(
    r"founder|\bowner\b|\bceo\b|chief executive|president|\bcoo\b|chief operating"
    r"|\bcto\b|chief technology|\bcio\b|chief information officer", re.I)
SECOND_PRIORITY = re.compile(
    r"\bvp\b|vice president|\bhead of\b|\bdirector\b|managing director", re.I)

ROLE_ORDER = ("economic", "technical", "operational", "champion", "risk")


def classify_role(job_title: str | None) -> str | None:
    """Map a job title to a SOP Module 41 buying-group role.

    Returns None when nothing matches — an unmapped title is not evidence of a
    decision-maker, so the caller rejects rather than guessing.
    """
    if not job_title:
        return None
    title = str(job_title).strip()
    if not title:
        return None
    for role, pattern in ROLE_PATTERNS:
        if pattern.search(title):
            return role
    return None


def reject_reason(job_title: str | None, about: str | None = None) -> str | None:
    """SOP Module 44 "people to avoid". Returns a reason, or None to keep.

    `about` is checked only for the soft consultant case; it is free text and
    too noisy to hard-reject on.
    """
    if not job_title or not str(job_title).strip():
        return "no_title"
    title = str(job_title)

    for reason, pattern in REJECT_PATTERNS:
        if pattern.search(title):
            return reason

    if CONSULTANT_PATTERN.search(title):
        # Soft: only a reject if there is no operational/technical anchor.
        role = classify_role(title)
        if role not in ("operational", "technical", "champion", "risk"):
            return "consultant"
    return None


def rank_score(job_title: str | None, role_type: str | None) -> float:
    """Module 42 priority weight, used to pick the best N people."""
    if not job_title:
        return 0.0
    title = str(job_title)
    score = 0.0
    if FIRST_PRIORITY.search(title):
        score += 10.0
    elif SECOND_PRIORITY.search(title):
        score += 7.0
    elif role_type:
        score += 4.0
    # Module 41: at a managed provider the operational owner is often closer to
    # the pain than the most senior person, so it gets a small nudge over a
    # bare seniority read.
    if role_type == "operational":
        score += 1.5
    elif role_type in ("technical", "economic"):
        score += 1.0
    return score


def rank_contacts(
    accepted: list[dict[str, Any]], target: int, cfg: Any = None
) -> list[dict[str, Any]]:
    """Choose up to `target` people, balanced across the buying group.

    SOP Module 41 best practice: "identify one economic or senior sponsor, one
    technical or operational evaluator, and one potential champion". A pure
    top-N by score would return four C-suite titles and no one who actually
    owns the incident queue, so this fills one of each role first and only then
    backfills by score.
    """
    if target <= 0 or not accepted:
        return []

    pool = sorted(accepted, key=lambda c: float(c.get("rank_score") or 0), reverse=True)
    chosen: list[dict[str, Any]] = []
    used: set[int] = set()

    def take(predicate: Any) -> None:
        if len(chosen) >= target:
            return
        for idx, person in enumerate(pool):
            if idx in used:
                continue
            if predicate(person.get("role_type")):
                chosen.append(person)
                used.add(idx)
                return

    take(lambda r: r == "economic")
    take(lambda r: r in ("technical", "operational"))
    take(lambda r: r == "champion")

    for idx, person in enumerate(pool):
        if len(chosen) >= target:
            break
        if idx not in used:
            chosen.append(person)
            used.add(idx)

    return chosen[:target]
