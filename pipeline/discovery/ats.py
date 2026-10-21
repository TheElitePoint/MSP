"""ATS job-board pipeline — structured hiring intent.

Hiring is the strongest first-party intent signal available, but the careers
page crawl only gets a binary "is a NOC Manager mentioned somewhere". Most
companies host their careers page on an ATS that publishes a **public,
unauthenticated JSON endpoint** intended to be consumed:

    Greenhouse       boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true
    Lever            api.lever.co/v0/postings/{org}?mode=json
    Ashby            api.ashbyhq.com/posting-api/job-board/{org}
    Workable         apply.workable.com/api/v1/widget/accounts/{account}
    SmartRecruiters  api.smartrecruiters.com/v1/companies/{id}/postings
    Recruitee        {org}.recruitee.com/api/offers/

Detection is free: the careers page we already crawl links out to one of these
hosts, so the token is extracted from that link.

Deliberately NOT used: Indeed, Glassdoor, ZipRecruiter and LinkedIn. Their
terms prohibit scraping, they sit behind aggressive bot protection, and they
are already on the aggregator blocklist because job boards kept being saved as
companies.

What structured postings unlock over the text scan:
  * real posting dates      -> genuine recency instead of a default multiplier
  * open-role counts        -> hiring_volume as a measured number
  * full descriptions       -> job posts name the actual RMM/PSA/monitoring
                               stack far more explicitly than marketing copy
  * location fields         -> expansion signal and a second HQ source
  * seniority mix           -> a first Service Delivery Director is a sharper
                               signal than a backfill
"""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime, timezone
from typing import Any

import requests
from bs4 import BeautifulSoup

log = logging.getLogger("pipeline.ats")

# host fragment -> (provider, regex capturing the org token from the URL)
ATS_PATTERNS: list[tuple[str, str, re.Pattern[str]]] = [
    ("greenhouse", "boards.greenhouse.io",
     re.compile(r"boards\.greenhouse\.io/(?:embed/job_board\?for=)?([A-Za-z0-9_-]+)")),
    ("greenhouse", "job-boards.greenhouse.io",
     re.compile(r"job-boards\.greenhouse\.io/([A-Za-z0-9_-]+)")),
    ("lever", "jobs.lever.co", re.compile(r"jobs\.lever\.co/([A-Za-z0-9_-]+)")),
    ("ashby", "jobs.ashbyhq.com", re.compile(r"jobs\.ashbyhq\.com/([A-Za-z0-9_-]+)")),
    ("workable", "apply.workable.com",
     re.compile(r"apply\.workable\.com/(?:j/)?([A-Za-z0-9_-]+)")),
    ("smartrecruiters", "careers.smartrecruiters.com",
     re.compile(r"careers\.smartrecruiters\.com/([A-Za-z0-9_-]+)")),
    ("recruitee", ".recruitee.com", re.compile(r"([A-Za-z0-9_-]+)\.recruitee\.com")),
    ("bamboohr", ".bamboohr.com",
     re.compile(r"([A-Za-z0-9_-]+)\.bamboohr\.com/(?:careers|jobs)")),
    ("jazzhr", ".applytojob.com", re.compile(r"([A-Za-z0-9_-]+)\.applytojob\.com")),
    # SMB / mid-market HR-payroll platforms. Measured on real MSP careers
    # pages, these — not Greenhouse/Lever — are what this ICP actually runs.
    ("paylocity", "recruiting.paylocity.com",
     re.compile(r"recruiting\.paylocity\.com/recruiting/jobs/(?:List|All)/(\d+)")),
    ("adp", "workforcenow.adp.com",
     re.compile(r"workforcenow\.adp\.com/\S*?cid=([0-9a-f-]{8,})")),
    ("isolved", "saashr.com", re.compile(r"(?:secure\d*\.)?saashr\.com/ta/(\w+)")),
    ("applicantpro", ".applicantpro.com", re.compile(r"([A-Za-z0-9_-]+)\.applicantpro\.com")),
    ("careerplug", ".careerplug.com", re.compile(r"([A-Za-z0-9_-]+)\.careerplug\.com")),
    ("clearcompany", ".clearcompany.com", re.compile(r"([A-Za-z0-9_-]+)\.clearcompany\.com")),
    ("breezy", ".breezy.hr", re.compile(r"([A-Za-z0-9_-]+)\.breezy\.hr")),
    ("zohorecruit", ".zohorecruit.com", re.compile(r"([A-Za-z0-9_-]+)\.zohorecruit\.com")),
    ("paycom", "paycomonline.net", re.compile(r"paycomonline\.net/\S*?clientkey=(\w+)")),
    ("paycor", "paycor.com/careers", re.compile(r"paycor\.com/careers/(\w+)")),
    ("icims", ".icims.com", re.compile(r"([A-Za-z0-9_-]+)\.icims\.com")),
    ("jobvite", ".jobvite.com", re.compile(r"jobvite\.com/(?:careers/)?([A-Za-z0-9_-]+)")),
    ("ukg", ".ultipro.com", re.compile(r"([A-Za-z0-9_-]+)\.ultipro\.com")),
]

# Platforms with a public JSON API we can consume directly. The rest are
# detected (proving the company is hiring, and on which system) and their
# public job-board HTML is title-scraped instead.
JSON_API_PROVIDERS = {
    "greenhouse", "lever", "ashby", "workable", "smartrecruiters", "recruitee",
}


class ATSProvider:
    """Fetches structured job postings from public ATS endpoints."""

    source_name = "ats_jobs"

    def __init__(self, cfg: Any, session: requests.Session | None = None) -> None:
        node = cfg.get("discovery.ats", {}) or {}
        self.enabled = bool(node.get("enabled", True))
        self.timeout = int(node.get("timeout_seconds", 20))
        self.sleep = float(node.get("sleep_between_calls", 0.5))
        self.max_jobs = int(node.get("max_jobs_per_company", 60))
        self.session = session or requests.Session()
        self.user_agent = cfg.get(
            "enrich.user_agent", "ManageITServicesResearchBot/1.0"
        )
        self.calls_made = 0
        self.failures = 0

    # -- detection -----------------------------------------------------------

    @staticmethod
    def detect(html_or_urls: str) -> tuple[str, str] | None:
        """Find an ATS provider and org token in page HTML or a URL blob."""
        for provider, host, pattern in ATS_PATTERNS:
            if host not in html_or_urls:
                continue
            match = pattern.search(html_or_urls)
            if match and match.group(1):
                token = match.group(1)
                if token.lower() in ("embed", "www", "api", "jobs", "careers"):
                    continue
                return provider, token
        return None

    def detect_from_soup(self, soup: BeautifulSoup | None, page_html: str = "") -> tuple[str, str] | None:
        blob = page_html
        if soup is not None:
            hrefs = " ".join(
                a.get("href", "") for a in soup.find_all("a", href=True)[:400]
            )
            iframes = " ".join(
                f.get("src", "") for f in soup.find_all("iframe", src=True)[:20]
            )
            scripts = " ".join(
                s.get("src", "") for s in soup.find_all("script", src=True)[:60]
            )
            blob = f"{blob} {hrefs} {iframes} {scripts}"
        return self.detect(blob)

    # -- fetching ------------------------------------------------------------

    def _get(self, url: str) -> Any | None:
        try:
            resp = self.session.get(
                url,
                timeout=self.timeout,
                headers={"User-Agent": self.user_agent, "Accept": "application/json"},
            )
            self.calls_made += 1
            if resp.status_code != 200:
                return None
            return resp.json()
        except (requests.RequestException, json.JSONDecodeError, ValueError) as exc:
            log.debug("ATS fetch failed %s: %s", url, exc)
            self.failures += 1
            return None
        finally:
            time.sleep(self.sleep)

    def fetch_jobs(self, provider: str, token: str) -> list[dict[str, Any]]:
        """Return normalised postings: {title, description, location, posted_at, url}."""
        if provider == "greenhouse":
            data = self._get(
                f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true"
            )
            return _norm_greenhouse(data)
        if provider == "lever":
            data = self._get(f"https://api.lever.co/v0/postings/{token}?mode=json")
            return _norm_lever(data)
        if provider == "ashby":
            data = self._get(
                f"https://api.ashbyhq.com/posting-api/job-board/{token}?includeCompensation=false"
            )
            return _norm_ashby(data)
        if provider == "workable":
            data = self._get(
                f"https://apply.workable.com/api/v1/widget/accounts/{token}?details=true"
            )
            return _norm_workable(data)
        if provider == "smartrecruiters":
            data = self._get(
                f"https://api.smartrecruiters.com/v1/companies/{token}/postings?limit=100"
            )
            return _norm_smartrecruiters(data)
        if provider == "recruitee":
            data = self._get(f"https://{token}.recruitee.com/api/offers/")
            return _norm_recruitee(data)
        # No public JSON API — the hosted board is still a public page.
        return self._scrape_board(provider, token)

    BOARD_URLS = {
        "paylocity": "https://recruiting.paylocity.com/recruiting/jobs/List/{token}",
        "isolved": "https://secure7.saashr.com/ta/{token}.jobs",
        "applicantpro": "https://{token}.applicantpro.com/jobs/",
        "careerplug": "https://{token}.careerplug.com/jobs",
        "clearcompany": "https://{token}.clearcompany.com/careers/jobs",
        "breezy": "https://{token}.breezy.hr/",
        "zohorecruit": "https://{token}.zohorecruit.com/jobs/Careers",
        "bamboohr": "https://{token}.bamboohr.com/careers",
        "jazzhr": "https://{token}.applytojob.com/apply",
    }

    def _scrape_board(self, provider: str, token: str) -> list[dict[str, Any]]:
        """Title-scrape a public hosted job board that has no JSON API."""
        template = self.BOARD_URLS.get(provider)
        if not template:
            return []
        url = template.format(token=token)
        try:
            resp = self.session.get(
                url, timeout=self.timeout, headers={"User-Agent": self.user_agent}
            )
            self.calls_made += 1
            if resp.status_code != 200 or "html" not in resp.headers.get("Content-Type", "").lower():
                return []
            return extract_titles_from_html(BeautifulSoup(resp.text, "lxml"))
        except requests.RequestException as exc:
            log.debug("board scrape failed %s: %s", url, exc)
            self.failures += 1
            return []
        finally:
            time.sleep(self.sleep)

    def jobs_for_company(
        self, careers_html: str, careers_soup: BeautifulSoup | None = None
    ) -> tuple[str | None, list[dict[str, Any]]]:
        """Detect the ATS from a careers page and pull its postings."""
        if not self.enabled:
            return None, []
        found = self.detect_from_soup(careers_soup, careers_html)
        if not found:
            return None, []
        provider, token = found
        jobs = self.fetch_jobs(provider, token)[: self.max_jobs]
        if jobs:
            log.debug("ATS %s/%s -> %d postings", provider, token, len(jobs))
        return f"{provider}:{token}", jobs


# --------------------------------------------------------------------------
# Normalisers — each ATS has its own shape
# --------------------------------------------------------------------------


def _iso(value: Any) -> str | None:
    if not value:
        return None
    if isinstance(value, (int, float)):
        # Lever uses epoch milliseconds.
        try:
            seconds = value / 1000.0 if value > 1e11 else float(value)
            return datetime.fromtimestamp(seconds, tz=timezone.utc).isoformat(timespec="seconds")
        except (ValueError, OSError, OverflowError):
            return None
    text = str(value).strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S.%f%z"):
        try:
            stamp = datetime.strptime(text.replace("Z", "+0000"), fmt)
            if stamp.tzinfo is None:
                stamp = stamp.replace(tzinfo=timezone.utc)
            return stamp.isoformat(timespec="seconds")
        except ValueError:
            continue
    return None


def _strip_html(value: Any) -> str:
    if not value:
        return ""
    text = str(value)
    if "<" in text:
        text = BeautifulSoup(text, "lxml").get_text(" ", strip=True)
    return re.sub(r"\s+", " ", text)[:8000]


def _norm_greenhouse(data: Any) -> list[dict[str, Any]]:
    if not isinstance(data, dict):
        return []
    out = []
    for job in data.get("jobs", []) or []:
        out.append({
            "title": (job.get("title") or "").strip(),
            "description": _strip_html(job.get("content")),
            "location": ((job.get("location") or {}).get("name") or "").strip(),
            "posted_at": _iso(job.get("updated_at") or job.get("first_published")),
            "url": job.get("absolute_url"),
        })
    return out


def _norm_lever(data: Any) -> list[dict[str, Any]]:
    if not isinstance(data, list):
        return []
    out = []
    for job in data:
        cats = job.get("categories") or {}
        out.append({
            "title": (job.get("text") or "").strip(),
            "description": _strip_html(job.get("descriptionPlain") or job.get("description")),
            "location": (cats.get("location") or "").strip(),
            "posted_at": _iso(job.get("createdAt")),
            "url": job.get("hostedUrl"),
        })
    return out


def _norm_ashby(data: Any) -> list[dict[str, Any]]:
    if not isinstance(data, dict):
        return []
    out = []
    for job in data.get("jobs", []) or []:
        out.append({
            "title": (job.get("title") or "").strip(),
            "description": _strip_html(job.get("descriptionPlain") or job.get("descriptionHtml")),
            "location": (job.get("location") or "").strip(),
            "posted_at": _iso(job.get("publishedAt") or job.get("updatedAt")),
            "url": job.get("jobUrl"),
        })
    return out


def _norm_workable(data: Any) -> list[dict[str, Any]]:
    if not isinstance(data, dict):
        return []
    out = []
    for job in data.get("jobs", []) or []:
        out.append({
            "title": (job.get("title") or "").strip(),
            "description": _strip_html(job.get("description")),
            "location": " ".join(
                str(job.get(k) or "") for k in ("city", "state", "country")
            ).strip(),
            "posted_at": _iso(job.get("published_on") or job.get("created_at")),
            "url": job.get("url") or job.get("application_url"),
        })
    return out


def _norm_smartrecruiters(data: Any) -> list[dict[str, Any]]:
    if not isinstance(data, dict):
        return []
    out = []
    for job in data.get("content", []) or []:
        loc = job.get("location") or {}
        out.append({
            "title": (job.get("name") or "").strip(),
            "description": _strip_html((job.get("jobAd") or {}).get("sections")),
            "location": ", ".join(
                str(loc.get(k)) for k in ("city", "region", "country") if loc.get(k)
            ),
            "posted_at": _iso(job.get("releasedDate")),
            "url": job.get("applyUrl") or job.get("ref"),
        })
    return out


def _norm_recruitee(data: Any) -> list[dict[str, Any]]:
    if not isinstance(data, dict):
        return []
    out = []
    for job in data.get("offers", []) or []:
        out.append({
            "title": (job.get("title") or "").strip(),
            "description": _strip_html(job.get("description")),
            "location": (job.get("location") or "").strip(),
            "posted_at": _iso(job.get("published_at") or job.get("created_at")),
            "url": job.get("careers_url") or job.get("url"),
        })
    return out


# --------------------------------------------------------------------------
# Heuristic title extraction
# --------------------------------------------------------------------------

# Role nouns that make a short Title Case phrase a job title rather than
# navigation. Kept tight so "Managed IT Services" is not read as a vacancy.
_ROLE_NOUNS = (
    "manager", "engineer", "technician", "analyst", "specialist", "director",
    "administrator", "consultant", "architect", "coordinator", "lead",
    "supervisor", "representative", "developer", "designer", "accountant",
    "president", "officer", "intern", "apprentice", "associate", "advisor",
    "strategist", "installer", "dispatcher", "scheduler", "recruiter",
)

# Navigation/marketing phrases that end in a role noun but are not vacancies.
_NOT_A_TITLE = (
    "account manager login", "client portal", "our team", "meet the team",
    "leadership team", "managed service provider", "service provider",
    "solutions provider", "technology provider",
)

_TITLE_SHAPE = re.compile(r"^[A-Z][A-Za-z0-9&/,'’.\-\s()]{2,70}$")

# Word-boundary matching is essential. Substring matching made "Leaders" hit
# "lead", "Advisory" hit "advisor" and "Architecture" hit "architect", so
# marketing navigation was extracted as vacancies.
_ROLE_NOUN_RE = re.compile(
    r"\b(?:" + "|".join(_ROLE_NOUNS) + r")s?\b", re.IGNORECASE
)


def extract_titles_from_html(soup: BeautifulSoup | None) -> list[dict[str, Any]]:
    """Pull job titles straight off a careers page.

    Most mid-market MSPs list openings as links or headings on their own
    careers page with no ATS and no JobPosting markup. This yields titles and
    an open-role count — no dates, so the intent signal carries the default
    recency multiplier rather than a measured one.
    """
    if soup is None:
        return []

    seen: set[str] = set()
    out: list[dict[str, Any]] = []

    for tag in soup.find_all(["a", "h2", "h3", "h4", "li"])[:600]:
        text = re.sub(r"\s+", " ", tag.get_text(" ", strip=True) or "").strip()
        if not text or len(text) < 5 or len(text) > 70:
            continue
        low = text.lower()
        if any(bad in low for bad in _NOT_A_TITLE):
            continue
        if not _ROLE_NOUN_RE.search(text):
            continue
        if not _TITLE_SHAPE.match(text):
            continue
        words = text.split()
        if not (2 <= len(words) <= 8):
            continue
        key = low
        if key in seen:
            continue
        seen.add(key)
        href = tag.get("href") if tag.name == "a" else None
        out.append({
            "title": text,
            "description": "",
            "location": "",
            "posted_at": None,
            "url": href,
        })
    return out[:40]


# --------------------------------------------------------------------------
# schema.org JobPosting — the fallback when there is no hosted ATS
# --------------------------------------------------------------------------


def jobs_from_jsonld(soup: BeautifulSoup | None) -> list[dict[str, Any]]:
    """Google Jobs requires JobPosting JSON-LD, so most career pages embed it."""
    if soup is None:
        return []
    out: list[dict[str, Any]] = []

    def walk(node: Any) -> None:
        if isinstance(node, list):
            for item in node:
                walk(item)
            return
        if not isinstance(node, dict):
            return
        node_type = node.get("@type")
        types = (
            [node_type] if isinstance(node_type, str)
            else node_type if isinstance(node_type, list) else []
        )
        if any(str(t).lower() == "jobposting" for t in types):
            loc = node.get("jobLocation") or {}
            if isinstance(loc, list):
                loc = loc[0] if loc else {}
            address = (loc.get("address") or {}) if isinstance(loc, dict) else {}
            out.append({
                "title": _strip_html(node.get("title"))[:200],
                "description": _strip_html(node.get("description")),
                "location": " ".join(
                    str(address.get(k) or "")
                    for k in ("addressLocality", "addressRegion")
                ).strip() if isinstance(address, dict) else "",
                "posted_at": _iso(node.get("datePosted")),
                "url": node.get("url"),
            })
        for value in node.values():
            if isinstance(value, (dict, list)):
                walk(value)

    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = tag.string or tag.get_text() or ""
        if not raw.strip() or "jobposting" not in raw.lower():
            continue
        try:
            walk(json.loads(raw))
        except (json.JSONDecodeError, ValueError):
            continue
    return out
