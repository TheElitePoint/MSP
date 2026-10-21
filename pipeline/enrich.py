"""Website enrichment — the verification step the SOP insists on.

SOP Module 38 says: open the official website and review home, services,
industries, about, case studies, careers and contact pages before saving a
company. This module does exactly that, then records what it found as
Level 1 (direct) evidence with the source URL and date checked.

Politeness: robots.txt is honoured, requests are rate-limited per host, and
every fetched URL is written to the ledger so repeat runs skip it.
"""

from __future__ import annotations

import logging
import re
import socket
import threading
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

from . import signals
from .dedup import is_junk_name

# Some sites serve an XML sitemap or feed from a URL that looks like a page.
# We parse everything as HTML on purpose; the warning is noise, not a defect.
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

log = logging.getLogger("pipeline.enrich")

# Which internal links we care about, in priority order.
PAGE_PATTERNS: dict[str, list[str]] = {
    "services": ["service", "what-we-do", "solutions", "offering", "managed"],
    "about": ["about", "who-we-are", "company", "team", "leadership", "our-story"],
    "industries": ["industr", "vertical", "sector", "who-we-serve", "markets"],
    "careers": ["career", "job", "join-us", "work-with-us", "hiring", "openings"],
    "contact": ["contact", "get-in-touch", "locations", "offices"],
    "case_studies": ["case-stud", "success-stor", "customer-stor", "portfolio", "clients"],
}

SKIP_EXTENSIONS = (
    ".pdf", ".jpg", ".jpeg", ".png", ".gif", ".svg", ".zip", ".mp4", ".mp3",
    ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".webp", ".ico",
)

PHONE_RE = re.compile(r"(?:\+?1[\s.-]?)?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}")
EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
# Real US state/territory codes only. A bare [A-Z]{2} matched "CV 12345" on a
# foreign site and recorded Cape Verde as a US state.
US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","DC","FL","GA","HI","ID","IL","IN",
    "IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH",
    "NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT",
    "VT","VA","WA","WV","WI","WY","PR","VI","GU","AS","MP",
}
STATE_RE = re.compile(r"\b([A-Z]{2})\s+\d{5}(?:-\d{4})?\b")


@dataclass
class CrawlResult:
    reachable: bool = False
    pages_fetched: int = 0
    text: str = ""
    page_texts: dict[str, str] = field(default_factory=dict)
    page_urls: dict[str, str] = field(default_factory=dict)
    site_name: str | None = None
    phone: str | None = None
    emails: list[str] = field(default_factory=list)
    state_code: str | None = None
    linkedin_url: str | None = None
    home_soup: Any = None
    page_links: dict[str, str] = field(default_factory=dict)
    careers_soup: Any = None
    error: str | None = None


class RobotsCache:
    """Per-host robots.txt cache with a shared lock.

    Deliberately does NOT use RobotFileParser.read(). That method fetches with
    urllib's default User-Agent, which WAF-fronted sites reject with 403 — and
    the stdlib then interprets a 403 on robots.txt as "disallow everything",
    silently blocking crawls of sites that publish no restrictions at all.

    Instead we fetch robots.txt ourselves with the crawler's real User-Agent
    and only honour a genuine 200 response. Any other outcome (403, 404,
    timeout, connection error) means "no usable robots.txt", which per the
    standard means the path is allowed.
    """

    def __init__(self, user_agent: str, timeout: int = 10) -> None:
        self.user_agent = user_agent
        self.timeout = timeout
        self._cache: dict[str, RobotFileParser | None] = {}
        self._lock = threading.Lock()

    def _load(self, host: str) -> RobotFileParser | None:
        try:
            resp = requests.get(
                urljoin(host, "/robots.txt"),
                timeout=self.timeout,
                headers={"User-Agent": self.user_agent},
                allow_redirects=True,
            )
        except requests.RequestException:
            return None
        if resp.status_code != 200:
            return None
        if "html" in resp.headers.get("Content-Type", "").lower():
            # A soft-404 page rendered as HTML, not a real robots.txt.
            return None
        parser = RobotFileParser()
        try:
            parser.parse(resp.text.splitlines())
        except Exception:  # noqa: BLE001
            return None
        return parser

    def allowed(self, url: str) -> bool:
        try:
            parsed = urlparse(url)
            host = f"{parsed.scheme}://{parsed.netloc}"
        except ValueError:
            return False

        with self._lock:
            cached = host in self._cache
            parser = self._cache.get(host)
        if not cached:
            parser = self._load(host)
            with self._lock:
                self._cache[host] = parser

        if parser is None:
            return True
        try:
            return parser.can_fetch(self.user_agent, url)
        except Exception:  # noqa: BLE001
            return True


class WebsiteEnricher:
    def __init__(self, cfg: Any) -> None:
        node = cfg.get("enrich", {}) or {}
        self.enabled_pages: list[str] = node.get("enabled_pages", ["home", "services", "about"])
        self.max_pages = int(node.get("max_pages_per_site", 12))
        self.timeout = int(node.get("request_timeout", 20))
        self.sleep = float(node.get("sleep_between_requests", 1.0))
        self.max_workers = int(node.get("max_workers", 6))
        self.respect_robots = bool(node.get("respect_robots_txt", True))
        self.user_agent = node.get("user_agent", "ManageITServicesResearchBot/1.0")
        self.robots = RobotsCache(self.user_agent)
        self._local = threading.local()

    @property
    def session(self) -> requests.Session:
        """One session per worker thread."""
        sess = getattr(self._local, "session", None)
        if sess is None:
            sess = requests.Session()
            sess.headers.update({
                "User-Agent": self.user_agent,
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-US,en;q=0.9",
            })
            self._local.session = sess
        return sess

    # -- fetching ------------------------------------------------------------

    def _fetch(self, url: str) -> tuple[str | None, int | None, str | None]:
        """Returns (html, status_code, final_url)."""
        if self.respect_robots and not self.robots.allowed(url):
            log.debug("robots.txt disallows %s", url)
            return None, None, None
        try:
            resp = self.session.get(url, timeout=self.timeout, allow_redirects=True)
            # A 403/404/500 still returns an HTML body. Treating that body as
            # company content is how block pages end up as company names and
            # descriptions, so anything outside 2xx is not usable content.
            if not (200 <= resp.status_code < 300):
                return None, resp.status_code, resp.url
            ctype = resp.headers.get("Content-Type", "")
            if "html" not in ctype.lower():
                return None, resp.status_code, resp.url
            if len(resp.content) > 4_000_000:
                return None, resp.status_code, resp.url
            return resp.text, resp.status_code, resp.url
        except (requests.RequestException, socket.error, ValueError) as exc:
            log.debug("fetch failed %s: %s", url, exc)
            return None, None, None
        finally:
            time.sleep(self.sleep)

    def crawl(self, website: str) -> CrawlResult:
        """Fetch the homepage plus the highest-value internal pages.

        Search results are often deep landing pages (/locations/wilmington-de),
        which carry far less of the company's own language than the root site.
        The root domain is tried first, with the original URL as the fallback.
        """
        result = CrawlResult()
        if not website:
            result.error = "no website"
            return result

        html = status = final_url = None
        for candidate in _crawl_targets(website):
            html, status, final_url = self._fetch(candidate)
            if html:
                break
        if not html:
            result.error = f"homepage unreachable (status={status})"
            return result

        base = final_url or website
        soup = BeautifulSoup(html, "lxml")
        home_text = _visible_text(soup)

        # A bot-challenge interstitial returns HTTP 200 with real HTML, so it
        # looks like a successful fetch. Left undetected, its title ("Just a
        # moment...") gets stored as the company name and its empty body as
        # the description. Treat it as unreachable instead.
        if is_bot_challenge(html, soup, home_text):
            result.error = "blocked by bot-protection interstitial"
            return result

        result.reachable = True
        result.pages_fetched = 1
        result.home_soup = soup
        result.page_links["home"] = _outbound_links(soup)
        result.page_texts["home"] = home_text
        result.page_urls["home"] = base
        result.site_name = _site_name(soup)
        result.linkedin_url = _find_linkedin(soup)

        # Pick internal links matching the page types we want.
        targets = self._select_links(soup, base)
        for page_type, url in targets:
            if result.pages_fetched >= self.max_pages:
                break
            if page_type in result.page_texts:
                continue
            page_html, _, page_url = self._fetch(url)
            if not page_html:
                continue
            page_soup = BeautifulSoup(page_html, "lxml")
            result.page_texts[page_type] = _visible_text(page_soup)
            result.page_urls[page_type] = page_url or url
            result.page_links[page_type] = _outbound_links(page_soup)
            if page_type == "careers":
                # Kept for ATS link detection and JobPosting JSON-LD parsing.
                result.careers_soup = page_soup
            result.pages_fetched += 1
            if not result.linkedin_url:
                result.linkedin_url = _find_linkedin(page_soup)

        result.text = "\n\n".join(result.page_texts.values())

        # Contact details tend to live on home/contact pages.
        contact_blob = " ".join(
            result.page_texts.get(k, "") for k in ("contact", "home", "about")
        )
        phones = PHONE_RE.findall(contact_blob)
        result.phone = phones[0].strip() if phones else None
        result.emails = sorted({
            e.lower() for e in EMAIL_RE.findall(contact_blob)
            if not e.lower().endswith(SKIP_EXTENSIONS)
        })[:5]
        result.state_code = next(
            (m.group(1) for m in STATE_RE.finditer(contact_blob) if m.group(1) in US_STATES),
            None,
        )

        return result

    def _select_links(self, soup: BeautifulSoup, base: str) -> list[tuple[str, str]]:
        """Choose at most one URL per wanted page type."""
        base_host = urlparse(base).netloc.lower().replace("www.", "")
        chosen: dict[str, str] = {}

        for anchor in soup.find_all("a", href=True):
            href = anchor["href"].strip()
            if not href or href.startswith(("#", "mailto:", "tel:", "javascript:")):
                continue
            url = urljoin(base, href)
            parsed = urlparse(url)
            if parsed.scheme not in ("http", "https"):
                continue
            if parsed.netloc.lower().replace("www.", "") != base_host:
                continue
            if parsed.path.lower().endswith(SKIP_EXTENSIONS):
                continue

            haystack = (parsed.path + " " + anchor.get_text(" ", strip=True)).lower()
            for page_type, patterns in PAGE_PATTERNS.items():
                if page_type not in self.enabled_pages or page_type in chosen:
                    continue
                if any(p in haystack for p in patterns):
                    chosen[page_type] = url
                    break

        order = [p for p in self.enabled_pages if p != "home"]
        return [(pt, chosen[pt]) for pt in order if pt in chosen]

    # -- batch ---------------------------------------------------------------

    def crawl_many(self, items: list[tuple[int, str]]) -> dict[int, CrawlResult]:
        """Crawl (company_id, website) pairs concurrently."""
        results: dict[int, CrawlResult] = {}
        if not items:
            return results
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = {pool.submit(self.crawl, url): cid for cid, url in items}
            for future in as_completed(futures):
                cid = futures[future]
                try:
                    results[cid] = future.result()
                except Exception as exc:  # noqa: BLE001
                    log.warning("crawl failed for company %s: %s", cid, exc)
                    results[cid] = CrawlResult(error=str(exc))
        return results


# --------------------------------------------------------------------------
# Extraction helpers
# --------------------------------------------------------------------------


# Titles and body markers used by bot-protection interstitials and error pages.
CHALLENGE_TITLES = {
    "just a moment", "just a moment...", "attention required", "access denied",
    "access to this page has been denied", "checking your browser",
    "one moment, please", "please wait", "security check", "are you a robot",
    "verify you are human", "verifying you are human", "human verification",
    "403 forbidden", "404 not found", "site not found", "error", "bot verification",
    "ddos-guard", "captcha", "blocked", "under construction", "coming soon",
    "domain for sale", "this site can't be reached", "server error",
    # WAF / CDN block pages that can return HTTP 200
    "sucuri website firewall", "sucuri website firewall - access denied",
    "website firewall", "access to this site has been blocked",
    "request blocked", "forbidden", "not acceptable", "temporarily unavailable",
    "service unavailable", "site temporarily unavailable", "maintenance",
}

CHALLENGE_BODY_MARKERS = (
    "cf-browser-verification", "challenge-platform", "cf_chl_", "__cf_chl",
    "enable javascript and cookies to continue", "ray id",
    "ddos protection by", "checking if the site connection is secure",
    "please enable javascript to continue", "incapsula incident id",
    "request unsuccessful. incapsula", "perimeterx", "px-captcha",
)


def is_bot_challenge(html: str, soup: BeautifulSoup, text: str) -> bool:
    """True when a 200 response is actually a challenge, error or parked page."""
    title = ""
    if soup.title and soup.title.string:
        title = soup.title.string.strip().lower().rstrip(".").strip()

    if title in CHALLENGE_TITLES or title.rstrip("...") in CHALLENGE_TITLES:
        return True

    low_html = html[:20000].lower()
    if any(marker in low_html for marker in CHALLENGE_BODY_MARKERS):
        return True

    # A real company homepage has substantive copy. A challenge page does not.
    if len(text) < 220 and title in ("", "home", "index"):
        return True

    return False


def _crawl_targets(website: str) -> list[str]:
    """Root domain first, then the original URL if it had a path."""
    try:
        parsed = urlparse(website if "://" in website else "https://" + website)
    except ValueError:
        return [website]
    if not parsed.netloc:
        return [website]
    root = f"{parsed.scheme}://{parsed.netloc}"
    path = (parsed.path or "").strip("/")
    if not path:
        return [root]
    return [root, website]


def _outbound_links(soup: BeautifulSoup) -> str:
    """All link/iframe/script targets on a page.

    ATS job boards are linked by href, and href attributes do not survive
    text extraction — which is why ATS detection against page *text* found
    almost nothing.
    """
    parts: list[str] = []
    parts += [a.get("href", "") for a in soup.find_all("a", href=True)[:400]]
    parts += [f.get("src", "") for f in soup.find_all("iframe", src=True)[:30]]
    parts += [sc.get("src", "") for sc in soup.find_all("script", src=True)[:80]]
    parts += [f.get("action", "") for f in soup.find_all("form", action=True)[:20]]
    return " ".join(p for p in parts if p)[:20000]


def _visible_text(soup: BeautifulSoup) -> str:
    for tag in soup(["script", "style", "noscript", "svg", "iframe"]):
        tag.decompose()
    text = soup.get_text(" ", strip=True)
    return re.sub(r"\s+", " ", text)[:60_000]


def _site_name(soup: BeautifulSoup) -> str | None:
    """Prefer og:site_name, then application-name, then <title>.

    Every candidate must pass the junk-name filter, so a block page titled
    "403 - Forbidden" or "Sucuri WebSite Firewall" can never be adopted as
    the company name and overwrite a good one from discovery.
    """
    candidates: list[str] = []

    for attrs in ({"property": "og:site_name"}, {"name": "application-name"}):
        meta = soup.find("meta", attrs=attrs)
        if meta and meta.get("content"):
            candidates.append(meta["content"].strip())

    if soup.title and soup.title.string:
        title = soup.title.string.strip()
        for sep in ("|", " - ", " – ", " — "):
            if sep in title:
                candidates.append(title.split(sep)[0].strip())
                break
        else:
            candidates.append(title)

    for candidate in candidates:
        if candidate and not is_junk_name(candidate):
            return _trim_tagline(candidate)[:120]
    return None


def _trim_tagline(name: str) -> str:
    """Drop a trailing tagline: 'Sourcepass, A Nationwide MSP' -> 'Sourcepass'.

    Only trims when what follows the comma reads like marketing copy, so
    legitimate names such as 'Smith, Bell & Co' are left intact.
    """
    if "," not in name:
        return name
    head, _, tail = name.partition(",")
    head, tail = head.strip(), tail.strip().lower()
    if len(head) < 3 or not tail:
        return name
    tagline_starts = ("a ", "an ", "the ", "your ", "we ", "delivering ", "providing ")
    tagline_words = ("provider", "services", "solutions", "leading", "trusted",
                     "nationwide", "managed it", "msp", "specialist", "experts",
                     "consulting", "company serving")
    if tail.startswith(tagline_starts) or any(w in tail for w in tagline_words):
        return head
    return name


def _find_linkedin(soup: BeautifulSoup) -> str | None:
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        if "linkedin.com/company/" in href.lower():
            return href.split("?")[0][:300]
    return None


def summarise_services(text: str, max_items: int = 8) -> str:
    """A short, factual service list built only from phrases present on the site."""
    found = signals.detect_group(text, signals.RECURRING)
    labels = {
        "managed_services": "Managed IT services",
        "co_managed": "Co-managed IT",
        "outsourced_it": "Outsourced IT / vCIO",
        "managed_infrastructure": "Managed infrastructure & network",
        "managed_cloud": "Managed cloud operations",
        "managed_security": "Managed security / SOC",
        "recurring_commercial": "Recurring service agreements",
    }
    ops = signals.detect_group(text, signals.OPERATIONAL)
    op_labels = {
        "service_desk": "Service desk",
        "noc": "NOC operations",
        "continuity": "Backup / disaster recovery",
        "monitoring": "Proactive monitoring",
    }
    items = [labels[k] for k in found if k in labels]
    items += [op_labels[k] for k in ops if k in op_labels]
    seen: list[str] = []
    for item in items:
        if item not in seen:
            seen.append(item)
    return ", ".join(seen[:max_items])


def summarise_industries(text: str) -> str:
    hits = signals.detect_group(text, signals.VERTICAL)
    labels = {
        "healthcare": "Healthcare",
        "financial": "Financial services",
        "regulated_other": "Regulated / compliance-driven",
        "critical_infrastructure": "Manufacturing, logistics, legal, insurance",
    }
    return ", ".join(labels[k] for k in hits if k in labels)
