from __future__ import annotations

import asyncio
import hashlib
import json
import re
import time
from collections import deque, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional, Set, List
from urllib.parse import urljoin, urlparse, urldefrag

import httpx
from bs4 import BeautifulSoup
from rich.console import Console
from urllib import robotparser

USER_AGENT = "ragchat-mirror/0.2 (+local)"
console = Console()

HTML_CT = {"text/html", "application/xhtml+xml"}
PDF_CT = {"application/pdf"}
SAVE_SUFFIXES = (".html", ".htm", ".pdf")  # Server CT can lie; also look at suffix


@dataclass
class CrawlConfig:
    allow_domains: list[str]
    obey_robots_txt: bool = True
    rate_limit_per_domain: float = 1.0  # req/sec
    max_pages: int = 1000
    raw_dir: Path = Path("data/raw")
    include_subdomains: bool = True
    use_sitemaps: bool = True


def _norm_url(base: str, href: str) -> Optional[str]:
    if not href:
        return None
    try:
        absu = urljoin(base, href)
        u, _frag = urldefrag(absu)
        parsed = urlparse(u)
        if not parsed.scheme.startswith("http"):
            return None
        # normalize netloc (strip default ports)
        netloc = parsed.hostname or ""
        if parsed.port:
            if (parsed.scheme == "http" and parsed.port == 80) or (parsed.scheme == "https" and parsed.port == 443):
                netloc = parsed.hostname or ""
            else:
                netloc = f"{parsed.hostname}:{parsed.port}"
        cleaned = parsed._replace(netloc=netloc).geturl()
        return cleaned
    except Exception:
        return None


def _domain_allowed(url: str, allow_domains: Iterable[str], include_subdomains: bool) -> bool:
    host = (urlparse(url).hostname or "").lower()
    if not host:
        return False
    for dom in allow_domains:
        d = dom.lower().lstrip(".")
        if host == d:
            return True
        if include_subdomains and (host.endswith("." + d)):
            return True
    return False


class RobotsCache:
    def __init__(self, client: httpx.Client):
        self.client = client
        self.cache: dict[str, robotparser.RobotFileParser] = {}
        self.sitemap_urls: dict[str, list[str]] = {}

    def allowed(self, url: str) -> bool:
        netloc = urlparse(url).netloc
        if netloc not in self.cache:
            rp = robotparser.RobotFileParser()
            robots_url = f"{urlparse(url).scheme}://{netloc}/robots.txt"
            try:
                resp = self.client.get(robots_url, headers={"User-Agent": USER_AGENT}, timeout=10)
                if resp.status_code >= 400:
                    rp.parse([])
                else:
                    lines = resp.text.splitlines()
                    rp.parse(lines)
                    # collect Sitemap: lines
                    sitemaps = []
                    for line in lines:
                        if line.lower().startswith("sitemap:"):
                            sitemaps.append(line.split(":", 1)[1].strip())
                    self.sitemap_urls[netloc] = sitemaps
            except Exception:
                rp.parse([])
                self.sitemap_urls[netloc] = []
            self.cache[netloc] = rp
        return self.cache[netloc].can_fetch(USER_AGENT, url)

    def sitemaps_for(self, url: str) -> list[str]:
        netloc = urlparse(url).netloc
        return self.sitemap_urls.get(netloc, [])


class RateLimiter:
    def __init__(self, per_domain_rps: float):
        self.min_interval = 1.0 / max(per_domain_rps, 0.001)
        self.last: dict[str, float] = defaultdict(lambda: 0.0)

    async def wait(self, url: str):
        host = urlparse(url).netloc
        now = time.monotonic()
        delta = now - self.last[host]
        if delta < self.min_interval:
            await asyncio.sleep(self.min_interval - delta)
        self.last[host] = time.monotonic()


def _hash_name(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]


def _guess_kind(url: str, content_type: str) -> str:
    u = url.lower()
    if u.endswith(".pdf") or "pdf" in (content_type or ""):
        return "pdf"
    return "html"


def _save_blob(raw_dir: Path, url: str, content: bytes, content_type: str):
    h = _hash_name(url)
    kind = _guess_kind(url, content_type)
    ext = ".pdf" if kind == "pdf" else ".html"
    out_dir = raw_dir / kind
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / f"{h}{ext}").write_bytes(content)
    meta = {
        "url": url,
        "content_type": content_type,
        "saved_path": f"{kind}/{h}{ext}",
        "time": time.time(),
    }
    (out_dir / f"{h}.meta.json").write_text(json.dumps(meta, ensure_ascii=False), encoding="utf-8")
    return f"{kind}/{h}{ext}"


async def _fetch(client: httpx.AsyncClient, url: str) -> Optional[httpx.Response]:
    try:
        return await client.get(url, timeout=20)
    except Exception as e:
        console.print(f"[red]Request error:[/red] {url} ({e})")
        return None


def _should_enqueue(url: str) -> bool:
    # We only mirror HTML pages (and we’ll save PDFs when encountered)
    return url.lower().endswith(SAVE_SUFFIXES) or True  # enqueue broadly; filter later


def _parse_links(base_url: str, html: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    hrefs = []
    for a in soup.find_all("a", href=True):
        n = _norm_url(base_url, a.get("href"))
        if n:
            hrefs.append(n)
    return hrefs


async def _discover_from_sitemaps(client: httpx.AsyncClient, start_urls: list[str]) -> Set[str]:
    discovered: Set[str] = set()
    robots = RobotsCache(client)
    for s in start_urls:
        # force robots load
        robots.allowed(s)
        for sm in robots.sitemaps_for(s):
            try:
                r = await _fetch(client, sm)
                if not r or r.status_code >= 400:
                    continue
                txt = r.text
                # minimal URL extraction (handles simple sitemap.xml and index)
                urls = re.findall(r"<loc>\s*(https?://[^<]+)\s*</loc>", txt, flags=re.IGNORECASE)
                discovered.update(urls)
                console.print(f"[dim]Sitemap discovered {len(urls)} urls from:[/dim] {sm}")
            except Exception:
                pass
    return discovered


async def crawl(config: CrawlConfig, seeds: list[str]):
    raw_dir = config.raw_dir
    raw_dir.mkdir(parents=True, exist_ok=True)
    (raw_dir / "html").mkdir(parents=True, exist_ok=True)
    (raw_dir / "pdf").mkdir(parents=True, exist_ok=True)

    manifest_path = raw_dir / "_manifest.jsonl"
    urls_txt = raw_dir / "urls.txt"
    # append-safe
    manifest_fh = manifest_path.open("a", encoding="utf-8")
    urls_fh = urls_txt.open("a", encoding="utf-8")

    seen: set[str] = set()
    q: deque[str] = deque()

    seeds_n = []
    for s in seeds:
        n = _norm_url(s, "") or s
        if n and _domain_allowed(n, config.allow_domains, config.include_subdomains):
            seeds_n.append(n)
        else:
            console.print(f"[yellow]Seed not allowed (whitelist):[/yellow] {s}")

    console.rule("[bold]Mirror start[/bold]")
    console.print(f"Allow domains: {config.allow_domains} (subdomains={'on' if config.include_subdomains else 'off'})")
    console.print(f"Max pages: {config.max_pages}, robots={'on' if config.obey_robots_txt else 'off'}")

    limiter = RateLimiter(config.rate_limit_per_domain)

    async with httpx.AsyncClient(follow_redirects=True, headers={"User-Agent": USER_AGENT}) as aclient:
        robots = RobotsCache(aclient)

        # sitemap discovery (optional)
        if config.use_sitemaps:
            discovered = await _discover_from_sitemaps(aclient, seeds_n)
            for u in discovered:
                if _domain_allowed(u, config.allow_domains, config.include_subdomains):
                    q.append(u)
                    seen.add(u)

        # enqueue seeds last so they’re visited early too
        for s in seeds_n:
            q.append(s)
            seen.add(s)

        saved = 0
        fetched = 0
        while q and fetched < config.max_pages:
            url = q.popleft()

            if config.obey_robots_txt and not robots.allowed(url):
                console.print(f"[dim]robots disallow:[/dim] {url}")
                continue

            await limiter.wait(url)
            r = await _fetch(aclient, url)
            fetched += 1
            if not r:
                continue

            ct = (r.headers.get("content-type") or "").split(";")[0].strip().lower()
            # decide save
            is_pdf = (ct in PDF_CT) or url.lower().endswith(".pdf")
            is_html = (ct in HTML_CT) or url.lower().endswith((".html", ".htm", "/"))
            if is_pdf or is_html:
                saved_path = _save_blob(raw_dir, url, r.content if is_pdf else r.text.encode("utf-8"), ct)
                manifest_fh.write(json.dumps({"url": url, "content_type": ct, "saved_path": saved_path}) + "\n")
                urls_fh.write(url + "\n")
                saved += 1
                console.print(("📄 PDF " if is_pdf else "🌐 HTML ") + f"saved: {url}")

            # enqueue links only from HTML
            if is_html:
                try:
                    links = _parse_links(url, r.text)
                    for nurl in links:
                        if nurl in seen:
                            continue
                        if not _domain_allowed(nurl, config.allow_domains, config.include_subdomains):
                            continue
                        if not _should_enqueue(nurl):
                            continue
                        seen.add(nurl)
                        q.append(nurl)
                except Exception as e:
                    console.print(f"[yellow]Parse warn:[/yellow] {url} ({e})")

        console.rule("[bold green]Done[/bold green]")
        console.print(f"Fetched: {fetched}, Saved: {saved}, out: {raw_dir.resolve()}")

    manifest_fh.close()
    urls_fh.close()
