#!/usr/bin/env python3
import asyncio, aiohttp, async_timeout, re, os, json, hashlib, time
from urllib.parse import urljoin, urlparse, urldefrag
from pathlib import Path
import urllib.robotparser as robotparser
from html.parser import HTMLParser
import argparse

ALLOWED_HOSTS = set()      # filled from args
START_URLS = []            # seeds
OUT_DIR = Path("data/raw")
SESSION_TIMEOUT = 20
CONCURRENCY = 8
MAX_PAGES = 10000
RATE_DELAY = 0.3           # politeness
USER_AGENT = "ragchat-pdfcrawler/0.1"

# --- HTML link extractor ------------------------------------------------------
class LinkExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []
    def handle_starttag(self, tag, attrs):
        if tag.lower() == "a":
            href = dict(attrs).get("href")
            if href:
                self.links.append(href)

def is_allowed_host(u: str) -> bool:
    try:
        h = urlparse(u).hostname or ""
    except Exception:
        return False
    if h == "scnat.ch": return True
    return any(h.endswith("." + ah) or h == ah for ah in ALLOWED_HOSTS)

def norm_url(base, href):
    # absolutize + remove fragments
    u = urljoin(base, href)
    u, _frag = urldefrag(u)
    return u

def looks_pdf(u: str) -> bool:
    return bool(re.search(r"\.pdf(\?|$)", u, re.I))

def url_to_id(u: str) -> str:
    return hashlib.sha1(u.encode("utf-8")).hexdigest()[:16]

async def fetch(session, url):
    try:
        with async_timeout.timeout(SESSION_TIMEOUT):
            async with session.get(url, headers={"User-Agent": USER_AGENT}) as resp:
                ct = resp.headers.get("Content-Type","").lower()
                b = await resp.read()
                return resp.status, ct, b
    except Exception:
        return None, "", b""

async def crawl(args):
    global ALLOWED_HOSTS, START_URLS
    ALLOWED_HOSTS = set(args.allow)
    START_URLS = args.seed

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    seen = set()
    to_visit = asyncio.Queue()
    for s in START_URLS:
        await to_visit.put(s)

    # robots per host
    robots_cache = {}
    def can_fetch(u: str) -> bool:
        try:
            pr = urlparse(u)
            host = pr.hostname or ""
            scheme = pr.scheme or "https"
        except Exception:
            return False
        if host not in robots_cache:
            rp = robotparser.RobotFileParser()
            rp.set_url(f"{scheme}://{host}/robots.txt")
            try:
                rp.read()
            except Exception:
                pass
            robots_cache[host] = rp
        return robots_cache[host].can_fetch(USER_AGENT, u)

    sem = asyncio.Semaphore(CONCURRENCY)
    pdf_saved = 0
    html_visited = 0
    discovered_pdf = set()

    async def worker():
        nonlocal pdf_saved, html_visited
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=SESSION_TIMEOUT)) as session:
            while True:
                try:
                    u = await asyncio.wait_for(to_visit.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    return
                if u in seen:
                    to_visit.task_done()
                    continue
                seen.add(u)

                if len(seen) > MAX_PAGES:
                    to_visit.task_done()
                    return

                if not is_allowed_host(u) or not can_fetch(u):
                    to_visit.task_done()
                    continue

                await asyncio.sleep(RATE_DELAY)
                async with sem:
                    status, ct, body = await fetch(session, u)

                if status != 200:
                    to_visit.task_done()
                    continue

                # If it's a PDF, save it
                if "application/pdf" in ct or looks_pdf(u):
                    sid = url_to_id(u)
                    pdf_path = OUT_DIR / f"{sid}.pdf"
                    meta_path = OUT_DIR / f"{sid}.meta.json"
                    try:
                        pdf_path.write_bytes(body)
                        meta = {"url": u, "content_type": "application/pdf", "saved_at": int(time.time())}
                        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
                        pdf_saved += 1
                    except Exception:
                        pass
                    to_visit.task_done()
                    continue

                # Otherwise if it's HTML, extract links
                if "text/html" in ct:
                    html_visited += 1
                    try:
                        text = body.decode("utf-8", errors="ignore")
                        parser = LinkExtractor(); parser.feed(text)
                        for href in parser.links:
                            nu = norm_url(u, href)
                            if not is_allowed_host(nu): 
                                continue
                            # enqueue HTML pages for discovery
                            if nu not in seen and (nu.startswith("http://") or nu.startswith("https://")):
                                # Always follow HTML; PDFs saved immediately when fetched
                                await to_visit.put(nu)
                            # Remember PDFs so we can print stats
                            if looks_pdf(nu):
                                discovered_pdf.add(nu)
                    except Exception:
                        pass

                to_visit.task_done()

    workers = [asyncio.create_task(worker()) for _ in range(CONCURRENCY)]
    await asyncio.gather(*workers, return_exceptions=True)

    # write discovered list for transparency
    (Path("data/audits")).mkdir(parents=True, exist_ok=True)
    (Path("data/audits/pdf_discovered_urls.txt")).write_text("\n".join(sorted(discovered_pdf)), encoding="utf-8")

    print("=== Focused PDF Crawl Summary ===")
    print(f"HTML pages visited:   {html_visited}")
    print(f"Unique URLs seen:     {len(seen)}")
    print(f"PDFs discovered:      {len(discovered_pdf)}")
    print(f"PDFs saved:           {pdf_saved}")
    print("Discovered list: data/audits/pdf_discovered_urls.txt")
    print("Saved PDFs:     data/raw/*.pdf")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", nargs="+", required=True, help="Seed URLs (space separated)")
    ap.add_argument("--allow", nargs="+", required=True, help="Allowed hostnames (scnat.ch, portal-cdn.scnat.ch, ...)")
    ap.add_argument("--max-pages", type=int, default=10000)
    args = ap.parse_args()
    MAX_PAGES = args.max_pages
    asyncio.run(crawl(args))
