#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fetch ALL PDFs from SCNAT (scnat.ch + subdomains + portal-cdn.scnat.ch)
in one pass with visible progress bars.

What it does:
  1) Crawls HTML pages (headless Chromium) up to --max-pages.
  2) On each page, captures BOTH:
     - <a href="...pdf"> links from the live DOM, and
     - ANY network request URL that contains '.pdf' (XHR/fetch too).
  3) Enqueues new HTML links (same domain family) to crawl (BFS).
  4) Downloads discovered PDFs concurrently with tqdm progress bars.
  5) Resumes: skips already-downloaded PDFs.

Outputs:
  - data/audits/pdf_urls_discovered.txt  (unique PDF URLs)
  - data/raw/<sha16>_<filename>.pdf      (binary)
  - data/raw/<sha16>.meta.json           (metadata)
  - Console summary at the end

NOTE: This script ONLY downloads PDFs. We’ll parse+chunk in the next step.
"""

import argparse
import hashlib
import json
import os
import queue
import re
import sys
import time
import urllib.robotparser as robotparser
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse, urljoin, urldefrag

import requests
from tqdm import tqdm
from playwright.sync_api import sync_playwright

# --------------------------- CONFIG DEFAULTS ---------------------------
ALLOWED_ROOTS = {"scnat.ch", "portal-cdn.scnat.ch"}  # allowed host family
USER_AGENT = "ragchat-pdf-crawler/1.0"
PAGE_TIMEOUT_MS = 15000
NETWORK_IDLE_WAIT_MS = 2000
DOWNLOAD_CHUNK = 1024 * 128

OUT_RAW = Path("data/raw")
OUT_AUDIT = Path("data/audits")
OUT_RAW.mkdir(parents=True, exist_ok=True)
OUT_AUDIT.mkdir(parents=True, exist_ok=True)

# --------------------------- HELPERS ----------------------------------
def is_allowed_host(u: str) -> bool:
    try:
        h = urlparse(u).hostname or ""
    except Exception:
        return False
    if h in ALLOWED_ROOTS:
        return True
    return any(h.endswith("." + root) for root in ALLOWED_ROOTS)

def norm_abs(base: str, href: str) -> str:
    u = urljoin(base, (href or "").strip())
    u, _ = urldefrag(u)
    return u

def looks_pdf(u: str) -> bool:
    # match .pdf at end of path or before querystring
    return bool(re.search(r"\.pdf($|\?)", u, re.IGNORECASE))

def sha16(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:16]

# robots.txt cache per host
_ROBOTS = {}
def can_fetch(u: str) -> bool:
    try:
        pr = urlparse(u)
        host = pr.hostname or ""
        scheme = pr.scheme or "https"
    except Exception:
        return False
    if host not in _ROBOTS:
        rp = robotparser.RobotFileParser()
        rp.set_url(f"{scheme}://{host}/robots.txt")
        try:
            rp.read()
        except Exception:
            pass
        _ROBOTS[host] = rp
    return _ROBOTS[host].can_fetch(USER_AGENT, u)

def target_paths_for(url: str) -> tuple[Path, Path]:
    sid = sha16(url)
    name = os.path.basename(urlparse(url).path) or f"{sid}.pdf"
    name = re.sub(r"[^A-Za-z0-9._-]", "_", name)
    if not name.lower().endswith(".pdf"):
        name += ".pdf"
    pdf_path = OUT_RAW / f"{sid}_{name}"
    meta_path = OUT_RAW / f"{sid}.meta.json"
    return pdf_path, meta_path

# --------------------------- DISCOVERY --------------------------------
def discover_pdf_urls(seeds: list[str], max_pages: int, rate_ms: int) -> list[str]:
    """BFS crawl with Playwright, capturing anchor hrefs AND ALL network requests that contain '.pdf'."""
    visited = set()
    q = queue.Queue()
    for s in seeds:
        if is_allowed_host(s):
            q.put(s)

    pdf_urls = set()
    pages_processed = 0

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            viewport={"width": 1366, "height": 900},
            user_agent=USER_AGENT,
        )

        # progress bar for pages
        bar = tqdm(total=max_pages, desc="Pages rendered", unit="page")
        while not q.empty() and pages_processed < max_pages:
            u = q.get()
            if u in visited:
                continue
            visited.add(u)

            if not is_allowed_host(u) or not u.startswith(("http://", "https://")):
                continue
            if not can_fetch(u):
                continue

            page = ctx.new_page()

            # capture ANY network request that looks like a PDF
            session_pdf = set()
            def on_request(req):
                ru = req.url
                if looks_pdf(ru) and is_allowed_host(ru) and can_fetch(ru):
                    session_pdf.add(ru)
            page.on("request", on_request)

            try:
                page.goto(u, timeout=PAGE_TIMEOUT_MS, wait_until="domcontentloaded")
                page.wait_for_timeout(NETWORK_IDLE_WAIT_MS)

                # also inspect the live DOM for anchors
                anchors = page.eval_on_selector_all("a", "els => els.map(e => e.getAttribute('href'))") or []
                for href in anchors:
                    if not href or href.startswith("javascript:"):
                        continue
                    absu = norm_abs(u, href)
                    if not is_allowed_host(absu):
                        continue
                    if looks_pdf(absu):
                        if can_fetch(absu):
                            session_pdf.add(absu)
                    else:
                        # enqueue more HTML pages (basic BFS)
                        if absu not in visited and absu.startswith(("http://", "https://")) and can_fetch(absu):
                            q.put(absu)

            except Exception:
                # ignore navigation/render errors; keep crawling
                pass
            finally:
                page.close()

            # record PDFs found on this page
            if session_pdf:
                pdf_urls.update(session_pdf)

            pages_processed += 1
            bar.update(1)
            if rate_ms > 0:
                time.sleep(rate_ms / 1000.0)

        bar.close()
        browser.close()

    # write discovered list
    out_list = OUT_AUDIT / "pdf_urls_discovered.txt"
    out_list.write_text("\n".join(sorted(pdf_urls)), encoding="utf-8")

    print("\n=== Discovery Summary ===")
    print(f"Pages attempted:   {min(pages_processed, max_pages)}")
    print(f"Unique PDFs found: {len(pdf_urls)}")
    print(f"List written:      {out_list}")
    return sorted(pdf_urls)

# --------------------------- DOWNLOAD ---------------------------------
def download_one(url: str) -> tuple[str, bool, int]:
    pdf_path, meta_path = target_paths_for(url)
    if pdf_path.exists() and meta_path.exists() and pdf_path.stat().st_size > 0:
        return (url, True, pdf_path.stat().st_size)
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, stream=True, timeout=30)
        if r.status_code != 200:
            return (url, False, 0)
        total = int(r.headers.get("Content-Length") or 0)
        size = 0
        with open(pdf_path, "wb") as f, tqdm(
            total=total if total > 0 else None,
            unit="B",
            unit_scale=True,
            desc=os.path.basename(pdf_path),
            leave=False,
        ) as bar:
            for chunk in r.iter_content(DOWNLOAD_CHUNK):
                if not chunk:
                    continue
                f.write(chunk)
                size += len(chunk)
                bar.update(len(chunk))
        meta = {
            "url": url,
            "content_type": "application/pdf",
            "saved_at": int(time.time()),
            "size": size,
            "source": "headless_crawl",
        }
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
        return (url, True, size)
    except Exception:
        return (url, False, 0)

def download_all(urls: list[str], workers: int) -> tuple[int, int]:
    ok = 0
    bytes_dl = 0
    urls = list(dict.fromkeys(urls))  # dedupe, keep order

    with ThreadPoolExecutor(max_workers=workers) as ex, tqdm(total=len(urls), desc="PDFs downloaded", unit="pdf") as pbar:
        futs = {ex.submit(download_one, u): u for u in urls}
        for fut in as_completed(futs):
            _, success, size = fut.result()
            if success:
                ok += 1
                bytes_dl += size
            pbar.update(1)

    return ok, bytes_dl

# --------------------------- MAIN -------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", nargs="+", default=["https://scnat.ch"], help="Seed URLs (space separated)")
    ap.add_argument("--max-pages", type=int, default=3000, help="Max HTML pages to render")
    ap.add_argument("--rate-ms", type=int, default=200, help="Delay between page renders (ms)")
    ap.add_argument("--workers", type=int, default=6, help="Concurrent PDF downloads")
    args = ap.parse_args()

    # 1) Discover all PDF URLs
    pdf_urls = discover_pdf_urls(args.seed, args.max_pages, args.rate_ms)

    if not pdf_urls:
        print("\nNo PDF URLs discovered. If you believe more exist, increase --max-pages or try again later.")
        return

    # 2) Download PDFs with progress bars (resumable)
    print("\nStarting downloads…")
    saved, bytes_dl = download_all(pdf_urls, args.workers)

    print("\n=== Download Summary ===")
    print(f"PDF URLs discovered: {len(pdf_urls)}")
    print(f"PDFs saved OK:       {saved}")
    print(f"Bytes saved:         {bytes_dl:,}")
    print("PDFs:   data/raw/*.pdf")
    print("Meta:   data/raw/*.meta.json")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted. Partial results kept.")
        sys.exit(130)
