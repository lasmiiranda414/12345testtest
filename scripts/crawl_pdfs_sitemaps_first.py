#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crawl PDFs with progress:
1) Discover from sitemaps (fast, robust)
2) (Optional) Scan a capped set of HTML pages for extra PDF links
3) Download PDFs with resume + tqdm progress bars
SCNAT-only (scnat.ch + portal-cdn.scnat.ch). Respects robots.txt.
"""

import argparse
import concurrent.futures as cf
import hashlib
import io
import os
import re
import sys
import time
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Iterable, List, Set
from urllib.parse import urlparse, urldefrag

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import urllib.robotparser as robotparser

USER_AGENT = "ragchat-sitemap-pdf/0.2"
OUT_DIR = Path("data/raw")
AUDIT_DIR = Path("data/audits")
TIMEOUT = 20
RETRIES = 3
CHUNK = 1024 * 128  # 128KB

ALLOWED = {"scnat.ch", "portal-cdn.scnat.ch"}

def is_allowed_host(u: str) -> bool:
    try:
        h = urlparse(u).hostname or ""
    except Exception:
        return False
    return h in ALLOWED or any(h.endswith("." + d) for d in ALLOWED)

def normalize_url(u: str) -> str:
    u, _frag = urldefrag(u.strip())
    return u

def looks_pdf(u: str) -> bool:
    return bool(re.search(r"\.pdf(\?|$)", u, re.I))

def sha16(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:16]

# ---------------- robots + sitemaps ----------------
def get_robots_sitemaps(host: str) -> List[str]:
    rp = robotparser.RobotFileParser()
    base = f"https://{host}"
    rp.set_url(f"{base}/robots.txt")
    try:
        rp.read()
    except Exception:
        pass
    maps = rp.site_maps() or []
    # Some sites forget to list sitemap in robots; try common locations
    common = [f"{base}/sitemap.xml", f"{base}/sitemap_index.xml"]
    for c in common:
        if c not in maps:
            maps.append(c)
    return maps

def fetch(url: str, stream=False):
    for _ in range(RETRIES):
        try:
            r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT, stream=stream, allow_redirects=True)
            if r.status_code == 200:
                return r
        except Exception:
            time.sleep(0.5)
    return None

def parse_sitemap(xml_bytes: bytes) -> List[str]:
    # return urls listed (handles both urlset and sitemapindex)
    urls = []
    try:
        soup = BeautifulSoup(xml_bytes, "lxml-xml")
    except Exception:
        return urls
    # urlset -> url -> loc
    for loc in soup.select("urlset url loc"):
        if loc.text:
            urls.append(loc.text.strip())
    # sitemapindex -> sitemap -> loc (nested)
    for loc in soup.select("sitemapindex sitemap loc"):
        if loc.text:
            urls.append(loc.text.strip())
    return urls

def gather_from_sitemaps(hosts: Iterable[str]) -> Set[str]:
    found = set()
    to_visit = []
    seen_maps = set()

    # seed maps
    for h in hosts:
        for m in get_robots_sitemaps(h):
            to_visit.append(m)

    with tqdm(total=0, desc="Sitemaps processed", unit="smap", leave=False) as pbar:
        while to_visit:
            sm = to_visit.pop()
            if sm in seen_maps:
                continue
            seen_maps.add(sm)
            r = fetch(sm, stream=False)
            if not r or not r.content:
                continue
            urls = parse_sitemap(r.content)
            # Separate nested sitemaps vs regular URLs
            nested = [u for u in urls if u.endswith(".xml")]
            pages  = [u for u in urls if not u.endswith(".xml")]
            to_visit.extend(nested)
            # keep only allowed + PDFs
            for u in pages:
                u = normalize_url(u)
                if not is_allowed_host(u):
                    continue
                if looks_pdf(u):
                    found.add(u)
            pbar.total = len(seen_maps)
            pbar.update(1)
    return found

# --------------- optional HTML scan for PDFs ---------------
class LinkExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []
    def handle_starttag(self, tag, attrs):
        if tag.lower() == "a":
            href = dict(attrs).get("href")
            if href:
                self.links.append(href)

def discover_pdfs_from_html(urls: List[str], max_pages: int = 2000) -> Set[str]:
    """Scan a capped set of HTML pages (from sitemaps) to find more PDFs."""
    found = set()
    urls = [u for u in urls if is_allowed_host(u)]
    urls = urls[:max_pages]

    for u in tqdm(urls, desc="HTML scanned", unit="page"):
        r = fetch(u, stream=False)
        if not r or "text/html" not in (r.headers.get("Content-Type", "")).lower():
            continue
        try:
            text = r.content.decode("utf-8", errors="ignore")
        except Exception:
            continue
        parser = LinkExtractor()
        try:
            parser.feed(text)
        except Exception:
            continue
        base = u
        for href in parser.links:
            if not href or href.startswith("javascript:"):
                continue
            absu = normalize_url(requests.compat.urljoin(base, href))
            if looks_pdf(absu) and is_allowed_host(absu):
                found.add(absu)
    return found

# --------------- downloader with tqdm + resume ---------------
@dataclass
class DownloadResult:
    url: str
    path: Path
    ok: bool
    size: int

def target_paths_for(url: str) -> (Path, Path):
    sid = sha16(url)
    # keep original filename suffix if present; default .pdf
    parsed = urlparse(url)
    name = os.path.basename(parsed.path) or f"{sid}.pdf"
    # sanitize
    name = re.sub(r"[^A-Za-z0-9._-]", "_", name)
    if not name.lower().endswith(".pdf"):
        name += ".pdf"
    pdf_path = OUT_DIR / f"{sid}_{name}"
    meta_path = OUT_DIR / f"{sid}.meta.json"
    return pdf_path, meta_path

def save_meta(meta_path: Path, url: str, size: int):
    meta = {
        "url": url,
        "content_type": "application/pdf",
        "saved_at": int(time.time()),
        "size": size,
        "source": "sitemaps_html"
    }
    meta_path.write_text(
        __import__("json").dumps(meta, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

def download_one(url: str) -> DownloadResult:
    try:
        pdf_path, meta_path = target_paths_for(url)
        if pdf_path.exists() and pdf_path.stat().st_size > 0 and meta_path.exists():
            return DownloadResult(url, pdf_path, True, pdf_path.stat().st_size)

        r = fetch(url, stream=True)
        if not r:
            return DownloadResult(url, pdf_path, False, 0)

        ctype = (r.headers.get("Content-Type", "")).lower()
        # trust extension + headers; some CDNs use octet-stream
        if ("pdf" not in ctype) and (not looks_pdf(url)):
            return DownloadResult(url, pdf_path, False, 0)

        total = int(r.headers.get("Content-Length") or 0)
        size = 0
        OUT_DIR.mkdir(parents=True, exist_ok=True)

        with open(pdf_path, "wb") as f, tqdm(
            total=total if total > 0 else None,
            unit="B",
            unit_scale=True,
            desc=os.path.basename(pdf_path),
            leave=False,
        ) as bar:
            for chunk in r.iter_content(chunk_size=CHUNK):
                if not chunk:
                    continue
                f.write(chunk)
                size += len(chunk)
                bar.update(len(chunk))

        save_meta(meta_path, url, size)
        return DownloadResult(url, pdf_path, True, size)
    except KeyboardInterrupt:
        raise
    except Exception:
        return DownloadResult(url, Path(""), False, 0)

def download_all(urls: List[str], max_workers: int = 6) -> List[DownloadResult]:
    results: List[DownloadResult] = []
    urls = list(dict.fromkeys(urls))  # dedupe, keep order
    with cf.ThreadPoolExecutor(max_workers=max_workers) as ex, tqdm(total=len(urls), desc="PDFs downloaded", unit="pdf") as pbar:
        futs = {ex.submit(download_one, u): u for u in urls}
        for fut in cf.as_completed(futs):
            res = fut.result()
            results.append(res)
            pbar.update(1)
    return results

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Only discover, don’t download")
    parser.add_argument("--scan-html", action="store_true", help="Also scan HTML pages from sitemaps for extra PDFs")
    parser.add_argument("--html-cap", type=int, default=1500, help="Max HTML pages to scan when --scan-html")
    parser.add_argument("--workers", type=int, default=6, help="Parallel downloads")
    args = parser.parse_args()

    AUDIT_DIR.mkdir(parents=True, exist_ok=True)

    # 1) Discover PDFs via sitemaps
    pdf_from_sitemaps = set()
    for host in sorted(ALLOWED):
        pdf_from_sitemaps |= gather_from_sitemaps([host])

    # 2) Optionally: discover more PDFs from HTML pages listed in sitemaps
    extra_pdfs = set()
    if args.scan_html:
        # We need the non-PDF URLs from sitemaps to scan
        html_urls = []
        for host in sorted(ALLOWED):
            for sm in get_robots_sitemaps(host):
                r = fetch(sm)
                if not r or not r.content:
                    continue
                urls = parse_sitemap(r.content)
                for u in urls:
                    u = normalize_url(u)
                    if is_allowed_host(u) and (not looks_pdf(u)):
                        html_urls.append(u)
        extra_pdfs = discover_pdfs_from_html(html_urls, max_pages=args.html_cap)

    # Save audits
    (AUDIT_DIR / "pdf_from_sitemaps.txt").write_text("\n".join(sorted(pdf_from_sitemaps)), encoding="utf-8")
    (AUDIT_DIR / "pdf_from_html_scan.txt").write_text("\n".join(sorted(extra_pdfs)), encoding="utf-8")

    all_pdf_urls = sorted(set(pdf_from_sitemaps) | set(extra_pdfs))

    print("\n=== Discovery Summary ===")
    print(f"PDF URLs from sitemaps: {len(pdf_from_sitemaps)}")
    print(f"Extra PDFs from HTML:   {len(extra_pdfs)}  (--scan-html cap={args.html_cap})")
    print(f"TOTAL unique PDFs:      {len(all_pdf_urls)}")
    print("Lists written to:")
    print("  data/audits/pdf_from_sitemaps.txt")
    print("  data/audits/pdf_from_html_scan.txt")

    if args.dry_run:
        print("\nDry run: not downloading. Re-run without --dry-run to fetch.")
        return

    # 3) Download with tqdm progress + resume
    if not all_pdf_urls:
        print("\nNo PDFs discovered. If you suspect JS-only links, next step will be a headless discovery pass.")
        return

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    print("\nStarting downloads…")
    results = download_all(all_pdf_urls, max_workers=args.workers)

    ok = sum(1 for r in results if r.ok)
    total_bytes = sum(r.size for r in results if r.ok)
    print("\n=== Download Summary ===")
    print(f"PDFs attempted: {len(results)}")
    print(f"PDFs saved OK:  {ok}")
    print(f"Bytes saved:    {total_bytes:,}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nInterrupted by user. Partial results kept.")
        sys.exit(130)
