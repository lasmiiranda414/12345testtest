#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse, csv, re, sys, time
from pathlib import Path
from urllib.parse import urljoin, urlparse, urldefrag

from tqdm import tqdm
from playwright.sync_api import sync_playwright

IN_CSV = Path("data/audits/short_html_lt50.csv")
OUT_DIR = Path("data/audits")
OUT_DIR.mkdir(parents=True, exist_ok=True)

ALLOWED = {"scnat.ch", "portal-cdn.scnat.ch"}  # SCNAT-only

def is_allowed(u: str) -> bool:
    try:
        h = urlparse(u).hostname or ""
    except Exception:
        return False
    return h in ALLOWED or any(h.endswith("." + d) for d in ALLOWED)

def norm_abs(base: str, href: str) -> str:
    u = urljoin(base, href.strip())
    u, _ = urldefrag(u)
    return u

def looks_pdf(u: str) -> bool:
    return bool(re.search(r"\.pdf(\?|$)", u, re.I))

def read_short_urls(limit=None):
    urls = []
    with open(IN_CSV, "r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            u = row.get("url") or ""
            if u:
                urls.append(u)
    # de-dup, keep order
    seen = set(); uniq = []
    for u in urls:
        if u not in seen:
            seen.add(u); uniq.append(u)
    return uniq[:limit] if limit else uniq

def discover(limit=None, timeout_ms=15000, network_idle_ms=2000, headless=True):
    urls = read_short_urls(limit=limit)
    found = set()
    pairs = []  # (page_url, pdf_url)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        ctx = browser.new_context(viewport={"width": 1366, "height": 900})
        page = ctx.new_page()

        for u in tqdm(urls, desc="Pages rendered", unit="page"):
            try:
                page.goto(u, timeout=timeout_ms, wait_until="domcontentloaded")
                # wait a bit for network to settle
                page.wait_for_timeout(network_idle_ms)
                # collect anchor hrefs (JS-rendered DOM)
                anchors = page.eval_on_selector_all("a", "els => els.map(e => e.getAttribute('href'))")
                for href in anchors or []:
                    if not href or href.startswith("javascript:"):
                        continue
                    au = norm_abs(u, href)
                    if looks_pdf(au) and is_allowed(au):
                        if au not in found:
                            found.add(au)
                            pairs.append((u, au))
            except Exception:
                continue

        browser.close()

    # write outputs
    (OUT_DIR / "pdf_from_headless.txt").write_text("\n".join(sorted(found)), encoding="utf-8")
    with open(OUT_DIR / "pdf_from_headless_pairs.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["page_url","pdf_url"]); w.writerows(pairs)

    print("\n=== Headless Discovery Summary ===")
    print(f"Pages attempted: {len(urls)}")
    print(f"Unique PDFs found: {len(found)}")
    print("Lists written to:")
    print("  data/audits/pdf_from_headless.txt")
    print("  data/audits/pdf_from_headless_pairs.csv")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=0, help="Limit pages (0 = all)")
    ap.add_argument("--timeout", type=int, default=15000, help="Per-page navigation timeout (ms)")
    ap.add_argument("--idle", type=int, default=2000, help="Extra wait after load (ms)")
    ap.add_argument("--headed", action="store_true", help="Show browser UI (for debugging)")
    args = ap.parse_args()
    discover(
        limit=(args.limit or None),
        timeout_ms=args.timeout,
        network_idle_ms=args.idle,
        headless=(not args.headed),
    )

