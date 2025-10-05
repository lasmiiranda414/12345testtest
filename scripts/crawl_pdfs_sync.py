#!/usr/bin/env python3
import time, re, os, json, hashlib, queue, argparse
from urllib.parse import urljoin, urlparse, urldefrag
from pathlib import Path
import urllib.robotparser as robotparser
import requests
from bs4 import BeautifulSoup

USER_AGENT = "ragchat-pdfcrawler-sync/0.1"
RATE_DELAY = 0.3
TIMEOUT = 20
MAX_PAGES_DEFAULT = 10000

def looks_pdf(u: str) -> bool:
    return bool(re.search(r"\.pdf(\?|$)", u, re.I))

def url_to_id(u: str) -> str:
    return hashlib.sha1(u.encode("utf-8")).hexdigest()[:16]

def is_allowed_host(u: str, allowed_hosts: set) -> bool:
    try:
        h = urlparse(u).hostname or ""
    except Exception:
        return False
    return h == "scnat.ch" or h in allowed_hosts or any(h.endswith(f".{ah}") for ah in allowed_hosts)

def can_fetch(u: str, robots_cache: dict) -> bool:
    try:
        pr = urlparse(u); host = pr.hostname or ""; scheme = pr.scheme or "https"
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

def fetch(u: str):
    try:
        r = requests.get(u, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT, allow_redirects=True)
        return r.status_code, r.headers.get("Content-Type","").lower(), r.content
    except Exception:
        return None, "", b""

def crawl(seeds, allow_hosts, out_dir: Path, max_pages: int):
    out_dir.mkdir(parents=True, exist_ok=True)
    q = queue.Queue()
    seen = set()
    robots_cache = {}
    discovered_pdfs = set()
    html_visited = 0
    pdf_saved = 0

    for s in seeds: q.put(s)

    while not q.empty() and len(seen) < max_pages:
        u = q.get()
        if u in seen: continue
        seen.add(u)

        if not is_allowed_host(u, allow_hosts): continue
        if not can_fetch(u, robots_cache): continue

        time.sleep(RATE_DELAY)
        status, ct, body = fetch(u)
        if status != 200: continue

        # If PDF, save
        if "application/pdf" in ct or looks_pdf(u):
            sid = url_to_id(u)
            pdf_path = out_dir / f"{sid}.pdf"
            meta_path = out_dir / f"{sid}.meta.json"
            try:
                pdf_path.write_bytes(body)
                meta = {"url": u, "content_type": "application/pdf", "saved_at": int(time.time())}
                meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
                pdf_saved += 1
            except Exception:
                pass
            continue

        # If HTML, parse links
        if "text/html" in ct or ct.startswith("text/"):
            html_visited += 1
            try:
                text = body.decode("utf-8", errors="ignore")
                soup = BeautifulSoup(text, "html.parser")
                for a in soup.find_all("a", href=True):
                    href = a["href"].strip()
                    if not href or href.startswith("javascript:") or href.startswith("mailto:"):
                        continue
                    nu = urljoin(u, href)
                    nu, _ = urldefrag(nu)
                    if not nu.startswith(("http://","https://")): 
                        continue
                    if not is_allowed_host(nu, allow_hosts):
                        continue
                    if looks_pdf(nu):
                        discovered_pdfs.add(nu)
                    if nu not in seen:
                        q.put(nu)
            except Exception:
                pass

    audits = Path("data/audits"); audits.mkdir(parents=True, exist_ok=True)
    (audits / "pdf_discovered_urls.txt").write_text("\n".join(sorted(discovered_pdfs)), encoding="utf-8")

    print("=== Focused PDF Crawl Summary (sync) ===")
    print(f"HTML pages visited:   {html_visited}")
    print(f"Unique URLs seen:     {len(seen)}")
    print(f"PDFs discovered:      {len(discovered_pdfs)}")
    print(f"PDFs saved:           {pdf_saved}")
    print("Discovered list: data/audits/pdf_discovered_urls.txt")
    print("Saved PDFs:     data/raw/*.pdf")

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", nargs="+", required=True)
    ap.add_argument("--allow", nargs="+", required=True)
    ap.add_argument("--max-pages", type=int, default=MAX_PAGES_DEFAULT)
    args = ap.parse_args()
    crawl(args.seed, set(args.allow), Path("data/raw"), args.max_pages)
