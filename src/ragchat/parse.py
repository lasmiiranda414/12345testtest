from __future__ import annotations
from pathlib import Path
import json
from typing import Optional

from bs4 import BeautifulSoup
from rich.console import Console
from pdfminer.high_level import extract_text as pdf_extract_text

console = Console()


def _read_meta_for(path: Path) -> dict:
    """Read sibling .meta.json written by the crawler (contains original URL, content-type)."""
    meta_path = path.with_suffix(".meta.json")
    if meta_path.exists():
        try:
            return json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _clean_html_to_text(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")

    # Remove boilerplate & non-text
    for bad in soup(["script", "style", "noscript", "iframe", "svg"]):
        bad.decompose()
    for bad in soup.select(
        "header, footer, nav, aside, .cookie, .cookies, .banner, .navbar, .footer, .nav, .menu"
    ):
        bad.decompose()

    parts = []

    # Title first
    if soup.title and soup.title.string:
        parts.append(soup.title.string.strip())

    # Prefer <main>, fallback to <body>, then whole soup
    root = soup.find("main") or soup.body or soup
    for s in root.stripped_strings:
        parts.append(s)

    # Collapse whitespace
    return " ".join(" ".join(parts).split())


def parse_html_file(path: Path) -> dict:
    html = path.read_text(encoding="utf-8", errors="ignore")
    text = _clean_html_to_text(html)
    meta = _read_meta_for(path)
    return {
        "source_file": str(path),
        "source_type": "html",
        "url": meta.get("url"),
        "content_type": meta.get("content_type"),
        "text": text,
    }


def parse_pdf_file(path: Path) -> dict:
    # pdfminer returns one big string; we normalize whitespace
    text = pdf_extract_text(str(path)) or ""
    text = " ".join(text.split())
    meta = _read_meta_for(path)
    return {
        "source_file": str(path),
        "source_type": "pdf",
        "url": meta.get("url"),
        "content_type": meta.get("content_type"),
        "text": text,
    }


def parse_all(raw_dir: Path, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)

    html_dir = raw_dir / "html"
    pdf_dir = raw_dir / "pdf"

    n_ok = 0
    n_err = 0

    # HTML → JSON
    if html_dir.exists():
        for f in sorted(html_dir.glob("*.html")):
            try:
                data = parse_html_file(f)
                out_path = out_dir / (f.stem + ".json")
                out_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
                console.print(f"✅ HTML parsed: {f.name}")
                n_ok += 1
            except Exception as e:
                console.print(f"[red]HTML parse error {f.name}: {e}[/red]")
                n_err += 1

    # PDF → JSON
    if pdf_dir.exists():
        for f in sorted(pdf_dir.glob("*.pdf")):
            try:
                data = parse_pdf_file(f)
                out_path = out_dir / (f.stem + ".json")
                out_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
                console.print(f"📄 PDF parsed:  {f.name}")
                n_ok += 1
            except Exception as e:
                console.print(f"[red]PDF parse error {f.name}: {e}[/red]")
                n_err += 1

    console.print(f"[bold green]Parsing done.[/bold green] OK={n_ok}  ERRORS={n_err}  → {out_dir}")
