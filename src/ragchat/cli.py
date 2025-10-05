import json
from pathlib import Path
from typing import List, Optional

import typer
from rich.console import Console
import yaml

from ragchat.crawl import CrawlConfig, crawl
from ragchat.parse import parse_all
from ragchat.chunk import chunk_all
from ragchat.index_dense import build_faiss
from ragchat.index_sparse import build_bm25

app = typer.Typer(help="ragchat CLI – local site mirroring & (later) RAG")
console = Console()

def project_root() -> Path:
    return Path(__file__).resolve().parents[2]

def load_config() -> dict:
    cfg_path = project_root() / "configs" / "config.yaml"
    with cfg_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)

@app.command()
def doctor():
    base = project_root()
    required = [
        base / "data" / "raw",
        base / "data" / "processed",
        base / "data" / "indices",
        base / "configs" / "config.yaml",
    ]
    ok = True
    for p in required:
        if not p.exists():
            ok = False
            console.print(f"[red]MISSING:[/red] {p}")
        else:
            console.print(f"[green]OK:[/green] {p}")
    if ok:
        console.print("[bold green]Environment looks good![/bold green]")
    else:
        raise SystemExit(1)

@app.command()
def hello(name: str = "world"):
    console.print(f"✨ Hello, {name}! ragchat is ready.")

@app.command()
def crawl_cmd(
    seed: List[str] = typer.Option(..., "--seed", help="Seed URL(s), repeat flag to add more"),
    allow: Optional[List[str]] = typer.Option(None, "--allow", help="Allowed domain(s) (root domain is enough)"),
    max_pages: int = typer.Option(None, "--max-pages", help="Max number of pages to fetch"),
    rate: float = typer.Option(None, "--rate", help="Requests per second per domain"),
    obey_robots: bool = typer.Option(True, "--obey-robots/--ignore-robots", help="Respect robots.txt"),
    include_subdomains: bool = typer.Option(True, "--include-subdomains/--no-subdomains", help="Follow subdomains"),
    use_sitemaps: bool = typer.Option(True, "--use-sitemaps/--no-sitemaps", help="Seed from robots Sitemap entries"),
):
    """Mirror HTML & PDFs across allowed domains."""
    base = project_root()
    cfg = load_config()

    allow_domains = allow if allow is not None else (cfg.get("crawl", {}).get("allow_domains") or [])
    if not allow_domains:
        console.print("[red]Please provide --allow domains or fill configs/config.yaml[/red]")
        raise SystemExit(2)

    c = CrawlConfig(
        allow_domains=allow_domains,
        obey_robots_txt=obey_robots if obey_robots is not None else bool(cfg["crawl"].get("obey_robots_txt", True)),
        rate_limit_per_domain=rate if rate is not None else float(cfg["crawl"].get("rate_limit_per_domain", 1.0)),
        max_pages=max_pages if max_pages is not None else int(cfg["crawl"].get("max_pages", 1000)),
        raw_dir=base / (cfg.get("paths", {}).get("raw_dir") or "data/raw"),
        include_subdomains=include_subdomains,
        use_sitemaps=use_sitemaps,
    )

    import asyncio as _asyncio
    _asyncio.run(crawl(c, list(seed)))

# The other commands can stay; we won't use them until later.
@app.command()
def parse_cmd():
    base = project_root()
    parse_all(base / "data" / "raw", base / "data" / "processed")

@app.command()
def chunk_cmd(size: int = 512, overlap: int = 64):
    base = project_root()
    chunk_all(base / "data" / "processed", base / "data" / "indices", size=size, overlap=overlap)





if __name__ == "__main__":
    app()
