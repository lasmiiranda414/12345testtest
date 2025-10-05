from __future__ import annotations
from pathlib import Path
import json
import pickle
from typing import List

from rank_bm25 import BM25Okapi
from rich.console import Console

console = Console()


def build_bm25(chunks_dir: Path, sparse_dir: Path):
    sparse_dir.mkdir(parents=True, exist_ok=True)
    docs: List[str] = []
    meta: List[dict] = []

    for f in sorted(chunks_dir.glob("*.chunks.json")):
        data = json.loads(f.read_text(encoding="utf-8"))
        src = data.get("meta", {}).get("source_file", str(f))
        for ch in data.get("chunks", []):
            if ch and ch.strip():
                docs.append(ch.strip())
                meta.append({"text": ch.strip(), "source_file": src})

    tokenized = [doc.lower().split() for doc in docs]
    bm25 = BM25Okapi(tokenized)

    with open(sparse_dir / "bm25.pkl", "wb") as fh:
        pickle.dump({"bm25": bm25, "docs": docs, "meta": meta}, fh)

    console.print(f"[bold green]BM25 gespeichert:[/bold green] {sparse_dir} (N={len(docs)})")
