from __future__ import annotations
from pathlib import Path
import json

import faiss
import numpy as np
from rich.console import Console

from ragchat.embed import load_chunks, embed_chunks

console = Console()


def build_faiss(chunks_dir: Path, dense_dir: Path):
    dense_dir.mkdir(parents=True, exist_ok=True)

    vectors_path = dense_dir / "vectors.npy"
    meta_path = dense_dir / "meta.jsonl"
    index_path = dense_dir / "faiss.index"

    if vectors_path.exists() and meta_path.exists():
        embs = np.load(vectors_path)
        console.print(f"[dim]Vectors vorhanden:[/dim] {vectors_path}")
        meta = [json.loads(l) for l in meta_path.read_text(encoding="utf-8").splitlines() if l.strip()]
    else:
        chunks = load_chunks(chunks_dir)
        embs, meta = embed_chunks(chunks, dense_dir)

    d = embs.shape[1]
    index = faiss.IndexFlatIP(d)  # Cosine: weil wir bereits normalisiert haben (IP == cosine)
    index.add(embs)
    faiss.write_index(index, str(index_path))

    console.print(f"[bold green]FAISS-Index gespeichert:[/bold green] {index_path} (N={embs.shape[0]}, D={d})")
