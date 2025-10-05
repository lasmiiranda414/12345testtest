from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import json
from typing import List, Dict

import numpy as np
from sentence_transformers import SentenceTransformer
from rich.console import Console

console = Console()

MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"  # 384-dim, CPU-freundlich


@dataclass
class ChunkItem:
    text: str
    source_file: str


def load_chunks(chunks_dir: Path) -> List[ChunkItem]:
    items: List[ChunkItem] = []
    for f in sorted(chunks_dir.glob("*.chunks.json")):
        data = json.loads(f.read_text(encoding="utf-8"))
        meta = data.get("meta", {})
        src = meta.get("source_file", str(f))
        for ch in data.get("chunks", []):
            if ch and ch.strip():
                items.append(ChunkItem(text=ch.strip(), source_file=src))
    return items


def embed_chunks(chunks: List[ChunkItem], out_dir: Path, batch_size: int = 64):
    out_dir.mkdir(parents=True, exist_ok=True)
    model = SentenceTransformer(MODEL_NAME, device="cpu")
    texts = [c.text for c in chunks]

    console.print(f"🔤 Embedding {len(texts)} Chunks mit [{MODEL_NAME}] …")
    embs = model.encode(texts, batch_size=batch_size, show_progress_bar=True, normalize_embeddings=True)
    embs = np.asarray(embs, dtype=np.float32)  # shape: (N, D)

    np.save(out_dir / "vectors.npy", embs)

    meta = [{"text": c.text, "source_file": c.source_file} for c in chunks]
    (out_dir / "meta.jsonl").write_text("\n".join(json.dumps(m, ensure_ascii=False) for m in meta), encoding="utf-8")

    console.print(f"[bold green]Embeddings gespeichert:[/bold green] {out_dir}")
    return embs, meta
