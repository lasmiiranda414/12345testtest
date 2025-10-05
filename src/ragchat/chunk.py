from __future__ import annotations
from pathlib import Path
import json
from typing import List
from rich.console import Console

console = Console()


def chunk_text(text: str, size: int = 512, overlap: int = 64) -> List[str]:
    """Simple word-window chunking. Adjust size/overlap freely."""
    text = " ".join((text or "").split())
    if not text:
        return []
    words = text.split()
    if not words:
        return []

    chunks: List[str] = []
    step = max(size - overlap, 1)
    for start in range(0, len(words), step):
        piece = " ".join(words[start : start + size]).strip()
        if piece:
            chunks.append(piece)
    return chunks


def chunk_all(processed_dir: Path, out_dir: Path, size: int = 512, overlap: int = 64):
    out_dir.mkdir(parents=True, exist_ok=True)
    n_files = 0
    total_chunks = 0

    for f in sorted(processed_dir.glob("*.json")):
        data = json.loads(f.read_text(encoding="utf-8"))
        text = data.get("text") or ""
        chunks = chunk_text(text, size=size, overlap=overlap)

        out_path = out_dir / (f.stem + ".chunks.json")
        out_data = {
            "chunks": chunks,
            "meta": {
                "source_file": data.get("source_file", str(f)),
                "source_type": data.get("source_type", "unknown"),
                "url": data.get("url"),
                "content_type": data.get("content_type"),
            },
        }
        out_path.write_text(json.dumps(out_data, ensure_ascii=False, indent=2), encoding="utf-8")

        console.print(f"✂️  {f.name} → {len(chunks)} chunks")
        n_files += 1
        total_chunks += len(chunks)

    console.print(f"[bold cyan]Chunking done:[/bold cyan] {n_files} files, {total_chunks} chunks → {out_dir}")
