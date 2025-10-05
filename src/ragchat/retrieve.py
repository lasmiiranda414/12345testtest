from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import json
import pickle
from typing import List

import faiss
import numpy as np
from rank_bm25 import BM25Okapi
from sentence_transformers import SentenceTransformer
from rich.console import Console

console = Console()
MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"


@dataclass
class Hit:
    text: str
    source_file: str
    score: float
    channel: str  # "dense" oder "bm25"


def _zscore(scores: np.ndarray) -> np.ndarray:
    if len(scores) == 0:
        return scores
    m = scores.mean()
    s = scores.std()
    return (scores - m) / (s + 1e-6)


class HybridIndex:
    def __init__(self, base: Path):
        self.base = base
        self.dense_dir = base / "data" / "indices" / "dense"
        self.sparse_dir = base / "data" / "indices" / "sparse"

        # Dense laden
        self.index = faiss.read_index(str(self.dense_dir / "faiss.index"))
        self.vectors = np.load(self.dense_dir / "vectors.npy")
        self.meta = [
            json.loads(l)
            for l in (self.dense_dir / "meta.jsonl").read_text(encoding="utf-8").splitlines()
            if l.strip()
        ]

        # Sparse laden
        obj = pickle.loads((self.sparse_dir / "bm25.pkl").read_bytes())
        self.bm25: BM25Okapi = obj["bm25"]
        self.docs: List[str] = obj["docs"]
        self.meta_sparse: List[dict] = obj["meta"]

        # Query-Encoder
        self.model = SentenceTransformer(MODEL_NAME, device="cpu")

    def search(
        self,
        query: str,
        k_dense: int = 20,
        k_bm25: int = 20,
        w_dense: float = 0.5,
        w_bm25: float = 0.5,
    ) -> List[Hit]:
        # Dense
        qv = self.model.encode([query], normalize_embeddings=True).astype(np.float32)
        D, I = self.index.search(qv, k_dense)  # IP ~ cosine (weil normalisiert)
        dense_hits = [
            Hit(
                text=self.meta[i]["text"],
                source_file=self.meta[i]["source_file"],
                score=float(D[0, j]),
                channel="dense",
            )
            for j, i in enumerate(I[0]) if i >= 0
        ]

        # BM25
        tokenized_q = query.lower().split()
        scores = self.bm25.get_scores(tokenized_q)
        top_idx = np.argsort(scores)[::-1][:k_bm25]
        bm25_hits = [
            Hit(
                text=self.meta_sparse[i]["text"],
                source_file=self.meta_sparse[i]["source_file"],
                score=float(scores[i]),
                channel="bm25",
            )
            for i in top_idx
        ]

        # Z-Score-Norm je Kanal + Fusion
        d_scores = np.array([h.score for h in dense_hits])
        b_scores = np.array([h.score for h in bm25_hits])
        d_norm = _zscore(d_scores) if len(d_scores) else np.array([])
        b_norm = _zscore(b_scores) if len(b_scores) else np.array([])

        for idx, h in enumerate(dense_hits):
            h.score = float(w_dense * (d_norm[idx] if len(d_norm) else 0.0))
        for idx, h in enumerate(bm25_hits):
            h.score = float(w_bm25 * (b_norm[idx] if len(b_norm) else 0.0))

        fused = dense_hits + bm25_hits
        fused.sort(key=lambda x: x.score, reverse=True)
        return fused
