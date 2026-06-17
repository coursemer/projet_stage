"""
SemanticSearch — Semaine 12 : recherche sémantique dans le Data Catalog.

Deux backends, activés par ordre de préférence :
  1. LLM embeddings (Mistral API)  — MISTRAL_API_KEY requis
  2. TF-IDF sklearn                — toujours disponible, fallback automatique

Usage :
    from spark.catalog import DataCatalog, SemanticSearch

    catalog = DataCatalog()
    search  = SemanticSearch()
    search.index(catalog.list_entries())

    results = search.query("pipeline qui nettoie les ventes")
    for r in results:
        print(r.score, r.entry.name, r.entry.description)
        # 0.91  clean_sales  Nettoyage et validation des ventes brutes…
"""
from __future__ import annotations

import math
import os
import re
from typing import List, Optional

from .models import CatalogEntry, SearchResult


class SemanticSearch:
    """
    Moteur de recherche sémantique sur les entrées du Data Catalog.

    Construit un index TF-IDF en mémoire (sklearn) sur les textes des entrées.
    Si MISTRAL_API_KEY est disponible et que le modèle d'embeddings est accessible,
    utilise les embeddings Mistral pour une meilleure précision sémantique.
    """

    def __init__(self, use_embeddings: bool = True) -> None:
        self._entries: List[CatalogEntry] = []
        self._use_embeddings = use_embeddings and bool(os.environ.get("MISTRAL_API_KEY"))
        self._tfidf_matrix = None
        self._vectorizer    = None
        self._embeddings    = None   # numpy array (n_entries, dim)

    # ── Public API ────────────────────────────────────────────────────────────

    def index(self, entries: List[CatalogEntry]) -> None:
        """Indexe une liste d'entrées. Doit être appelé avant query()."""
        self._entries = entries
        if not entries:
            return

        if self._use_embeddings:
            try:
                self._build_embedding_index(entries)
                return
            except Exception:
                pass  # Fallback TF-IDF

        self._build_tfidf_index(entries)

    def query(self, text: str, top_k: int = 5) -> List[SearchResult]:
        """Recherche les `top_k` entrées les plus proches de la requête."""
        if not self._entries:
            return []

        if self._embeddings is not None:
            try:
                return self._query_embeddings(text, top_k)
            except Exception:
                pass

        return self._query_tfidf(text, top_k)

    @property
    def backend(self) -> str:
        return "embeddings" if self._embeddings is not None else "tfidf"

    @property
    def n_indexed(self) -> int:
        return len(self._entries)

    # ── TF-IDF ────────────────────────────────────────────────────────────────

    def _build_tfidf_index(self, entries: List[CatalogEntry]) -> None:
        from sklearn.feature_extraction.text import TfidfVectorizer
        import numpy as np

        texts = [e.to_text() for e in entries]
        self._vectorizer = TfidfVectorizer(
            analyzer="word",
            ngram_range=(1, 2),
            min_df=1,
            sublinear_tf=True,
        )
        self._tfidf_matrix = self._vectorizer.fit_transform(texts)

    def _query_tfidf(self, text: str, top_k: int) -> List[SearchResult]:
        import numpy as np

        query_vec = self._vectorizer.transform([text])
        # Cosine similarity = dot product (les vecteurs TF-IDF sont déjà L2-normalisés)
        scores = (self._tfidf_matrix * query_vec.T).toarray().flatten()
        top_idx = scores.argsort()[::-1][:top_k]
        return [
            SearchResult(
                entry=self._entries[i],
                score=round(float(scores[i]), 4),
                backend="tfidf",
            )
            for i in top_idx
            if scores[i] > 0
        ]

    # ── LLM Embeddings (Mistral) ──────────────────────────────────────────────

    def _build_embedding_index(self, entries: List[CatalogEntry]) -> None:
        import numpy as np
        texts = [e.to_text() for e in entries]
        vecs  = [self._embed(t) for t in texts]
        self._embeddings = np.array(vecs, dtype=float)

    def _query_embeddings(self, text: str, top_k: int) -> List[SearchResult]:
        import numpy as np
        q_vec  = np.array(self._embed(text), dtype=float)
        q_norm = np.linalg.norm(q_vec)
        if q_norm == 0:
            return []
        q_unit = q_vec / q_norm

        scores = []
        for i, doc_vec in enumerate(self._embeddings):
            d_norm = np.linalg.norm(doc_vec)
            if d_norm == 0:
                scores.append(0.0)
            else:
                scores.append(float(np.dot(q_unit, doc_vec / d_norm)))

        top_idx = sorted(range(len(scores)), key=lambda i: scores[i], reverse=True)[:top_k]
        return [
            SearchResult(
                entry=self._entries[i],
                score=round(scores[i], 4),
                backend="embeddings",
            )
            for i in top_idx
            if scores[i] > 0
        ]

    def _embed(self, text: str) -> List[float]:
        """Appelle l'API Mistral pour obtenir un vecteur d'embedding."""
        import json
        import urllib.request

        api_key = os.environ["MISTRAL_API_KEY"]
        payload = json.dumps({
            "model": "mistral-embed",
            "input": [text],
        }).encode()
        req = urllib.request.Request(
            "https://api.mistral.ai/v1/embeddings",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        return data["data"][0]["embedding"]
