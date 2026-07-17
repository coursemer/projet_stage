"""
CatalogDescriber — descriptions automatiques du Data Catalog via Mistral Small.

Remplace les descriptions codées en dur : chaque entrée du catalogue
(pipeline, modèle dbt) reçoit une description générée par le LLM à partir
de ce qu'on sait réellement du pipeline (schéma, statistiques récentes,
tags), avec repli Ollama puis un texte générique si aucun backend LLM
n'est disponible — jamais bloquant.

Usage :
    from spark.catalog.llm_describer import CatalogDescriber

    describer = CatalogDescriber()
    text = describer.describe(
        name="clean_sales",
        entry_type="pipeline",
        tags=["spark", "sales"],
        schema_snapshot={"amount": "float64", "region": "string"},
        stats={"row_count": 48000, "duration_seconds": 28.0},
    )
"""
from __future__ import annotations

import hashlib
import json
import os
import urllib.request
from typing import Any, Dict, List, Optional

_SYSTEM_PROMPT = (
    "Tu es un data steward. On te donne le nom, le type, les tags et éventuellement "
    "le schéma/les statistiques d'un asset de données (pipeline ou modèle dbt). "
    "Rédige UNE phrase concise (25 mots max) en français décrivant ce que fait cet "
    "asset, à destination d'un catalogue de données. Ne mentionne aucun chiffre "
    "précis d'exécution (ça change à chaque run) — décris la fonction, pas l'état "
    "actuel. Réponds uniquement par la phrase, sans guillemets ni préambule."
)


class CatalogDescriber:
    """Génère les descriptions du Data Catalog via Mistral (Small) → Ollama → générique."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "mistral-small-latest",
        ollama_url: str = "http://localhost:11434",
        ollama_model: str = "mistral",
        cache_dir: Optional[str] = None,
    ) -> None:
        self.api_key      = api_key or os.environ.get("MISTRAL_API_KEY", "")
        self.model        = os.environ.get("METRICS_LLM_CATALOG_MODEL", model)
        self.ollama_url   = os.environ.get("METRICS_LLM_OLLAMA_URL", ollama_url)
        self.ollama_model = ollama_model
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        self.cache_dir = cache_dir or os.environ.get(
            "METRICS_LLM_CACHE_DIR", os.path.join(base_dir, "spark", "data", "llm_cache")
        )
        self._client = None

    @property
    def configured(self) -> bool:
        return bool(self.api_key)

    # ── API publique ──────────────────────────────────────────────────────────

    def describe(
        self,
        name: str,
        entry_type: str = "pipeline",
        tags: Optional[List[str]] = None,
        schema_snapshot: Optional[Dict[str, str]] = None,
        stats: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Retourne une description en une phrase, générée par le LLM (jamais bloquant)."""
        cache_key = _cache_key(name, entry_type, tags, schema_snapshot)
        cached = self._cache_get(cache_key)
        if cached:
            return cached

        prompt = self._build_prompt(name, entry_type, tags, schema_snapshot, stats)

        if self.api_key:
            try:
                text = self._mistral_call(prompt)
                self._cache_set(cache_key, text)
                return text
            except Exception:
                pass

        try:
            text = self._ollama_call(prompt)
            self._cache_set(cache_key, text)
            return text
        except Exception:
            pass

        return f"{entry_type.capitalize()} {name}."

    # ── Prompt ────────────────────────────────────────────────────────────────

    def _build_prompt(
        self,
        name: str,
        entry_type: str,
        tags: Optional[List[str]],
        schema_snapshot: Optional[Dict[str, str]],
        stats: Optional[Dict[str, Any]],
    ) -> str:
        lines = [f"Nom : {name}", f"Type : {entry_type}"]
        if tags:
            lines.append(f"Tags : {', '.join(tags)}")
        if schema_snapshot:
            cols = ", ".join(f"{c}:{t}" for c, t in list(schema_snapshot.items())[:15])
            lines.append(f"Colonnes : {cols}")
        if stats:
            lines.append(f"Ordre de grandeur observé : {stats}")
        return "\n".join(lines)

    # ── Mistral ───────────────────────────────────────────────────────────────

    def _get_client(self):
        if self._client is None:
            try:
                from mistralai import Mistral
            except ImportError:
                from mistralai.client import Mistral  # mistralai>=2.x
            self._client = Mistral(api_key=self.api_key)
        return self._client

    def _mistral_call(self, prompt: str) -> str:
        client = self._get_client()
        response = client.chat.complete(
            model=self.model,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            max_tokens=80,
        )
        return response.choices[0].message.content.strip()

    # ── Ollama fallback (stdlib only) ────────────────────────────────────────

    def _ollama_call(self, prompt: str) -> str:
        payload = json.dumps({
            "model": self.ollama_model,
            "messages": [
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            "stream": False,
        }).encode("utf-8")
        req = urllib.request.Request(
            f"{self.ollama_url}/api/chat", data=payload,
            headers={"Content-Type": "application/json"}, method="POST",
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        return data["message"]["content"].strip()

    # ── Cache fichier (évite de régénérer à chaque rerun Streamlit) ──────────

    def _cache_path(self, key: str) -> str:
        os.makedirs(self.cache_dir, exist_ok=True)
        return os.path.join(self.cache_dir, f"desc_{key}.txt")

    def _cache_get(self, key: str) -> Optional[str]:
        path = self._cache_path(key)
        if os.path.isfile(path):
            with open(path, encoding="utf-8") as f:
                return f.read().strip()
        return None

    def _cache_set(self, key: str, text: str) -> None:
        with open(self._cache_path(key), "w", encoding="utf-8") as f:
            f.write(text)


def _cache_key(
    name: str,
    entry_type: str,
    tags: Optional[List[str]],
    schema_snapshot: Optional[Dict[str, str]],
) -> str:
    raw = json.dumps(
        {"name": name, "type": entry_type, "tags": tags or [], "schema": schema_snapshot or {}},
        sort_keys=True,
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
