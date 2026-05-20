"""
LLM Explainer — Semaine 10 : Mistral AI integration for anomaly explanation.

Primary:  Mistral AI — La Plateforme (mistral-large-latest)
          RGPD-compliant, hosted in France (OVHcloud), intra-UE.
Fallback: Ollama local (mistral or llama3.1) — zero data transfer.
Template: Rule-based fallback if both API and Ollama are unavailable.

Ref: docs/analyse_llm.docx — recommandation principale Mistral AI.

Usage:
    from spark.metrics.llm_explainer import LLMExplainer

    explainer = LLMExplainer()                      # uses MISTRAL_API_KEY env var
    anomalies = explainer.enrich_anomalies(anomalies)
    print(anomaly.context["llm_explanation"])

Environment variables:
    MISTRAL_API_KEY        — Mistral API key (required for cloud calls)
    METRICS_LLM_MODEL      — override model (default: mistral-large-latest)
    METRICS_LLM_OLLAMA_URL — override Ollama URL (default: http://localhost:11434)
    METRICS_LLM_CACHE_DIR  — directory for file-based response cache
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

try:
    from .anomaly_detection.models import Anomaly, AnomalyLevel, Severity
except ImportError:
    from spark.metrics.anomaly_detection.models import Anomaly, AnomalyLevel, Severity


# ── System prompt (English per analyse_llm.docx recommendations) ─────────────

_SYSTEM_PROMPT = (
    "You are a Data Reliability Engineer for a data platform built on Airflow, Spark and dbt. "
    "Your role is to analyze anomalies detected in ETL pipelines and produce clear, actionable explanations. "
    "Pipelines: ingest_sales, clean_sales, aggregate_sales, customer_360, inventory_scd2. "
    "Rules: (1) Be concise — 3 sentences max. "
    "(2) Name the probable root cause, not just the symptom. "
    "(3) Propose one immediate corrective action. "
    "(4) If the anomaly is likely benign, say so. "
    "Reply in French."
)

_USER_TEMPLATE = (
    "Pipeline: {pipeline}\n"
    "Detection layer: {level}\n"
    "Severity: {severity}\n"
    "Metric: {metric}\n"
    "Observed: {observed}\n"
    "Expected: {expected}\n"
    "Description: {description}\n"
    "Context: {ctx}\n"
    "Explain and propose a fix."
)

# ── Template fallback ─────────────────────────────────────────────────────────

_TEMPLATES: dict[str, str] = {
    AnomalyLevel.VOLUME: (
        "Volume anormal détecté ({metric_name}, pipeline {pipeline_name}) : "
        "valeur observée {observed:.4g} vs attendue {expected}. "
        "Vérifiez la source de données et les logs d'ingestion pour détecter une perte ou un doublon."
    ),
    AnomalyLevel.DISTRIBUTION: (
        "Distribution statistique inhabituelle sur {metric_name} ({pipeline_name}). "
        "Inspectez les données brutes pour un changement de format ou de population source."
    ),
    AnomalyLevel.SCHEMA: (
        "Changement de schéma inattendu dans {pipeline_name} sur {metric_name}. "
        "Comparez le schéma actuel avec la dernière version validée."
    ),
    AnomalyLevel.PERFORMANCE: (
        "Dégradation de performance : {metric_name} = {observed:.4g}s (seuil : {expected}). "
        "Vérifiez les ressources Spark et identifiez les goulots d'étranglement dans {pipeline_name}."
    ),
    AnomalyLevel.TEMPORAL: (
        "Anomalie temporelle sur {pipeline_name} : {description} "
        "Comparez avec les mêmes périodes des semaines précédentes."
    ),
    AnomalyLevel.ML: (
        "Isolation Forest a signalé ce snapshot comme anormal (score={observed:.4f}). "
        "Plusieurs métriques combinées s'écartent du profil habituel de {pipeline_name}. "
        "Analysez les métriques individuelles pour isoler la cause."
    ),
}


# ── Main class ────────────────────────────────────────────────────────────────

class LLMExplainer:
    """
    Generates natural-language explanations for ML-detected pipeline anomalies.

    Priority: Mistral API → Ollama local → template fallback.
    """

    def __init__(
        self,
        api_key:     Optional[str] = None,
        model:       str = "mistral-large-latest",
        ollama_url:  str = "http://localhost:11434",
        ollama_model: str = "mistral",
        max_tokens:  int = 256,
        cache_dir:   Optional[str] = None,
    ) -> None:
        self.api_key      = api_key or os.environ.get("MISTRAL_API_KEY", "")
        self.model        = os.environ.get("METRICS_LLM_MODEL", model)
        self.ollama_url   = os.environ.get("METRICS_LLM_OLLAMA_URL", ollama_url)
        self.ollama_model = ollama_model
        self.max_tokens   = max_tokens
        self.cache_dir    = cache_dir or os.environ.get(
            "METRICS_LLM_CACHE_DIR",
            os.path.join(BASE_DIR, "spark", "data", "llm_cache"),
        )
        self._client = None

    # ── Public API ────────────────────────────────────────────────────────────

    def enrich_anomalies(self, anomalies: List[Anomaly]) -> List[Anomaly]:
        """Populate context['llm_explanation'] for each anomaly in-place."""
        for anomaly in anomalies:
            explanation, backend = self._explain(anomaly)
            anomaly.context["llm_explanation"] = explanation
            anomaly.context["llm_backend"]     = backend
            anomaly.context["llm_ts"]          = datetime.now(timezone.utc).isoformat()
        return anomalies

    def explain_alert_dict(self, alert: dict) -> tuple[str, str]:
        """Explain an alert dict (from AlertManager.query). Returns (text, backend)."""
        level_str = str(alert.get("algorithm", "")).replace("ml:", "")
        try:
            level = AnomalyLevel(level_str)
        except ValueError:
            level = AnomalyLevel.VOLUME

        anomaly = Anomaly(
            id=str(alert.get("id", "?")),
            pipeline_name=str(alert.get("source", "?")),
            level=level,
            severity=Severity.MEDIUM,
            description=str(alert.get("details", "")),
            metric_name=str(alert.get("metric_name", "?")),
            observed_value=float(alert.get("value") or 0.0),
        )
        return self._explain(anomaly)

    # ── Internal dispatch ─────────────────────────────────────────────────────

    def _explain(self, anomaly: Anomaly) -> tuple[str, str]:
        prompt = self._build_prompt(anomaly)
        cache_key = _cache_key(prompt)

        cached = self._cache_get(cache_key)
        if cached:
            return cached, "cache"

        if self.api_key:
            try:
                text = self._mistral_call(prompt)
                self._cache_set(cache_key, text)
                return text, f"mistral:{self.model}"
            except Exception:
                pass

        try:
            text = self._ollama_call(prompt)
            self._cache_set(cache_key, text)
            return text, f"ollama:{self.ollama_model}"
        except Exception:
            pass

        text = self._template(anomaly)
        return text, "template"

    # ── Mistral API ───────────────────────────────────────────────────────────

    def _mistral_call(self, prompt: str) -> str:
        client = self._get_client()
        response = client.chat.complete(
            model=self.model,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user",   "content": prompt},
            ],
            max_tokens=self.max_tokens,
        )
        return response.choices[0].message.content.strip()

    def _get_client(self):
        if self._client is None:
            from mistralai import Mistral
            self._client = Mistral(api_key=self.api_key)
        return self._client

    # ── Ollama fallback (stdlib only, no extra deps) ──────────────────────────

    def _ollama_call(self, prompt: str) -> str:
        payload = json.dumps({
            "model":    self.ollama_model,
            "messages": [
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user",   "content": prompt},
            ],
            "stream": False,
        }).encode()

        req = urllib.request.Request(
            f"{self.ollama_url}/api/chat",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=20) as resp:
            data = json.loads(resp.read())
        return data["message"]["content"].strip()

    # ── Template fallback ─────────────────────────────────────────────────────

    def _template(self, anomaly: Anomaly) -> str:
        tpl = _TEMPLATES.get(anomaly.level, _TEMPLATES[AnomalyLevel.ML])
        obs = anomaly.observed_value if anomaly.observed_value is not None else 0.0
        exp = str(anomaly.expected_value) if anomaly.expected_value is not None else "historique"
        try:
            return tpl.format(
                pipeline_name=anomaly.pipeline_name,
                metric_name=anomaly.metric_name,
                observed=obs,
                expected=exp,
                description=anomaly.description,
            )
        except (KeyError, ValueError):
            return (
                f"Anomalie {anomaly.level} sur {anomaly.pipeline_name}/{anomaly.metric_name}. "
                "Consultez les logs du pipeline pour plus de détails."
            )

    # ── Prompt builder ────────────────────────────────────────────────────────

    def _build_prompt(self, anomaly: Anomaly) -> str:
        obs = f"{anomaly.observed_value:.4g}" if anomaly.observed_value is not None else "N/A"
        exp = str(anomaly.expected_value) if anomaly.expected_value is not None else "normal historical range"
        ctx = _format_context(anomaly.context)
        return _USER_TEMPLATE.format(
            pipeline=anomaly.pipeline_name,
            level=anomaly.level,
            severity=anomaly.severity,
            metric=anomaly.metric_name,
            observed=obs,
            expected=exp,
            description=anomaly.description[:300],
            ctx=ctx,
        )

    # ── File cache (per analyse_llm.docx cost-optimisation strategy) ─────────

    def _cache_path(self, key: str) -> str:
        return os.path.join(self.cache_dir, f"{key}.json")

    def _cache_get(self, key: str) -> Optional[str]:
        path = self._cache_path(key)
        if os.path.exists(path):
            try:
                with open(path) as f:
                    return json.load(f).get("text")
            except Exception:
                pass
        return None

    def _cache_set(self, key: str, text: str) -> None:
        os.makedirs(self.cache_dir, exist_ok=True)
        path = self._cache_path(key)
        try:
            with open(path, "w") as f:
                json.dump({"text": text, "ts": datetime.now(timezone.utc).isoformat()}, f)
        except Exception:
            pass


# ── Helpers ───────────────────────────────────────────────────────────────────

def _cache_key(prompt: str) -> str:
    return hashlib.sha256(prompt.encode()).hexdigest()[:16]


def _format_context(ctx: Dict[str, Any]) -> str:
    if not ctx:
        return "none"
    parts = []
    for k, v in ctx.items():
        if k.startswith("llm_"):
            continue
        if isinstance(v, float):
            parts.append(f"{k}={v:.4g}")
        elif isinstance(v, list) and len(v) > 5:
            parts.append(f"{k}=[{', '.join(str(x) for x in v[:5])}…]")
        else:
            parts.append(f"{k}={v}")
    return ", ".join(parts) if parts else "none"
