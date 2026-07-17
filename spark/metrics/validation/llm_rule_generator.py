"""
LLMRuleGenerator — déduction de règles de qualité par Mistral AI.

Complète TestGenerator (statistique pure, mean ± k·σ) avec une analyse LLM :

  Règles techniques   — déduites des logs d'exécution (échecs dbt, erreurs
                        Airflow, timeouts) : ce que les statistiques sur
                        metrics.db ne capturent pas (un message d'erreur
                        récurrent, une cause racine).
  Règles fonctionnelles — déduites du comportement métier du pipeline
                        (volumes, taux de rejet, durée) à partir du même
                        historique PipelineMetrics que TestGenerator, mais
                        avec un raisonnement en langage naturel plutôt
                        qu'un simple calcul de moyenne/écart-type.

Les règles produites passent par le même circuit d'approbation que
TestGenerator (RuleStore, statut "pending"), avec `source="llm"` et un champ
`reasoning` renseigné pour que l'opérateur voie la justification avant
d'approuver/rejeter.

Usage :
    from spark.metrics.validation.llm_rule_generator import LLMRuleGenerator
    from spark.catalog.rule_store import RuleStore

    gen   = LLMRuleGenerator()
    rules = gen.generate(history, dbt_target_dir="dbt/target",
                          dbt_log_path="dbt/logs/dbt.log", airflow_log_dir="logs")
    RuleStore().save_rules(rules)   # statut "pending", à valider dans le dashboard
"""
from __future__ import annotations

import glob
import json
import os
import statistics
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..anomaly_detection.models import PipelineMetrics
from .models import GeneratedRule

_SYSTEM_PROMPT = (
    "Tu es un ingénieur qualité de données senior. On te donne : (1) un résumé "
    "statistique des données clients traitées par un pipeline (volumes, valeurs "
    "nulles, doublons, min/max observés par colonne), et (2) des extraits de logs "
    "techniques (dbt, Airflow). Ta tâche : proposer des règles de qualité en "
    "distinguant strictement :\n"
    "  - 'technical' : dérivées des LOGS (erreurs, échecs, timeouts, rollbacks "
    "observés dans dbt.log/Airflow) — jamais des statistiques sur les données\n"
    "  - 'functional' : des INTERVALLES MÉTIER PLAUSIBLES sur les champs clients, "
    "raisonnés à partir du SENS de la colonne (pas seulement de sa moyenne/écart-type). "
    "Exemple : pour une colonne 'age', l'intervalle plausible est [0, 120] — si le "
    "min/max observé sort largement de cette plage (ex. âge à 200 ou -5), c'est une "
    "erreur de qualité à signaler, même si statistiquement peu fréquent. Fais ce "
    "raisonnement pour chaque colonne pertinente : quantité (doit être positive et "
    "raisonnable), prix/montant (positif, pas aberrant), date (dans une plage "
    "calendaire plausible), taux/pourcentage (entre 0 et 100), etc. Compare toujours "
    "l'intervalle métier plausible que tu déduis aux min/max RÉELLEMENT observés "
    "fournis en entrée pour dire s'il y a déjà une violation.\n"
    "Réponds UNIQUEMENT en JSON valide, sans texte autour, au format :\n"
    '{"rules": [{"metric_name": "...", "rule_type": "technical|functional", '
    '"threshold_low": <float|null>, "threshold_high": <float|null>, '
    '"reasoning": "justification courte en français, précise si une violation est '
    'déjà observée dans les données fournies"}]}\n'
    "N'invente pas de métrique sans justification tirée des données fournies. "
    "Si rien de pertinent ne peut être déduit, réponds {\"rules\": []}."
)


class LLMRuleGenerator:
    """Génère des GeneratedRule via Mistral, à partir des logs + de l'historique."""

    def __init__(
        self,
        model: str = "mistral-large-latest",
        api_key: Optional[str] = None,
        max_tokens: int = 1024,
        max_log_chars: int = 4000,
    ) -> None:
        self.model = os.environ.get("METRICS_LLM_MODEL", model)
        self.api_key = api_key or os.environ.get("MISTRAL_API_KEY", "")
        self.max_tokens = max_tokens
        self.max_log_chars = max_log_chars
        self._client = None

    @property
    def configured(self) -> bool:
        return bool(self.api_key)

    # ── API publique ──────────────────────────────────────────────────────────

    def generate(
        self,
        history: List[PipelineMetrics],
        dbt_target_dir: str = "dbt/target",
        dbt_log_path: str = "dbt/logs/dbt.log",
        airflow_log_dir: str = "logs",
    ) -> List[GeneratedRule]:
        """Propose des règles techniques + fonctionnelles pour le pipeline de `history`."""
        if not history or not self.configured:
            return []
        pipeline = history[0].pipeline_name

        functional_summary = self._summarize_functional(history)
        technical_summary = self._summarize_technical(
            pipeline, dbt_target_dir, dbt_log_path, airflow_log_dir
        )
        prompt = self._build_prompt(pipeline, functional_summary, technical_summary)

        try:
            raw = self._mistral_json_call(prompt)
        except Exception:
            return []

        proposals = self._parse(raw)
        return [
            self._to_rule(pipeline, p) for p in proposals
            if p.get("metric_name")
        ]

    # ── Résumés fournis au LLM ────────────────────────────────────────────────

    def _summarize_functional(self, history: List[PipelineMetrics]) -> str:
        lines = [f"Pipeline : {history[0].pipeline_name}", f"Échantillons : {len(history)}"]

        row_counts = [m.row_count for m in history if m.row_count is not None]
        if row_counts:
            lines.append(
                f"row_count : moyenne={statistics.mean(row_counts):.1f} "
                f"min={min(row_counts):.1f} max={max(row_counts):.1f}"
            )

        durations = [m.duration_seconds for m in history if m.duration_seconds is not None]
        if durations:
            lines.append(
                f"duration_seconds : moyenne={statistics.mean(durations):.1f} "
                f"min={min(durations):.1f} max={max(durations):.1f}"
            )

        failures = [m.task_failures for m in history if m.task_failures is not None]
        if failures:
            lines.append(f"task_failures : moyenne={statistics.mean(failures):.2f}")

        successes = [m.success for m in history if m.success is not None]
        if successes:
            rate = sum(1 for s in successes if s) / len(successes)
            lines.append(f"success_rate : {rate:.1%}")

        # Données clients — stats par colonne (null_rate, distinct_count, moyenne...)
        # C'est la matière première des règles "functional" : la définition métier
        # porte sur les CHAMPS CLIENTS eux-mêmes, pas sur le volume/la durée du run.
        client_cols: Dict[str, List[Dict[str, float]]] = {}
        for m in history:
            for col, stats in m.column_stats.items():
                client_cols.setdefault(col, []).append(stats)

        if client_cols:
            lines.append("\nDonnées clients — statistiques par colonne :")
            for col, samples in client_cols.items():
                stat_names = {k for s in samples for k in s}
                for stat_name in stat_names:
                    values = [s[stat_name] for s in samples if stat_name in s]
                    if values:
                        lines.append(
                            f"  {col}.{stat_name} : moyenne={statistics.mean(values):.3f} "
                            f"min={min(values):.3f} max={max(values):.3f}"
                        )

        return "\n".join(lines)

    def _summarize_technical(
        self, pipeline: str, dbt_target_dir: str, dbt_log_path: str, airflow_log_dir: str
    ) -> str:
        parts: List[str] = []

        run_results = os.path.join(dbt_target_dir, "run_results.json")
        if os.path.isfile(run_results):
            parts.append("── dbt run_results.json ──\n" + self._summarize_dbt_run_results(run_results))

        if os.path.isfile(dbt_log_path):
            parts.append("── dbt.log (extrait) ──\n" + self._tail_file(dbt_log_path))

        airflow_excerpt = self._tail_airflow_logs(airflow_log_dir, pipeline)
        if airflow_excerpt:
            parts.append("── logs Airflow (extrait) ──\n" + airflow_excerpt)

        return "\n\n".join(parts) if parts else "(aucun log technique disponible)"

    def _summarize_dbt_run_results(self, path: str) -> str:
        try:
            with open(path) as f:
                data = json.load(f)
        except Exception:
            return "(illisible)"

        results = data.get("results", [])
        by_status: Dict[str, int] = {}
        failed_names = []
        for r in results:
            status = r.get("status", "unknown")
            by_status[status] = by_status.get(status, 0) + 1
            if status in ("fail", "error"):
                name = (r.get("unique_id") or "").split(".")[-1]
                failed_names.append(name)

        lines = [f"Statuts : {by_status}"]
        if failed_names:
            lines.append(f"Modèles/tests en échec : {failed_names[:20]}")
        return "\n".join(lines)

    def _tail_file(self, path: str, max_lines: int = 80) -> str:
        try:
            with open(path, errors="replace") as f:
                lines = f.readlines()
        except Exception:
            return "(illisible)"
        return "".join(lines[-max_lines:])[: self.max_log_chars]

    def _tail_airflow_logs(self, log_dir: str, pipeline: str) -> str:
        if not os.path.isdir(log_dir):
            return ""
        candidates = glob.glob(os.path.join(log_dir, "**", f"*{pipeline}*"), recursive=True)
        candidates += glob.glob(os.path.join(log_dir, "**", "*.log"), recursive=True)
        candidates = sorted(set(candidates), key=lambda p: os.path.getmtime(p), reverse=True)[:5]
        if not candidates:
            return ""

        chunks = []
        budget = self.max_log_chars
        for path in candidates:
            if budget <= 0:
                break
            text = self._tail_file(path, max_lines=60)[:budget]
            chunks.append(f"[{os.path.basename(path)}]\n{text}")
            budget -= len(text)
        return "\n".join(chunks)

    def _build_prompt(self, pipeline: str, functional_summary: str, technical_summary: str) -> str:
        return (
            f"Pipeline analysé : {pipeline}\n\n"
            f"=== Comportement fonctionnel (historique métriques) ===\n{functional_summary}\n\n"
            f"=== Logs techniques ===\n{technical_summary}\n"
        )

    # ── Mistral ───────────────────────────────────────────────────────────────

    def _get_client(self):
        if self._client is None:
            try:
                from mistralai import Mistral
            except ImportError:
                from mistralai.client import Mistral  # mistralai>=2.x
            self._client = Mistral(api_key=self.api_key)
        return self._client

    def _mistral_json_call(self, prompt: str) -> str:
        client = self._get_client()
        response = client.chat.complete(
            model=self.model,
            messages=[
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            max_tokens=self.max_tokens,
            response_format={"type": "json_object"},
        )
        return response.choices[0].message.content.strip()

    @staticmethod
    def _parse(raw: str) -> List[Dict[str, Any]]:
        try:
            data = json.loads(raw)
        except Exception:
            return []
        rules = data.get("rules", []) if isinstance(data, dict) else data
        return rules if isinstance(rules, list) else []

    def _to_rule(self, pipeline: str, proposal: Dict[str, Any]) -> GeneratedRule:
        metric = str(proposal["metric_name"])
        rule_type = str(proposal.get("rule_type", "functional"))
        rule_id = f"{pipeline}__{metric.replace(':', '_').replace('.', '_')}__llm_{rule_type}"
        return GeneratedRule(
            rule_id=rule_id,
            pipeline_name=pipeline,
            metric_name=metric,
            threshold_low=_as_float(proposal.get("threshold_low")),
            threshold_high=_as_float(proposal.get("threshold_high")),
            confidence=0.5,   # pas basé sur n_samples ; à ajuster par FeedbackLoop après revue
            generated_at=datetime.now(timezone.utc),
            source=f"llm:{rule_type}",
            reasoning=str(proposal.get("reasoning", "")),
        )


def _as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
