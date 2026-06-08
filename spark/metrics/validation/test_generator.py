"""
TestGenerator — Semaine 11 : génération automatique de règles de qualité.

Analyse l'historique des PipelineMetrics et produit des GeneratedRule
calibrées sur les statistiques observées.

Règles générées par métrique :
  row_count        → [0, mean + σ*k]   (borne basse toujours 0)
  duration_seconds → [0, mean + σ*k]   (SLA-like)
  task_failures    → [0, mean + σ*k]
  success          → True attendu
  column_stats.*   → [mean - σ*k, mean + σ*k] pour chaque stat numérique

Usage :
    from spark.metrics.validation import TestGenerator
    from spark.metrics.anomaly_detection.models import PipelineMetrics

    gen = TestGenerator(sigma_factor=3.0, min_samples=5)
    rules = gen.generate(history_snapshots)
    gen.save_rules(rules, "rules_ingest_sales.json")
"""
from __future__ import annotations

import json
import math
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from ..anomaly_detection.models import PipelineMetrics
from .models import GeneratedRule


class TestGenerator:
    """
    Génère des règles de qualité à partir d'une liste de PipelineMetrics historiques.

    Parameters
    ----------
    sigma_factor : float
        Nombre de σ pour les bornes (défaut : 3.0).
    min_samples  : int
        Nombre minimal d'échantillons pour générer une règle avec confiance > 0.
    """

    def __init__(self, sigma_factor: float = 3.0, min_samples: int = 5) -> None:
        self.sigma_factor = sigma_factor
        self.min_samples  = min_samples

    # ── Public API ────────────────────────────────────────────────────────────

    def generate(self, history: List[PipelineMetrics]) -> List[GeneratedRule]:
        """Génère toutes les règles depuis l'historique."""
        if not history:
            return []
        pipeline = history[0].pipeline_name
        rules: List[GeneratedRule] = []

        rules.extend(self._rules_for_row_count(pipeline, history))
        rules.extend(self._rules_for_duration(pipeline, history))
        rules.extend(self._rules_for_failures(pipeline, history))
        rules.extend(self._rules_for_success(pipeline, history))
        rules.extend(self._rules_for_column_stats(pipeline, history))

        return rules

    def save_rules(self, rules: List[GeneratedRule], path: str) -> None:
        """Persiste les règles en JSON."""
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w") as f:
            json.dump([r.to_dict() for r in rules], f, indent=2)

    @staticmethod
    def load_rules(path: str) -> List[GeneratedRule]:
        """Charge des règles depuis un fichier JSON."""
        with open(path) as f:
            return [GeneratedRule.from_dict(d) for d in json.load(f)]

    # ── Rule builders ─────────────────────────────────────────────────────────

    def _rules_for_row_count(
        self, pipeline: str, history: List[PipelineMetrics]
    ) -> List[GeneratedRule]:
        values = [m.row_count for m in history if m.row_count is not None]
        if not values:
            return []
        mean, std, n = _stats(values)
        high = mean + self.sigma_factor * std if std > 0 else mean * 2
        return [self._make_rule(
            pipeline, "row_count", n,
            threshold_low=0.0,
            threshold_high=max(high, 1.0),
            mean=mean, std=std,
        )]

    def _rules_for_duration(
        self, pipeline: str, history: List[PipelineMetrics]
    ) -> List[GeneratedRule]:
        values = [m.duration_seconds for m in history if m.duration_seconds is not None]
        if not values:
            return []
        mean, std, n = _stats(values)
        high = mean + self.sigma_factor * std if std > 0 else mean * 3
        return [self._make_rule(
            pipeline, "duration_seconds", n,
            threshold_low=0.0,
            threshold_high=max(high, 1.0),
            mean=mean, std=std,
        )]

    def _rules_for_failures(
        self, pipeline: str, history: List[PipelineMetrics]
    ) -> List[GeneratedRule]:
        values = [float(m.task_failures) for m in history if m.task_failures is not None]
        if not values:
            return []
        mean, std, n = _stats(values)
        high = mean + self.sigma_factor * std if std > 0 else max(mean * 2, 1.0)
        return [self._make_rule(
            pipeline, "task_failures", n,
            threshold_low=0.0,
            threshold_high=max(high, 0.0),
            mean=mean, std=std,
        )]

    def _rules_for_success(
        self, pipeline: str, history: List[PipelineMetrics]
    ) -> List[GeneratedRule]:
        values = [m.success for m in history if m.success is not None]
        if not values:
            return []
        success_rate = sum(1 for v in values if v) / len(values)
        rule = self._make_rule(
            pipeline, "success", len(values),
            threshold_low=1.0,   # 1 = True ; False = 0 → violation
            threshold_high=1.0,
            mean=success_rate, std=0.0,
        )
        return [rule]

    def _rules_for_column_stats(
        self, pipeline: str, history: List[PipelineMetrics]
    ) -> List[GeneratedRule]:
        """Génère une règle par (colonne, stat) présente dans column_stats."""
        aggregated: Dict[str, List[float]] = {}
        for m in history:
            for col, stats in m.column_stats.items():
                for stat_name, val in stats.items():
                    key = f"col:{col}.{stat_name}"
                    aggregated.setdefault(key, []).append(val)

        rules = []
        for key, values in aggregated.items():
            if len(values) < 2:
                continue
            mean, std, n = _stats(values)
            low  = mean - self.sigma_factor * std if std > 0 else mean * 0.5
            high = mean + self.sigma_factor * std if std > 0 else mean * 2.0
            # null_rate doit toujours être ≥ 0
            if "null_rate" in key:
                low = max(low, 0.0)
            rules.append(self._make_rule(
                pipeline, key, n,
                threshold_low=low,
                threshold_high=high,
                mean=mean, std=std,
            ))
        return rules

    # ── Helper ────────────────────────────────────────────────────────────────

    def _make_rule(
        self,
        pipeline: str,
        metric: str,
        n: int,
        *,
        threshold_low: Optional[float],
        threshold_high: Optional[float],
        mean: float,
        std: float,
    ) -> GeneratedRule:
        confidence = min(1.0, n / max(self.min_samples * 2, 1))
        rule_id = f"{pipeline}__{metric.replace(':', '_').replace('.', '_')}__{int(self.sigma_factor*10)}s"
        return GeneratedRule(
            rule_id=rule_id,
            pipeline_name=pipeline,
            metric_name=metric,
            sigma_factor=self.sigma_factor,
            threshold_low=round(threshold_low, 4) if threshold_low is not None else None,
            threshold_high=round(threshold_high, 4) if threshold_high is not None else None,
            mean=round(mean, 4),
            std=round(std, 4),
            n_samples=n,
            confidence=round(confidence, 3),
        )


# ── Stats helpers ─────────────────────────────────────────────────────────────

def _stats(values: List[float]) -> Tuple[float, float, int]:
    n    = len(values)
    mean = sum(values) / n
    variance = sum((v - mean) ** 2 for v in values) / max(n - 1, 1)
    std  = math.sqrt(variance)
    return mean, std, n
