"""
HistoricalValidator — Semaine 11 : exécution des règles sur données historiques labelisées.

Principe :
  - Un LabeledSnapshot associe un PipelineMetrics à un label is_anomaly (bool).
  - Pour chaque règle, on évalue chaque snapshot :
      rule.evaluate() = False  → règle détecte une anomalie
      label = True             → anomalie réelle
    => TP si les deux matchent, FP si seulement la règle, FN si seulement le label.

Usage :
    from spark.metrics.validation import HistoricalValidator, LabeledSnapshot

    labeled = [
        LabeledSnapshot(metrics=m, is_anomaly=False)   # normal
        for m in history_clean
    ] + [
        LabeledSnapshot(metrics=m, is_anomaly=True)    # injecté
        for m in history_anomaly
    ]

    validator = HistoricalValidator()
    results = validator.validate(rules, labeled)
    report  = validator.report(results)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ..anomaly_detection.models import PipelineMetrics
from .models import GeneratedRule, ValidationResult


@dataclass
class LabeledSnapshot:
    """Un snapshot de métriques avec son label de vérité terrain."""
    metrics: PipelineMetrics
    is_anomaly: bool
    anomaly_types: List[str] = field(default_factory=list)


class HistoricalValidator:
    """
    Évalue des GeneratedRules sur un ensemble de LabeledSnapshot
    et retourne les métriques de qualité (precision, recall, F1).
    """

    def validate(
        self,
        rules: List[GeneratedRule],
        labeled: List[LabeledSnapshot],
    ) -> List[ValidationResult]:
        """Retourne un ValidationResult par règle."""
        return [self._eval_rule(rule, labeled) for rule in rules]

    def report(self, results: List[ValidationResult]) -> Dict[str, Any]:
        """Résumé agrégé des résultats de validation."""
        if not results:
            return {"rules": 0, "mean_f1": 0.0, "mean_precision": 0.0, "mean_recall": 0.0}

        f1s  = [r.f1        for r in results]
        prec = [r.precision for r in results]
        rec  = [r.recall    for r in results]

        return {
            "rules": len(results),
            "mean_f1":        round(sum(f1s)  / len(f1s),  3),
            "mean_precision": round(sum(prec) / len(prec), 3),
            "mean_recall":    round(sum(rec)  / len(rec),  3),
            "best_rule":  max(results, key=lambda r: r.f1).rule_id,
            "worst_rule": min(results, key=lambda r: r.f1).rule_id,
            "details": [r.summary() for r in results],
        }

    # ── Internal ──────────────────────────────────────────────────────────────

    def _eval_rule(
        self, rule: GeneratedRule, labeled: List[LabeledSnapshot]
    ) -> ValidationResult:
        result = ValidationResult(
            rule_id=rule.rule_id,
            pipeline_name=rule.pipeline_name,
            metric_name=rule.metric_name,
        )

        for snap in labeled:
            # Extraire la valeur de la métrique depuis le snapshot
            value = _extract_metric(snap.metrics, rule.metric_name)

            # rule.evaluate() returns True si pas d'anomalie, False si anomalie
            rule_fires = not rule.evaluate(value)   # True = anomalie signalée
            actual     = snap.is_anomaly

            if rule_fires and actual:
                result.tp += 1
            elif rule_fires and not actual:
                result.fp += 1
            elif not rule_fires and actual:
                result.fn += 1
            else:
                result.tn += 1

        return result


# ── Helpers ───────────────────────────────────────────────────────────────────

def _extract_metric(m: PipelineMetrics, metric_name: str) -> Optional[float]:
    """
    Extrait une valeur numérique depuis PipelineMetrics selon le nom de métrique.

    Noms supportés :
      row_count, duration_seconds, task_failures, success
      col:<column>.<stat>   (ex. col:amount.null_rate)
    """
    if metric_name == "row_count":
        return float(m.row_count) if m.row_count is not None else None
    if metric_name == "duration_seconds":
        return m.duration_seconds
    if metric_name == "task_failures":
        return float(m.task_failures) if m.task_failures is not None else None
    if metric_name == "success":
        return float(m.success) if m.success is not None else None

    # col:<column>.<stat>
    if metric_name.startswith("col:"):
        rest = metric_name[4:]            # "amount.null_rate"
        dot  = rest.rfind(".")
        if dot == -1:
            return None
        col, stat = rest[:dot], rest[dot+1:]
        col_stats = m.column_stats.get(col, {})
        val = col_stats.get(stat)
        return float(val) if val is not None else None

    # extra dict (fallback)
    val = m.extra.get(metric_name)
    return float(val) if val is not None else None
