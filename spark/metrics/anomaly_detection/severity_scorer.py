"""
Severity Scorer
----------------
Post-processes all anomalies from every detector and assigns a final
numeric score (0–100) plus a Severity enum value.

Scoring factors:
  1. Base score from anomaly level  (schema > volume > distribution/perf > temporal > ml)
  2. Sigma multiplier               (more σ = higher score)
  3. Recurrence bonus               (seen before = escalate)
  4. Pipeline criticality weight    (configurable per pipeline)
  5. ML agreement bonus             (if ML also flagged = escalate)
"""
from __future__ import annotations

from typing import Dict, List, Optional

from .models import Anomaly, AnomalyLevel, Severity


# Base scores per detection level
LEVEL_BASE_SCORES: Dict[str, float] = {
    AnomalyLevel.SCHEMA:        70.0,
    AnomalyLevel.VOLUME:        55.0,
    AnomalyLevel.PERFORMANCE:   55.0,
    AnomalyLevel.DISTRIBUTION:  45.0,
    AnomalyLevel.TEMPORAL:      40.0,
    AnomalyLevel.ML:            35.0,
}

# Score thresholds for final Severity label
SCORE_TO_SEVERITY = [
    (80.0, Severity.CRITICAL),
    (60.0, Severity.HIGH),
    (40.0, Severity.MEDIUM),
    (0.0,  Severity.LOW),
]


class SeverityScorer:
    """
    Assign a severity score (0–100) and final Severity label to each anomaly.

    Parameters
    ----------
    pipeline_weights : dict[str, float]
        Per-pipeline criticality multiplier (default 1.0).
        e.g. {"sales_analytics": 1.5, "inventory": 0.8}
    recurrence_window : int
        How many historical anomaly events to check for recurrence.
    ml_agreement_bonus : float
        Score bonus when an ML detector also flagged the same metric.
    """

    def __init__(
        self,
        pipeline_weights: Optional[Dict[str, float]] = None,
        recurrence_window: int = 7,
        ml_agreement_bonus: float = 10.0,
    ) -> None:
        self.pipeline_weights = pipeline_weights or {}
        self.recurrence_window = recurrence_window
        self.ml_agreement_bonus = ml_agreement_bonus

    # ------------------------------------------------------------------

    def score(
        self,
        anomalies: List[Anomaly],
        anomaly_history: Optional[List[Anomaly]] = None,
        ml_flagged_metrics: Optional[set] = None,
    ) -> List[Anomaly]:
        """
        Score and mutate each Anomaly in-place, then return the list.

        Parameters
        ----------
        anomalies : list[Anomaly]
            Anomalies from the current detection run.
        anomaly_history : list[Anomaly] | None
            Recent past anomalies for recurrence checking.
        ml_flagged_metrics : set[str] | None
            Set of metric names flagged by the ML detector this run.
        """
        history = anomaly_history or []
        ml_metrics = ml_flagged_metrics or set()

        # Build recurrence lookup: (pipeline, metric) → count in history
        recurrence_counts: Dict[tuple, int] = {}
        for past in history[-self.recurrence_window * 50:]:  # keep reasonable limit
            key = (past.pipeline_name, past.metric_name, past.level)
            recurrence_counts[key] = recurrence_counts.get(key, 0) + 1

        for anomaly in anomalies:
            score = self._compute_score(anomaly, recurrence_counts, ml_metrics)
            anomaly.severity_score = round(min(score, 100.0), 2)
            anomaly.severity = self._score_to_severity(anomaly.severity_score)

            # Set recurrence fields
            key = (anomaly.pipeline_name, anomaly.metric_name, anomaly.level)
            count = recurrence_counts.get(key, 0)
            anomaly.is_recurring = count > 0
            anomaly.occurrence_count = count + 1   # include current

        return anomalies

    # ------------------------------------------------------------------

    def _compute_score(
        self,
        anomaly: Anomaly,
        recurrence_counts: Dict[tuple, int],
        ml_metrics: set,
    ) -> float:
        # 1. Base from level
        score = LEVEL_BASE_SCORES.get(anomaly.level, 35.0)

        # 2. Sigma multiplier (caps at +20)
        if anomaly.deviation_sigma is not None:
            sigma_bonus = min((anomaly.deviation_sigma - 3.0) * 4.0, 20.0)
            score += max(sigma_bonus, 0.0)

        # 3. Recurrence bonus (up to +15)
        key = (anomaly.pipeline_name, anomaly.metric_name, anomaly.level)
        count = recurrence_counts.get(key, 0)
        if count > 0:
            recurrence_bonus = min(count * 3.0, 15.0)
            score += recurrence_bonus

        # 4. Pipeline criticality weight
        weight = self.pipeline_weights.get(anomaly.pipeline_name, 1.0)
        score *= weight

        # 5. ML agreement bonus
        if anomaly.metric_name in ml_metrics and anomaly.level != AnomalyLevel.ML:
            score += self.ml_agreement_bonus

        return score

    # ------------------------------------------------------------------

    @staticmethod
    def _score_to_severity(score: float) -> Severity:
        for threshold, severity in SCORE_TO_SEVERITY:
            if score >= threshold:
                return severity
        return Severity.LOW

    # ------------------------------------------------------------------

    def summarize(self, anomalies: List[Anomaly]) -> Dict:
        """Return a human-readable scoring summary."""
        if not anomalies:
            return {"total": 0, "scored": []}

        scored = sorted(
            [
                {
                    "id": a.id,
                    "pipeline": a.pipeline_name,
                    "level": a.level,
                    "metric": a.metric_name,
                    "score": a.severity_score,
                    "severity": a.severity,
                    "recurring": a.is_recurring,
                    "occurrences": a.occurrence_count,
                }
                for a in anomalies
            ],
            key=lambda x: x["score"],
            reverse=True,
        )

        return {
            "total": len(anomalies),
            "critical": sum(1 for a in anomalies if a.severity == Severity.CRITICAL),
            "high":     sum(1 for a in anomalies if a.severity == Severity.HIGH),
            "medium":   sum(1 for a in anomalies if a.severity == Severity.MEDIUM),
            "low":      sum(1 for a in anomalies if a.severity == Severity.LOW),
            "scored": scored,
        }
