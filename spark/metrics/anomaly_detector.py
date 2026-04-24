"""
Détection d'anomalies sur les séries temporelles de métriques pipeline.

Quatre algorithmes disponibles, combinables :

  zscore     — signale les points où |z| > seuil (défaut 3.0)
               Bon pour : distributions normales, pics ponctuels
  iqr        — signale les valeurs hors [Q1 - k*IQR, Q3 + k*IQR] (k défaut 1.5)
               Bon pour : distributions asymétriques, robuste aux outliers
  threshold  — règles statiques min/max par métrique
               Bon pour : contraintes métier connues (ex. rejection_rate > 20 %)
  trend      — signale N dégradations consécutives (défaut 3)
               Bon pour : dérives lentes (ex. durée qui augmente chaque run)

Usage :
    from spark.metrics.anomaly_detector import AnomalyDetector
    from spark.metrics.storage import get_store

    store    = get_store()
    detector = AnomalyDetector(store)
    alerts   = detector.run()          # détecte sur toutes les métriques
    for a in alerts:
        print(a)
"""

from __future__ import annotations

import math
import os
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import sys
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

try:
    from .storage import SQLiteMetricsStore, get_store
except ImportError:
    from spark.metrics.storage import SQLiteMetricsStore, get_store


# ── Règles de seuils statiques (métier) ───────────────────────────────────────

DEFAULT_THRESHOLDS: dict[str, dict] = {
    "job.rejection_rate_pct":   {"max": 20.0,  "severity": "critical"},
    "job.duration_seconds":     {"max": 300.0, "severity": "warning"},
    "dag.duration_seconds":     {"max": 1800.0,"severity": "warning"},
    "dag.status":               {"min": 1.0,   "severity": "critical"},   # 0 = échec
    "task.try_number":          {"max": 3.0,   "severity": "warning"},
    "dbt.run.pass_rate":        {"min": 80.0,  "severity": "critical"},
    "dbt.model.status":         {"min": 1.0,   "severity": "critical"},
    "job.rows_output":          {"min": 1.0,   "severity": "warning"},    # 0 lignes = suspect
}


# ── Dataclass résultat ─────────────────────────────────────────────────────────

@dataclass
class AnomalyAlert:
    """Résultat d'une détection d'anomalie sur une série de métriques."""
    metric_name:  str
    source:       str
    algorithm:    str                    # zscore | iqr | threshold | trend
    severity:     str                    # critical | warning | info
    value:        float
    expected:     str                    # description de la plage normale
    ts:           str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    tags:         dict = field(default_factory=dict)
    details:      str = ""

    def __str__(self) -> str:
        return (
            f"[{self.severity.upper()}] {self.source}/{self.metric_name} "
            f"— {self.algorithm} — valeur={self.value:.3g} ({self.expected}) "
            f"| {self.details}"
        )


# ── Détecteur principal ────────────────────────────────────────────────────────

class AnomalyDetector:
    """
    Détecte des anomalies dans les métriques stockées dans SQLite.

    Paramètres
    ----------
    store           : instance SQLiteMetricsStore (ou compatible)
    zscore_thresh   : seuil z-score (défaut 3.0)
    iqr_k           : facteur IQR (défaut 1.5)
    trend_n         : nb dégradations consécutives pour déclencher trend (défaut 3)
    min_history     : nb minimum de points pour activer zscore/iqr/trend (défaut 5)
    thresholds      : règles statiques {metric_name: {min?, max?, severity}}
    algorithms      : liste d'algorithmes à activer (défaut : tous)
    """

    def __init__(
        self,
        store: Optional[SQLiteMetricsStore] = None,
        zscore_thresh: float = 3.0,
        iqr_k:         float = 1.5,
        trend_n:       int   = 3,
        min_history:   int   = 5,
        thresholds:    Optional[dict] = None,
        algorithms:    Optional[list] = None,
    ):
        self.store         = store or get_store()
        self.zscore_thresh = zscore_thresh
        self.iqr_k         = iqr_k
        self.trend_n       = trend_n
        self.min_history   = min_history
        self.thresholds    = thresholds if thresholds is not None else DEFAULT_THRESHOLDS
        self.algorithms    = set(algorithms or ["zscore", "iqr", "threshold", "trend"])

    # ── API publique ──────────────────────────────────────────────────────────

    def run(self, source: Optional[str] = None, limit: int = 1000) -> list[AnomalyAlert]:
        """Lance tous les algorithmes sur les métriques disponibles."""
        summary = self.store.summary()
        alerts: list[AnomalyAlert] = []

        for row in summary:
            if source and row["source"] != source:
                continue
            src  = row["source"]
            name = row["name"]

            # Récupérer la série temporelle (triée chronologiquement)
            points = self.store.query(source=src, name=name, limit=limit)
            points.sort(key=lambda p: p["ts"])
            values = [p["value"] for p in points if p["value"] is not None]

            if not values:
                continue

            last_val  = values[-1]
            last_tags = points[-1].get("tags", {}) if points else {}
            last_ts   = points[-1]["ts"] if points else datetime.now(timezone.utc).isoformat()

            # ── Seuils statiques ──────────────────────────────────────────
            if "threshold" in self.algorithms:
                a = self._check_threshold(name, src, last_val, last_ts, last_tags)
                if a:
                    alerts.append(a)

            if len(values) < self.min_history:
                continue

            # ── Z-score ───────────────────────────────────────────────────
            if "zscore" in self.algorithms:
                a = self._check_zscore(name, src, values, last_ts, last_tags)
                if a:
                    alerts.append(a)

            # ── IQR ───────────────────────────────────────────────────────
            if "iqr" in self.algorithms:
                a = self._check_iqr(name, src, values, last_ts, last_tags)
                if a:
                    alerts.append(a)

            # ── Tendance ──────────────────────────────────────────────────
            if "trend" in self.algorithms:
                a = self._check_trend(name, src, values, last_ts, last_tags)
                if a:
                    alerts.append(a)

        return alerts

    # ── Algorithmes privés ────────────────────────────────────────────────────

    def _check_threshold(
        self, name: str, source: str, value: float, ts: str, tags: dict
    ) -> Optional[AnomalyAlert]:
        rule = self.thresholds.get(name)
        if not rule:
            return None

        vmin = rule.get("min")
        vmax = rule.get("max")
        sev  = rule.get("severity", "warning")

        if vmin is not None and value < vmin:
            return AnomalyAlert(
                metric_name=name, source=source, algorithm="threshold",
                severity=sev, value=value, ts=ts, tags=tags,
                expected=f">= {vmin}",
                details=f"valeur {value:.3g} < min autorisé {vmin}",
            )
        if vmax is not None and value > vmax:
            return AnomalyAlert(
                metric_name=name, source=source, algorithm="threshold",
                severity=sev, value=value, ts=ts, tags=tags,
                expected=f"<= {vmax}",
                details=f"valeur {value:.3g} > max autorisé {vmax}",
            )
        return None

    def _check_zscore(
        self, name: str, source: str, values: list, ts: str, tags: dict
    ) -> Optional[AnomalyAlert]:
        if len(values) < 2:
            return None
        # Baseline = tous les points sauf le dernier (évite que l'outlier gonfle σ)
        baseline = values[:-1]
        mean = statistics.mean(baseline)
        try:
            std = statistics.stdev(baseline)
        except statistics.StatisticsError:
            return None
        if std == 0:
            return None

        last = values[-1]
        z = (last - mean) / std
        if abs(z) <= self.zscore_thresh:
            return None

        sev = "critical" if abs(z) > self.zscore_thresh * 1.5 else "warning"
        return AnomalyAlert(
            metric_name=name, source=source, algorithm="zscore",
            severity=sev, value=last, ts=ts, tags=tags,
            expected=f"μ={mean:.3g} ± {self.zscore_thresh}σ={std:.3g}",
            details=f"z-score={z:.2f} (seuil ±{self.zscore_thresh})",
        )

    def _check_iqr(
        self, name: str, source: str, values: list, ts: str, tags: dict
    ) -> Optional[AnomalyAlert]:
        sorted_vals = sorted(values)
        n = len(sorted_vals)
        q1 = sorted_vals[n // 4]
        q3 = sorted_vals[(3 * n) // 4]
        iqr = q3 - q1
        if iqr == 0:
            return None

        lower = q1 - self.iqr_k * iqr
        upper = q3 + self.iqr_k * iqr
        last  = values[-1]

        if lower <= last <= upper:
            return None

        direction = "above" if last > upper else "below"
        sev = "critical" if abs(last - (upper if direction == "above" else lower)) > 2 * iqr else "warning"
        return AnomalyAlert(
            metric_name=name, source=source, algorithm="iqr",
            severity=sev, value=last, ts=ts, tags=tags,
            expected=f"[{lower:.3g}, {upper:.3g}]",
            details=f"valeur {direction} la plage IQR (Q1={q1:.3g}, Q3={q3:.3g}, IQR={iqr:.3g})",
        )

    def _check_trend(
        self, name: str, source: str, values: list, ts: str, tags: dict
    ) -> Optional[AnomalyAlert]:
        if len(values) < self.trend_n + 1:
            return None

        tail = values[-(self.trend_n + 1):]
        # Dégradation = augmentation continue (durée, taux de rejet) OU diminution (rows_output, pass_rate)
        is_increasing = all(tail[i] < tail[i + 1] for i in range(len(tail) - 1))
        is_decreasing = all(tail[i] > tail[i + 1] for i in range(len(tail) - 1))

        if not (is_increasing or is_decreasing):
            return None

        direction  = "hausse" if is_increasing else "baisse"
        pct_change = abs((tail[-1] - tail[0]) / tail[0] * 100) if tail[0] != 0 else 0
        sev        = "critical" if pct_change > 50 else "warning"

        return AnomalyAlert(
            metric_name=name, source=source, algorithm="trend",
            severity=sev, value=tail[-1], ts=ts, tags=tags,
            expected=f"stable (variation < 50%)",
            details=(
                f"{self.trend_n} valeurs consécutives en {direction} "
                f"({tail[0]:.3g} → {tail[-1]:.3g}, +{pct_change:.1f}%)"
            ),
        )
