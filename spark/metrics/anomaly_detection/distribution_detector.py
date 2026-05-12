"""
Distribution Anomaly Detector
------------------------------
Detects shifts in column-level statistics: mean, std, null_rate, cardinality.

Strategy per column:
  - Compare current stat to rolling historical baseline using Z-score.
  - Flag null_rate increases above an absolute threshold too.
"""
from __future__ import annotations

import uuid
from typing import Dict, List, Optional, Tuple

import numpy as np

from .models import Anomaly, AnomalyLevel, PipelineMetrics, Severity


# Stats we track and their "higher = worse" flag for severity direction
TRACKED_STATS: List[Tuple[str, bool]] = [
    ("mean",        False),   # direction-agnostic
    ("std",         False),
    ("null_rate",   True),    # higher null_rate is always bad
    ("min",         False),
    ("max",         False),
]

# Absolute null_rate threshold – always flag if exceeded regardless of history
NULL_RATE_HARD_THRESHOLD = 0.30   # 30 %


class DistributionDetector:
    """
    Detect column distribution drift using per-stat Z-scores.

    Parameters
    ----------
    sigma_threshold : float
        Z-score threshold to flag a stat as anomalous.
    min_history : int
        Minimum historical points before enabling detection.
    columns_to_check : list[str] | None
        If provided, restrict detection to these columns only.
    """

    def __init__(
        self,
        sigma_threshold: float = 3.0,
        min_history: int = 7,
        columns_to_check: Optional[List[str]] = None,
    ) -> None:
        self.sigma_threshold = sigma_threshold
        self.min_history = min_history
        self.columns_to_check = columns_to_check

    # ------------------------------------------------------------------

    def detect(
        self,
        current: PipelineMetrics,
        history: List[PipelineMetrics],
    ) -> List[Anomaly]:
        anomalies: List[Anomaly] = []

        if not current.column_stats:
            return anomalies

        columns = (
            self.columns_to_check
            if self.columns_to_check
            else list(current.column_stats.keys())
        )

        for col in columns:
            if col not in current.column_stats:
                continue

            current_col_stats = current.column_stats[col]

            # Build per-stat history arrays
            historical: Dict[str, List[float]] = {s: [] for s, _ in TRACKED_STATS}
            for snap in history:
                col_hist = snap.column_stats.get(col, {})
                for stat, _ in TRACKED_STATS:
                    if stat in col_hist:
                        historical[stat].append(col_hist[stat])

            for stat, higher_is_worse in TRACKED_STATS:
                if stat not in current_col_stats:
                    continue

                observed = current_col_stats[stat]
                hist_vals = historical[stat]

                # ── Hard threshold for null_rate ───────────────────────
                if stat == "null_rate" and observed > NULL_RATE_HARD_THRESHOLD:
                    anomalies.append(
                        self._make_anomaly(
                            pipeline=current.pipeline_name,
                            col=col,
                            stat=stat,
                            observed=observed,
                            expected=None,
                            sigma=None,
                            severity=Severity.HIGH,
                            description=(
                                f"Column '{col}' null_rate is {observed:.1%} "
                                f"(above hard threshold of {NULL_RATE_HARD_THRESHOLD:.0%})."
                            ),
                        )
                    )
                    continue

                if len(hist_vals) < self.min_history:
                    continue

                arr = np.array(hist_vals, dtype=float)
                mean = arr.mean()
                std = arr.std(ddof=1) if len(arr) > 1 else 0.0

                if std == 0:
                    # Flag only if value is completely different
                    if abs(observed - mean) > 1e-9:
                        anomalies.append(
                            self._make_anomaly(
                                pipeline=current.pipeline_name,
                                col=col,
                                stat=stat,
                                observed=observed,
                                expected=mean,
                                sigma=None,
                                severity=Severity.MEDIUM,
                                description=(
                                    f"Column '{col}' {stat} changed from constant "
                                    f"{mean:.4g} to {observed:.4g}."
                                ),
                            )
                        )
                    continue

                z = (observed - mean) / std
                abs_z = abs(z)

                if abs_z > self.sigma_threshold:
                    severity = self._sigma_to_severity(abs_z, higher_is_worse, z > 0)
                    direction = "increase" if z > 0 else "decrease"
                    anomalies.append(
                        self._make_anomaly(
                            pipeline=current.pipeline_name,
                            col=col,
                            stat=stat,
                            observed=observed,
                            expected=mean,
                            sigma=abs_z,
                            severity=severity,
                            description=(
                                f"Column '{col}' {stat} {direction}: "
                                f"{observed:.4g} vs historical mean {mean:.4g} "
                                f"({abs_z:.1f}σ)."
                            ),
                        )
                    )

        return anomalies

    # ------------------------------------------------------------------

    def _sigma_to_severity(
        self, z: float, higher_is_worse: bool, is_increase: bool
    ) -> Severity:
        """Map Z-score (and direction) to severity."""
        if z >= 6:
            return Severity.CRITICAL
        if z >= 4.5:
            return Severity.HIGH if (higher_is_worse and is_increase) else Severity.MEDIUM
        if z >= 3:
            return Severity.MEDIUM
        return Severity.LOW

    def _make_anomaly(
        self,
        pipeline: str,
        col: str,
        stat: str,
        observed: Optional[float],
        expected: Optional[float],
        sigma: Optional[float],
        severity: Severity,
        description: str,
    ) -> Anomaly:
        return Anomaly(
            id=f"{pipeline}-dist-{col}-{stat}-{uuid.uuid4().hex[:8]}",
            pipeline_name=pipeline,
            level=AnomalyLevel.DISTRIBUTION,
            severity=severity,
            description=description,
            metric_name=f"{col}.{stat}",
            observed_value=observed,
            expected_value=expected,
            deviation_sigma=sigma,
            context={"detector": "DistributionDetector", "column": col, "stat": stat},
        )
