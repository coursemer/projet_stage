"""
Temporal Pattern Detector
--------------------------
Detects time-aware anomalies that point-in-time detectors miss:

  1. Seasonality  – weekday vs. weekend baselines per metric
  2. Trend        – progressive degradation / improvement over time
  3. Correlation  – co-occurring anomalies across pipelines
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
from scipy import stats as scipy_stats

from .models import Anomaly, AnomalyLevel, PipelineMetrics, Severity, TrendDirection


# ─────────────────────────────────────────────────────────────────────────────
# Seasonality Detector
# ─────────────────────────────────────────────────────────────────────────────

class SeasonalityDetector:
    """
    Build per-day-of-week baselines and flag deviations.

    For each metric (row_count, duration_seconds) it computes:
      - Mean and std for each weekday (0=Mon … 6=Sun) from history.
      - Flags the current value if it deviates > sigma_threshold σ
        from the *same-weekday* baseline.

    This avoids false positives like "low volume on Sunday".
    """

    def __init__(self, sigma_threshold: float = 3.0, min_per_day: int = 3) -> None:
        self.sigma_threshold = sigma_threshold
        self.min_per_day = min_per_day

    def detect(
        self,
        current: PipelineMetrics,
        history: List[PipelineMetrics],
    ) -> List[Anomaly]:
        anomalies: List[Anomaly] = []
        today_dow = current.measured_at.weekday()   # 0=Mon, 6=Sun

        for metric in ("row_count", "duration_seconds"):
            current_val = getattr(current, metric, None)
            if current_val is None:
                continue

            # Collect same-day-of-week values from history
            same_day_vals = [
                getattr(snap, metric)
                for snap in history
                if snap.measured_at.weekday() == today_dow
                and getattr(snap, metric) is not None
            ]

            if len(same_day_vals) < self.min_per_day:
                continue

            arr = np.array(same_day_vals, dtype=float)
            mean = arr.mean()
            std = arr.std(ddof=1) if len(arr) > 1 else 0.0

            if std == 0:
                continue

            z = abs(float(current_val) - mean) / std
            if z > self.sigma_threshold:
                day_name = current.measured_at.strftime("%A")
                severity = Severity.HIGH if z >= 4.5 else Severity.MEDIUM
                anomalies.append(Anomaly(
                    id=f"{current.pipeline_name}-seasonal-{metric}-{uuid.uuid4().hex[:8]}",
                    pipeline_name=current.pipeline_name,
                    level=AnomalyLevel.TEMPORAL,
                    severity=severity,
                    description=(
                        f"Seasonality anomaly ({day_name}): {metric} is "
                        f"{float(current_val):.4g} vs {day_name} baseline "
                        f"{mean:.4g} ({z:.1f}σ)."
                    ),
                    metric_name=metric,
                    observed_value=float(current_val),
                    expected_value=mean,
                    deviation_sigma=z,
                    context={
                        "detector": "SeasonalityDetector",
                        "day_of_week": day_name,
                        "same_day_samples": len(same_day_vals),
                    },
                ))

        return anomalies


# ─────────────────────────────────────────────────────────────────────────────
# Trend Detector
# ─────────────────────────────────────────────────────────────────────────────

class TrendDetector:
    """
    Detect progressive degradation / improvement using linear regression
    over a rolling window.

    Metrics checked: row_count (degradation = drop), duration_seconds (degradation = rise).
    """

    METRIC_DEGRADATION_DIRECTION: Dict[str, str] = {
        "row_count":        "negative",   # degrading if slope < 0
        "duration_seconds": "positive",   # degrading if slope > 0
        "failure_rate":     "positive",
    }

    def __init__(
        self,
        window: int = 14,              # days to look back
        p_value_threshold: float = 0.05,
        min_points: int = 7,
        relative_change_threshold: float = 0.20,   # 20% total change over window
    ) -> None:
        self.window = window
        self.p_value_threshold = p_value_threshold
        self.min_points = min_points
        self.relative_change_threshold = relative_change_threshold

    def detect(
        self,
        current: PipelineMetrics,
        history: List[PipelineMetrics],
    ) -> List[Anomaly]:
        anomalies: List[Anomaly] = []

        recent = history[-self.window:] + [current]

        for metric, bad_direction in self.METRIC_DEGRADATION_DIRECTION.items():
            values_with_time: List[Tuple[float, float]] = []
            for snap in recent:
                val = getattr(snap, metric, None)
                if val is None and metric == "failure_rate":
                    # Derive failure_rate from success field
                    val = 0.0 if snap.success else 1.0
                if val is not None:
                    t = snap.measured_at.timestamp()
                    values_with_time.append((t, float(val)))

            if len(values_with_time) < self.min_points:
                continue

            times, vals = zip(*values_with_time)
            times_arr = np.array(times, dtype=float)
            vals_arr  = np.array(vals,  dtype=float)

            # Normalise time to [0, 1] for numerical stability
            t_norm = (times_arr - times_arr[0]) / (times_arr[-1] - times_arr[0] + 1e-9)

            slope, intercept, r_value, p_value, _ = scipy_stats.linregress(
                t_norm, vals_arr
            )

            # Only flag statistically significant trends
            if p_value > self.p_value_threshold:
                continue

            # Only flag if the total predicted change is large enough
            total_change = abs(slope * 1.0)   # over normalised range [0,1]
            base = abs(intercept) if abs(intercept) > 1e-9 else 1.0
            relative_change = total_change / base
            if relative_change < self.relative_change_threshold:
                continue

            is_degrading = (
                (bad_direction == "negative" and slope < 0) or
                (bad_direction == "positive" and slope > 0)
            )

            if not is_degrading:
                continue

            direction = TrendDirection.DEGRADING
            severity = Severity.HIGH if relative_change > 0.50 else Severity.MEDIUM

            anomalies.append(Anomaly(
                id=f"{current.pipeline_name}-trend-{metric}-{uuid.uuid4().hex[:8]}",
                pipeline_name=current.pipeline_name,
                level=AnomalyLevel.TEMPORAL,
                severity=severity,
                description=(
                    f"Degrading trend detected in '{metric}' over the last "
                    f"{len(values_with_time)} data points "
                    f"(slope={slope:.4g}, p={p_value:.3f}, "
                    f"Δ≈{relative_change:.0%})."
                ),
                metric_name=metric,
                observed_value=float(vals_arr[-1]),
                expected_value=float(vals_arr[0]),
                context={
                    "detector": "TrendDetector",
                    "slope": slope,
                    "r_squared": r_value ** 2,
                    "p_value": p_value,
                    "direction": direction,
                    "relative_change": relative_change,
                    "window_size": len(values_with_time),
                },
            ))

        return anomalies


# ─────────────────────────────────────────────────────────────────────────────
# Cross-pipeline Correlation Detector
# ─────────────────────────────────────────────────────────────────────────────

class CorrelationDetector:
    """
    Detect co-occurring metric drops/spikes across multiple pipelines
    that suggest a shared upstream cause.

    Usage: call detect() once with the most recent snapshot for EACH pipeline.
    """

    def __init__(
        self,
        window: int = 10,              # number of recent points per pipeline
        correlation_threshold: float = 0.80,
        min_points: int = 5,
    ) -> None:
        self.window = window
        self.correlation_threshold = correlation_threshold
        self.min_points = min_points

    def detect_cross_pipeline(
        self,
        pipeline_histories: Dict[str, List[PipelineMetrics]],
        metric: str = "row_count",
    ) -> List[Anomaly]:
        """
        Parameters
        ----------
        pipeline_histories : dict pipeline_name → list of PipelineMetrics
        metric : metric to correlate (default 'row_count')
        """
        anomalies: List[Anomaly] = []

        # Build series per pipeline
        series: Dict[str, np.ndarray] = {}
        for pipeline, history in pipeline_histories.items():
            vals = [
                getattr(snap, metric)
                for snap in history[-self.window:]
                if getattr(snap, metric) is not None
            ]
            if len(vals) >= self.min_points:
                series[pipeline] = np.array(vals, dtype=float)

        pipeline_names = list(series.keys())
        n = len(pipeline_names)

        if n < 2:
            return anomalies

        reported_pairs = set()

        for i in range(n):
            for j in range(i + 1, n):
                p1, p2 = pipeline_names[i], pipeline_names[j]
                s1, s2 = series[p1], series[p2]

                min_len = min(len(s1), len(s2))
                if min_len < self.min_points:
                    continue

                s1, s2 = s1[-min_len:], s2[-min_len:]

                # Compute Pearson correlation on first-differences (removes trend bias)
                d1 = np.diff(s1)
                d2 = np.diff(s2)

                if d1.std() == 0 or d2.std() == 0:
                    continue

                corr, p_val = scipy_stats.pearsonr(d1, d2)

                pair_key = frozenset([p1, p2])
                if corr >= self.correlation_threshold and pair_key not in reported_pairs:
                    reported_pairs.add(pair_key)
                    # Check if both recently dropped
                    both_dropped = d1[-1] < 0 and d2[-1] < 0
                    if both_dropped:
                        severity = Severity.HIGH
                        description = (
                            f"Co-occurring drop in '{metric}' across "
                            f"'{p1}' and '{p2}' (correlation={corr:.2f}). "
                            f"Likely shared upstream failure."
                        )
                    else:
                        severity = Severity.MEDIUM
                        description = (
                            f"High correlation in '{metric}' changes between "
                            f"'{p1}' and '{p2}' (corr={corr:.2f}). "
                            f"Monitor for shared upstream issues."
                        )

                    anomalies.append(Anomaly(
                        id=f"cross-{p1}-{p2}-{metric}-{uuid.uuid4().hex[:8]}",
                        pipeline_name=p1,   # attribute to first pipeline
                        level=AnomalyLevel.TEMPORAL,
                        severity=severity,
                        description=description,
                        metric_name=metric,
                        context={
                            "detector": "CorrelationDetector",
                            "pipeline_pair": [p1, p2],
                            "pearson_correlation": corr,
                            "p_value": p_val,
                            "metric": metric,
                        },
                    ))

        return anomalies
