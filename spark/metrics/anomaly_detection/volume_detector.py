"""
Volume Anomaly Detector
-----------------------
Detects unexpected row-count changes compared to historical baseline.

Strategy:
  1. Compute rolling mean and std from history.
  2. Flag if current count deviates by more than `sigma_threshold` σ.
  3. Also flag hard drops (> `drop_pct` percent decrease) regardless of σ.
"""
from __future__ import annotations

import uuid
from datetime import datetime
from typing import List, Optional

import numpy as np

from .models import Anomaly, AnomalyLevel, PipelineMetrics, Severity


class VolumeDetector:
    """
    Detect volume anomalies using Z-score + hard-drop thresholds.

    Parameters
    ----------
    sigma_threshold : float
        Number of standard deviations to flag as anomalous (default 3.0).
    drop_pct_threshold : float
        Hard minimum percentage drop to always flag (default 0.50 = 50%).
    spike_pct_threshold : float
        Hard maximum percentage increase to always flag (default 5.0 = 500%).
    min_history : int
        Minimum number of historical data points required before flagging.
    """

    def __init__(
        self,
        sigma_threshold: float = 3.0,
        drop_pct_threshold: float = 0.50,
        spike_pct_threshold: float = 5.0,
        min_history: int = 7,
    ) -> None:
        self.sigma_threshold = sigma_threshold
        self.drop_pct_threshold = drop_pct_threshold
        self.spike_pct_threshold = spike_pct_threshold
        self.min_history = min_history

    # ------------------------------------------------------------------
    def detect(
        self,
        current: PipelineMetrics,
        history: List[PipelineMetrics],
    ) -> List[Anomaly]:
        """
        Parameters
        ----------
        current : PipelineMetrics   – the latest snapshot
        history : list[PipelineMetrics]  – historical snapshots (oldest first)

        Returns
        -------
        list[Anomaly] – may be empty if nothing is wrong
        """
        anomalies: List[Anomaly] = []

        if current.row_count is None:
            return anomalies  # nothing to check

        historical_counts = [
            m.row_count for m in history if m.row_count is not None
        ]

        if len(historical_counts) < self.min_history:
            # Not enough history – return an informational anomaly only if
            # the count is exactly 0 (complete data loss)
            if current.row_count == 0:
                anomalies.append(
                    self._make_anomaly(
                        pipeline=current.pipeline_name,
                        description="Row count is 0 – possible complete data loss.",
                        observed=0,
                        expected=None,
                        sigma=None,
                        severity=Severity.CRITICAL,
                    )
                )
            return anomalies

        arr = np.array(historical_counts, dtype=float)
        mean = arr.mean()
        std = arr.std(ddof=1) if len(arr) > 1 else 0.0
        current_val = float(current.row_count)

        # ── Z-score check ──────────────────────────────────────────────
        if std > 0:
            z = abs(current_val - mean) / std
            if z > self.sigma_threshold:
                direction = "spike" if current_val > mean else "drop"
                severity = self._sigma_to_severity(z)
                anomalies.append(
                    self._make_anomaly(
                        pipeline=current.pipeline_name,
                        description=(
                            f"Volume {direction}: {current_val:,.0f} rows "
                            f"({z:.1f}σ from historical mean {mean:,.0f})."
                        ),
                        observed=current_val,
                        expected=mean,
                        sigma=z,
                        severity=severity,
                    )
                )
                return anomalies  # one anomaly per run is enough

        # ── Hard-drop / spike check (when std ≈ 0 or z just missed) ──
        if mean > 0:
            ratio = current_val / mean
            if ratio < (1 - self.drop_pct_threshold):
                drop_pct = (1 - ratio) * 100
                anomalies.append(
                    self._make_anomaly(
                        pipeline=current.pipeline_name,
                        description=(
                            f"Volume hard-drop: {drop_pct:.1f}% below "
                            f"historical mean ({current_val:,.0f} vs {mean:,.0f})."
                        ),
                        observed=current_val,
                        expected=mean,
                        sigma=None,
                        severity=Severity.HIGH,
                    )
                )
            elif ratio > (1 + self.spike_pct_threshold):
                spike_pct = (ratio - 1) * 100
                anomalies.append(
                    self._make_anomaly(
                        pipeline=current.pipeline_name,
                        description=(
                            f"Volume spike: {spike_pct:.1f}% above "
                            f"historical mean ({current_val:,.0f} vs {mean:,.0f})."
                        ),
                        observed=current_val,
                        expected=mean,
                        sigma=None,
                        severity=Severity.MEDIUM,
                    )
                )

        return anomalies

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _sigma_to_severity(self, z: float) -> Severity:
        if z >= 6:
            return Severity.CRITICAL
        if z >= 4.5:
            return Severity.HIGH
        if z >= 3:
            return Severity.MEDIUM
        return Severity.LOW

    def _make_anomaly(
        self,
        pipeline: str,
        description: str,
        observed: Optional[float],
        expected: Optional[float],
        sigma: Optional[float],
        severity: Severity,
    ) -> Anomaly:
        return Anomaly(
            id=f"{pipeline}-volume-{uuid.uuid4().hex[:8]}",
            pipeline_name=pipeline,
            level=AnomalyLevel.VOLUME,
            severity=severity,
            description=description,
            metric_name="row_count",
            observed_value=observed,
            expected_value=expected,
            deviation_sigma=sigma,
            context={"detector": "VolumeDetector"},
        )
