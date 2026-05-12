"""
Performance Anomaly Detector
-----------------------------
Detects anomalies in pipeline execution metrics:
  - Duration spikes (Z-score + hard SLA threshold)
  - Failure rate increases
  - Task failure count spikes
"""
from __future__ import annotations

import uuid
from typing import List, Optional

import numpy as np

from .models import Anomaly, AnomalyLevel, PipelineMetrics, Severity


class PerformanceDetector:
    """
    Detect performance anomalies in pipeline execution metrics.

    Parameters
    ----------
    sigma_threshold : float
        Z-score above which duration is anomalous.
    sla_seconds : float | None
        Hard SLA for pipeline duration. Always flagged CRITICAL if breached.
    min_history : int
        Minimum historical data points.
    failure_rate_threshold : float
        Rolling failure rate above which an alert is raised (0.0–1.0).
    """

    def __init__(
        self,
        sigma_threshold: float = 3.0,
        sla_seconds: Optional[float] = None,
        min_history: int = 7,
        failure_rate_threshold: float = 0.20,
    ) -> None:
        self.sigma_threshold = sigma_threshold
        self.sla_seconds = sla_seconds
        self.min_history = min_history
        self.failure_rate_threshold = failure_rate_threshold

    # ------------------------------------------------------------------

    def detect(
        self,
        current: PipelineMetrics,
        history: List[PipelineMetrics],
    ) -> List[Anomaly]:
        anomalies: List[Anomaly] = []

        anomalies.extend(self._check_duration(current, history))
        anomalies.extend(self._check_failure_rate(current, history))
        anomalies.extend(self._check_task_failures(current, history))

        return anomalies

    # ------------------------------------------------------------------
    # Duration
    # ------------------------------------------------------------------

    def _check_duration(
        self,
        current: PipelineMetrics,
        history: List[PipelineMetrics],
    ) -> List[Anomaly]:
        anomalies: List[Anomaly] = []

        if current.duration_seconds is None:
            return anomalies

        dur = current.duration_seconds

        # Hard SLA breach
        if self.sla_seconds and dur > self.sla_seconds:
            anomalies.append(
                self._make_anomaly(
                    pipeline=current.pipeline_name,
                    description=(
                        f"SLA BREACH: pipeline took {dur:.0f}s "
                        f"(SLA: {self.sla_seconds:.0f}s)."
                    ),
                    metric_name="duration_seconds",
                    observed=dur,
                    expected=self.sla_seconds,
                    sigma=None,
                    severity=Severity.CRITICAL,
                )
            )
            return anomalies   # Don't double-report

        historical_durations = [
            m.duration_seconds
            for m in history
            if m.duration_seconds is not None
        ]

        if len(historical_durations) < self.min_history:
            return anomalies

        arr = np.array(historical_durations, dtype=float)
        mean = arr.mean()
        std = arr.std(ddof=1) if len(arr) > 1 else 0.0

        if std == 0:
            return anomalies

        z = (dur - mean) / std

        if z > self.sigma_threshold:          # Only spike, not improvement
            severity = self._sigma_to_severity(z)
            anomalies.append(
                self._make_anomaly(
                    pipeline=current.pipeline_name,
                    description=(
                        f"Duration spike: {dur:.0f}s vs historical mean "
                        f"{mean:.0f}s ({z:.1f}σ)."
                    ),
                    metric_name="duration_seconds",
                    observed=dur,
                    expected=mean,
                    sigma=z,
                    severity=severity,
                )
            )

        return anomalies

    # ------------------------------------------------------------------
    # Failure rate
    # ------------------------------------------------------------------

    def _check_failure_rate(
        self,
        current: PipelineMetrics,
        history: List[PipelineMetrics],
    ) -> List[Anomaly]:
        anomalies: List[Anomaly] = []

        recent_window = history[-20:]   # last 20 runs
        all_runs = recent_window + [current]
        success_flags = [m.success for m in all_runs if m.success is not None]

        if len(success_flags) < 5:
            return anomalies

        failure_rate = 1.0 - (sum(success_flags) / len(success_flags))

        if failure_rate > self.failure_rate_threshold:
            severity = (
                Severity.CRITICAL if failure_rate > 0.50
                else Severity.HIGH if failure_rate > 0.30
                else Severity.MEDIUM
            )
            anomalies.append(
                self._make_anomaly(
                    pipeline=current.pipeline_name,
                    description=(
                        f"High failure rate: {failure_rate:.1%} of the last "
                        f"{len(success_flags)} runs failed."
                    ),
                    metric_name="failure_rate",
                    observed=failure_rate,
                    expected=self.failure_rate_threshold,
                    sigma=None,
                    severity=severity,
                )
            )

        return anomalies

    # ------------------------------------------------------------------
    # Task failure count
    # ------------------------------------------------------------------

    def _check_task_failures(
        self,
        current: PipelineMetrics,
        history: List[PipelineMetrics],
    ) -> List[Anomaly]:
        anomalies: List[Anomaly] = []

        if current.task_failures is None or current.task_failures == 0:
            return anomalies

        historical_failures = [
            m.task_failures for m in history if m.task_failures is not None
        ]

        if len(historical_failures) < self.min_history:
            if current.task_failures > 0:
                anomalies.append(
                    self._make_anomaly(
                        pipeline=current.pipeline_name,
                        description=f"{current.task_failures} task failure(s) detected.",
                        metric_name="task_failures",
                        observed=float(current.task_failures),
                        expected=0.0,
                        sigma=None,
                        severity=Severity.MEDIUM,
                    )
                )
            return anomalies

        arr = np.array(historical_failures, dtype=float)
        mean = arr.mean()
        std = arr.std(ddof=1) if len(arr) > 1 else 0.0
        observed = float(current.task_failures)

        if std > 0:
            z = (observed - mean) / std
            if z > self.sigma_threshold:
                anomalies.append(
                    self._make_anomaly(
                        pipeline=current.pipeline_name,
                        description=(
                            f"Task failure spike: {current.task_failures} failures "
                            f"vs historical mean {mean:.1f} ({z:.1f}σ)."
                        ),
                        metric_name="task_failures",
                        observed=observed,
                        expected=mean,
                        sigma=z,
                        severity=self._sigma_to_severity(z),
                    )
                )
        elif observed > mean:
            anomalies.append(
                self._make_anomaly(
                    pipeline=current.pipeline_name,
                    description=(
                        f"Task failures above historical constant "
                        f"({current.task_failures} vs {mean:.0f})."
                    ),
                    metric_name="task_failures",
                    observed=observed,
                    expected=mean,
                    sigma=None,
                    severity=Severity.MEDIUM,
                )
            )

        return anomalies

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
        metric_name: str,
        observed: Optional[float],
        expected: Optional[float],
        sigma: Optional[float],
        severity: Severity,
    ) -> Anomaly:
        return Anomaly(
            id=f"{pipeline}-perf-{uuid.uuid4().hex[:8]}",
            pipeline_name=pipeline,
            level=AnomalyLevel.PERFORMANCE,
            severity=severity,
            description=description,
            metric_name=metric_name,
            observed_value=observed,
            expected_value=expected,
            deviation_sigma=sigma,
            context={"detector": "PerformanceDetector"},
        )
