"""
AnomalyDetector – Central Orchestrator
---------------------------------------
Single entry point that runs all detection layers in sequence:

  1. VolumeDetector
  2. DistributionDetector
  3. SchemaDetector
  4. PerformanceDetector
  5. SeasonalityDetector
  6. TrendDetector
  7. MLBaselineDetector

Then scores all anomalies via SeverityScorer.

Usage
-----
    from anomaly_detection import AnomalyDetector
    from anomaly_detection.models import PipelineMetrics

    detector = AnomalyDetector.for_pipeline(
        pipeline_name="sales_analytics",
        sla_seconds=3600,
        critical_columns=["order_id", "customer_id"],
        tracked_columns=["amount", "quantity"],
        pipeline_weight=1.5,
    )

    result = detector.detect(current_metrics, history_metrics)
    print(result.summary())
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, List, Optional

from .detectors import (
    DistributionDetector,
    PerformanceDetector,
    SchemaDetector,
    VolumeDetector,
)
from .ml import MLBaselineDetector
from .models import Anomaly, AnomalyLevel, DetectionResult, PipelineMetrics
from .scoring import SeverityScorer
from .temporal import CorrelationDetector, SeasonalityDetector, TrendDetector

logger = logging.getLogger(__name__)


class AnomalyDetector:
    """
    Orchestrates all detectors for a single pipeline.

    Parameters
    ----------
    volume_detector         : VolumeDetector instance
    distribution_detector   : DistributionDetector instance
    schema_detector         : SchemaDetector instance
    performance_detector    : PerformanceDetector instance
    seasonality_detector    : SeasonalityDetector instance
    trend_detector          : TrendDetector instance
    ml_detector             : MLBaselineDetector instance
    scorer                  : SeverityScorer instance
    """

    def __init__(
        self,
        volume_detector: VolumeDetector,
        distribution_detector: DistributionDetector,
        schema_detector: SchemaDetector,
        performance_detector: PerformanceDetector,
        seasonality_detector: SeasonalityDetector,
        trend_detector: TrendDetector,
        ml_detector: MLBaselineDetector,
        scorer: SeverityScorer,
    ) -> None:
        self.volume_detector = volume_detector
        self.distribution_detector = distribution_detector
        self.schema_detector = schema_detector
        self.performance_detector = performance_detector
        self.seasonality_detector = seasonality_detector
        self.trend_detector = trend_detector
        self.ml_detector = ml_detector
        self.scorer = scorer

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def for_pipeline(
        cls,
        pipeline_name: str,
        *,
        # Volume
        volume_sigma: float = 3.0,
        volume_drop_pct: float = 0.50,
        # Performance
        sla_seconds: Optional[float] = None,
        failure_rate_threshold: float = 0.20,
        # Schema
        reference_schema: Optional[Dict[str, str]] = None,
        critical_columns: Optional[List[str]] = None,
        # Distribution / ML
        tracked_columns: Optional[List[str]] = None,
        # Temporal
        trend_window: int = 14,
        seasonality_min_per_day: int = 3,
        # Scoring
        pipeline_weight: float = 1.0,
        # Misc
        min_history: int = 7,
        ml_min_train: int = 30,
    ) -> "AnomalyDetector":
        """Convenience factory for common configurations."""
        return cls(
            volume_detector=VolumeDetector(
                sigma_threshold=volume_sigma,
                drop_pct_threshold=volume_drop_pct,
                min_history=min_history,
            ),
            distribution_detector=DistributionDetector(
                sigma_threshold=3.0,
                min_history=min_history,
                columns_to_check=tracked_columns,
            ),
            schema_detector=SchemaDetector(
                reference_schema=reference_schema,
                critical_columns=critical_columns,
            ),
            performance_detector=PerformanceDetector(
                sla_seconds=sla_seconds,
                failure_rate_threshold=failure_rate_threshold,
                min_history=min_history,
            ),
            seasonality_detector=SeasonalityDetector(
                min_per_day=seasonality_min_per_day,
            ),
            trend_detector=TrendDetector(
                window=trend_window,
                min_points=min_history,
            ),
            ml_detector=MLBaselineDetector(
                min_train_samples=ml_min_train,
                tracked_columns=tracked_columns or [],
            ),
            scorer=SeverityScorer(
                pipeline_weights={pipeline_name: pipeline_weight},
            ),
        )

    # ------------------------------------------------------------------
    # Main detection method
    # ------------------------------------------------------------------

    def detect(
        self,
        current: PipelineMetrics,
        history: List[PipelineMetrics],
        anomaly_history: Optional[List[Anomaly]] = None,
    ) -> DetectionResult:
        """
        Run all detectors and return a DetectionResult.

        Parameters
        ----------
        current         : Latest metric snapshot
        history         : Historical snapshots (oldest first)
        anomaly_history : Past anomalies for recurrence scoring
        """
        all_anomalies: List[Anomaly] = []

        # ── Rule-based layers ─────────────────────────────────────────
        for detector_name, detector in [
            ("volume",       self.volume_detector),
            ("distribution", self.distribution_detector),
            ("schema",       self.schema_detector),
            ("performance",  self.performance_detector),
            ("seasonality",  self.seasonality_detector),
            ("trend",        self.trend_detector),
        ]:
            try:
                found = detector.detect(current, history)
                logger.debug(
                    "[%s] %s: %d anomalies", current.pipeline_name,
                    detector_name, len(found)
                )
                all_anomalies.extend(found)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "[%s] %s detector failed: %s",
                    current.pipeline_name, detector_name, exc
                )

        # ── ML layer ──────────────────────────────────────────────────
        try:
            ml_anomalies = self.ml_detector.detect(current, history)
            all_anomalies.extend(ml_anomalies)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "[%s] ML detector failed: %s", current.pipeline_name, exc
            )
            ml_anomalies = []

        # Collect ML-flagged metrics for scoring bonus
        ml_flagged_metrics = {
            a.context.get("metric_name", a.metric_name) for a in ml_anomalies
        }

        # ── Score all anomalies ───────────────────────────────────────
        scored = self.scorer.score(
            anomalies=all_anomalies,
            anomaly_history=anomaly_history,
            ml_flagged_metrics=ml_flagged_metrics,
        )

        return DetectionResult(
            pipeline_name=current.pipeline_name,
            run_at=datetime.utcnow(),
            anomalies=scored,
        )
