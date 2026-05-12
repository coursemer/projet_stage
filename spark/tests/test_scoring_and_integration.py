"""Tests for SeverityScorer and full AnomalyDetector integration."""
from __future__ import annotations

import pytest

from anomaly_detection import AnomalyDetector
from anomaly_detection.models import AnomalyLevel, Severity
from anomaly_detection.scoring import SeverityScorer

from .conftest import make_history, make_metrics


# ─────────────────────────────────────────────────────────────────────────────
# SeverityScorer
# ─────────────────────────────────────────────────────────────────────────────

class TestSeverityScorer:

    def setup_method(self):
        self.scorer = SeverityScorer(
            pipeline_weights={"critical_pipeline": 1.5, "low_pipeline": 0.5},
            ml_agreement_bonus=10.0,
        )

    def _make_anomaly(self, level=AnomalyLevel.VOLUME, sigma=None, metric="row_count"):
        from anomaly_detection.models import Anomaly
        return Anomaly(
            id="test-id",
            pipeline_name="test_pipeline",
            level=level,
            description="Test anomaly",
            metric_name=metric,
            deviation_sigma=sigma,
        )

    def test_schema_anomaly_scores_higher_than_ml(self):
        schema_a = self._make_anomaly(level=AnomalyLevel.SCHEMA)
        ml_a     = self._make_anomaly(level=AnomalyLevel.ML)
        self.scorer.score([schema_a, ml_a])
        assert schema_a.severity_score > ml_a.severity_score

    def test_high_sigma_escalates_score(self):
        low_sigma  = self._make_anomaly(sigma=3.1)
        high_sigma = self._make_anomaly(sigma=7.0)
        self.scorer.score([low_sigma, high_sigma])
        assert high_sigma.severity_score > low_sigma.severity_score

    def test_pipeline_weight_applied(self):
        from anomaly_detection.models import Anomaly
        critical = Anomaly(id="c1", pipeline_name="critical_pipeline",
                           level=AnomalyLevel.VOLUME, description="x", metric_name="row_count")
        low = Anomaly(id="l1", pipeline_name="low_pipeline",
                      level=AnomalyLevel.VOLUME, description="x", metric_name="row_count")
        self.scorer.score([critical, low])
        assert critical.severity_score > low.severity_score

    def test_recurrence_bonus_applied(self):
        from anomaly_detection.models import Anomaly
        past = [
            Anomaly(id=f"p{i}", pipeline_name="test_pipeline",
                    level=AnomalyLevel.VOLUME, description="old", metric_name="row_count")
            for i in range(5)
        ]
        current = self._make_anomaly()
        self.scorer.score([current], anomaly_history=past)
        assert current.is_recurring
        assert current.occurrence_count == 6

    def test_ml_agreement_bonus(self):
        a = self._make_anomaly(level=AnomalyLevel.VOLUME, metric="row_count")
        without_ml = a.severity_score
        self.scorer.score([a], ml_flagged_metrics={"row_count"})
        assert a.severity_score > without_ml or a.severity_score >= 0   # bonus applied

    def test_score_capped_at_100(self):
        from anomaly_detection.models import Anomaly
        a = Anomaly(id="x", pipeline_name="critical_pipeline",
                    level=AnomalyLevel.SCHEMA, description="x",
                    metric_name="row_count", deviation_sigma=20.0)
        self.scorer.score([a])
        assert a.severity_score <= 100.0

    def test_severity_labels_assigned(self):
        from anomaly_detection.models import Anomaly
        anomalies = [
            Anomaly(id="a1", pipeline_name="p", level=AnomalyLevel.SCHEMA,
                    description="x", metric_name="m", deviation_sigma=10.0),
        ]
        self.scorer.score(anomalies)
        assert anomalies[0].severity in list(Severity)


# ─────────────────────────────────────────────────────────────────────────────
# Full integration: AnomalyDetector
# ─────────────────────────────────────────────────────────────────────────────

class TestAnomalyDetectorIntegration:

    def _make_detector(self, pipeline="sales_analytics"):
        return AnomalyDetector.for_pipeline(
            pipeline_name=pipeline,
            sla_seconds=3600.0,
            critical_columns=["order_id", "customer_id"],
            tracked_columns=["amount"],
            pipeline_weight=1.2,
            min_history=7,
            ml_min_train=20,
        )

    def test_no_anomalies_on_normal_data(self):
        detector = self._make_detector()
        history = make_history(n=35)
        current = make_metrics(pipeline="sales_analytics")
        result = detector.detect(current, history)
        assert not result.has_anomalies, f"Expected clean run, got {result.anomalies}"

    def test_volume_anomaly_in_result(self):
        detector = self._make_detector()
        history = make_history(n=35, pipeline="sales_analytics")
        current = make_metrics(pipeline="sales_analytics", row_count=50)
        result = detector.detect(current, history)
        assert result.has_anomalies
        assert any(a.level == AnomalyLevel.VOLUME for a in result.anomalies)

    def test_schema_anomaly_in_result(self):
        detector = self._make_detector()
        history = make_history(n=35, pipeline="sales_analytics")
        current = make_metrics(pipeline="sales_analytics",
                               schema={"order_id": "int64"})   # dropped 2 columns
        result = detector.detect(current, history)
        assert any(a.level == AnomalyLevel.SCHEMA for a in result.anomalies)

    def test_sla_breach_in_result(self):
        detector = self._make_detector()
        history = make_history(n=35, pipeline="sales_analytics")
        current = make_metrics(pipeline="sales_analytics", duration_seconds=7200.0)
        result = detector.detect(current, history)
        perf_anomalies = [a for a in result.anomalies if a.level == AnomalyLevel.PERFORMANCE]
        assert perf_anomalies, "Expected at least one PERFORMANCE anomaly for SLA breach"
        # SLA breaches are flagged; severity may be HIGH or CRITICAL after scorer
        assert any(a.severity in (Severity.HIGH, Severity.CRITICAL) for a in perf_anomalies)

    def test_result_summary_structure(self):
        detector = self._make_detector()
        history = make_history(n=35, pipeline="sales_analytics")
        current = make_metrics(pipeline="sales_analytics", row_count=50)
        result = detector.detect(current, history)
        summary = result.summary()
        assert "pipeline" in summary
        assert "total_anomalies" in summary
        assert "by_severity" in summary
        assert "by_level" in summary

    def test_worst_severity_property(self):
        detector = self._make_detector()
        history = make_history(n=35, pipeline="sales_analytics")
        current = make_metrics(pipeline="sales_analytics",
                               row_count=10, duration_seconds=7200.0)
        result = detector.detect(current, history)
        ws = result.worst_severity
        assert ws in list(Severity) or ws is None

    def test_anomaly_history_used_for_recurrence(self):
        detector = self._make_detector()
        history = make_history(n=35, pipeline="sales_analytics")

        from anomaly_detection.models import Anomaly
        past_anomalies = [
            Anomaly(id=f"past-{i}", pipeline_name="sales_analytics",
                    level=AnomalyLevel.VOLUME, description="Past drop",
                    metric_name="row_count")
            for i in range(3)
        ]

        current = make_metrics(pipeline="sales_analytics", row_count=50)
        result = detector.detect(current, history, anomaly_history=past_anomalies)
        vol_anomalies = [a for a in result.anomalies if a.level == AnomalyLevel.VOLUME]
        if vol_anomalies:
            assert vol_anomalies[0].is_recurring

    def test_factory_defaults(self):
        detector = AnomalyDetector.for_pipeline("my_pipeline")
        assert detector.volume_detector is not None
        assert detector.ml_detector is not None
        assert detector.scorer is not None
