"""Tests for DistributionDetector, SchemaDetector, PerformanceDetector."""
import pytest

from anomaly_detection.detectors import (
    DistributionDetector,
    PerformanceDetector,
    SchemaDetector,
)
from anomaly_detection.models import AnomalyLevel, Severity

from .conftest import make_history, make_metrics


# ─────────────────────────────────────────────────────────────────────────────
# Distribution
# ─────────────────────────────────────────────────────────────────────────────

class TestDistributionDetector:

    def setup_method(self):
        self.detector = DistributionDetector(sigma_threshold=3.0)

    def test_no_anomaly_normal_distribution(self, normal_history, current_normal):
        anomalies = self.detector.detect(current_normal, normal_history)
        assert anomalies == []

    def test_null_rate_hard_threshold(self, normal_history, current_null_rate_spike):
        anomalies = self.detector.detect(current_null_rate_spike, normal_history)
        assert any(
            "null_rate" in a.metric_name and a.severity == Severity.HIGH
            for a in anomalies
        ), f"Expected high-severity null_rate anomaly, got {anomalies}"

    def test_mean_drift_detected(self, normal_history, current_mean_drift):
        anomalies = self.detector.detect(current_mean_drift, normal_history)
        assert any(
            "amount.mean" in a.metric_name for a in anomalies
        ), f"Expected mean drift anomaly, got {anomalies}"

    def test_no_anomaly_when_no_column_stats(self, normal_history):
        current = make_metrics()
        current.column_stats = {}
        anomalies = self.detector.detect(current, normal_history)
        assert anomalies == []

    def test_columns_to_check_filter(self, normal_history, current_mean_drift):
        # Only check 'nonexistent_col' → should find nothing
        detector = DistributionDetector(columns_to_check=["nonexistent_col"])
        anomalies = detector.detect(current_mean_drift, normal_history)
        assert anomalies == []

    def test_insufficient_history_per_stat(self):
        history = make_history(n=3)
        current = make_metrics(col_mean=500.0)
        anomalies = self.detector.detect(current, history)
        # Not enough history, should not flag stat-based (but may flag hard threshold)
        stat_anomalies = [a for a in anomalies if "mean" in a.metric_name]
        assert stat_anomalies == []


# ─────────────────────────────────────────────────────────────────────────────
# Schema
# ─────────────────────────────────────────────────────────────────────────────

class TestSchemaDetector:

    def setup_method(self):
        self.reference = {
            "order_id": "int64",
            "amount": "float64",
            "customer_id": "int64",
        }
        self.detector = SchemaDetector(
            reference_schema=self.reference,
            critical_columns=["customer_id"],
        )

    def test_no_anomaly_matching_schema(self, normal_history, current_normal):
        anomalies = self.detector.detect(current_normal, normal_history)
        assert anomalies == []

    def test_dropped_column_detected(self, normal_history, schema_dropped_column):
        anomalies = self.detector.detect(schema_dropped_column, normal_history)
        dropped = [a for a in anomalies if "DROPPED" in a.description]
        assert len(dropped) >= 1

    def test_dropped_critical_column_is_critical(self, normal_history):
        current = make_metrics(schema={"order_id": "int64", "amount": "float64"})
        anomalies = self.detector.detect(current, normal_history)
        critical = [a for a in anomalies if a.severity == Severity.CRITICAL]
        assert critical, "Dropped critical column should be CRITICAL"

    def test_added_column_is_low_severity(self, normal_history):
        current = make_metrics(schema={
            **self.reference,
            "new_col": "object",
        })
        anomalies = self.detector.detect(current, normal_history)
        added = [a for a in anomalies if "New unexpected" in a.description]
        assert added
        assert all(a.severity == Severity.LOW for a in added)

    def test_type_change_detected(self, normal_history, schema_type_change):
        anomalies = self.detector.detect(schema_type_change, normal_history)
        type_changes = [a for a in anomalies if "type changed" in a.description]
        assert type_changes

    def test_no_reference_uses_first_history(self, normal_history, schema_dropped_column):
        detector = SchemaDetector()   # No reference_schema
        anomalies = detector.detect(schema_dropped_column, normal_history)
        dropped = [a for a in anomalies if "DROPPED" in a.description]
        assert dropped

    def test_empty_history_and_no_reference_returns_empty(self, schema_dropped_column):
        detector = SchemaDetector()
        anomalies = detector.detect(schema_dropped_column, [])
        assert anomalies == []


# ─────────────────────────────────────────────────────────────────────────────
# Performance
# ─────────────────────────────────────────────────────────────────────────────

class TestPerformanceDetector:

    def setup_method(self):
        self.detector = PerformanceDetector(sla_seconds=1800.0)

    def test_no_anomaly_normal_duration(self, normal_history, current_normal):
        anomalies = self.detector.detect(current_normal, normal_history)
        assert anomalies == []

    def test_sla_breach_is_critical(self, normal_history, current_duration_spike):
        anomalies = self.detector.detect(current_duration_spike, normal_history)
        sla_breach = [a for a in anomalies if "SLA" in a.description]
        assert sla_breach
        assert sla_breach[0].severity == Severity.CRITICAL

    def test_duration_spike_without_sla(self, normal_history, current_duration_spike):
        detector = PerformanceDetector()   # No SLA
        anomalies = detector.detect(current_duration_spike, normal_history)
        perf = [a for a in anomalies if a.level == AnomalyLevel.PERFORMANCE]
        assert perf

    def test_high_failure_rate_flagged(self):
        from .conftest import make_metrics
        history = [make_metrics(success=(i % 3 != 0)) for i in range(20)]
        current = make_metrics(success=False)
        anomalies = self.detector.detect(current, history)
        failure = [a for a in anomalies if "failure rate" in a.description]
        assert failure

    def test_task_failure_spike(self, normal_history):
        current = make_metrics(task_failures=10)
        anomalies = self.detector.detect(current, normal_history)
        task = [a for a in anomalies if "task" in a.metric_name.lower() or "task" in a.description.lower()]
        assert task
