"""Tests for VolumeDetector."""
import pytest

from anomaly_detection.detectors import VolumeDetector
from anomaly_detection.models import AnomalyLevel, Severity

from .conftest import make_history, make_metrics


class TestVolumeDetector:

    def setup_method(self):
        self.detector = VolumeDetector(sigma_threshold=3.0, drop_pct_threshold=0.50)

    # ── Normal cases ──────────────────────────────────────────────────

    def test_no_anomaly_on_normal_data(self, normal_history, current_normal):
        anomalies = self.detector.detect(current_normal, normal_history)
        assert anomalies == [], f"Expected no anomalies, got {anomalies}"

    def test_insufficient_history_returns_empty(self):
        history = make_history(n=5)  # below min_history=7
        current = make_metrics(row_count=10_000)
        anomalies = self.detector.detect(current, history)
        assert anomalies == []

    def test_zero_row_count_with_no_history_is_critical(self):
        history = make_history(n=3)
        current = make_metrics(row_count=0)
        anomalies = self.detector.detect(current, history)
        assert len(anomalies) == 1
        assert anomalies[0].severity == Severity.CRITICAL

    # ── Volume drops ──────────────────────────────────────────────────

    def test_large_volume_drop_detected(self, normal_history, current_volume_drop):
        anomalies = self.detector.detect(current_volume_drop, normal_history)
        assert len(anomalies) == 1
        a = anomalies[0]
        assert a.level == AnomalyLevel.VOLUME
        assert a.observed_value == 500.0
        assert a.severity in (Severity.HIGH, Severity.CRITICAL)

    def test_moderate_drop_not_flagged_below_threshold(self, normal_history):
        # 10% drop should not trigger
        current = make_metrics(row_count=9_000)
        anomalies = self.detector.detect(current, normal_history)
        assert anomalies == []

    # ── Volume spikes ─────────────────────────────────────────────────

    def test_large_volume_spike_detected(self, normal_history, current_volume_spike):
        anomalies = self.detector.detect(current_volume_spike, normal_history)
        assert len(anomalies) >= 1
        levels = {a.level for a in anomalies}
        assert AnomalyLevel.VOLUME in levels

    # ── Anomaly fields ────────────────────────────────────────────────

    def test_anomaly_has_required_fields(self, normal_history, current_volume_drop):
        anomalies = self.detector.detect(current_volume_drop, normal_history)
        assert anomalies
        a = anomalies[0]
        assert a.id
        assert a.pipeline_name == "test_pipeline"
        assert a.description
        assert a.metric_name == "row_count"
        assert a.observed_value is not None
        assert a.expected_value is not None

    def test_none_row_count_returns_empty(self, normal_history):
        current = make_metrics(row_count=None)
        current.row_count = None
        anomalies = self.detector.detect(current, normal_history)
        assert anomalies == []
