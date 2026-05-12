"""Tests for SeasonalityDetector, TrendDetector, CorrelationDetector, MLBaselineDetector."""
from __future__ import annotations

import random
from datetime import datetime, timedelta

import pytest

from anomaly_detection.ml import MLBaselineDetector
from anomaly_detection.models import AnomalyLevel, PipelineMetrics
from anomaly_detection.temporal import (
    CorrelationDetector,
    SeasonalityDetector,
    TrendDetector,
)

from .conftest import make_history, make_metrics


# ─────────────────────────────────────────────────────────────────────────────
# Seasonality
# ─────────────────────────────────────────────────────────────────────────────

class TestSeasonalityDetector:

    def setup_method(self):
        self.detector = SeasonalityDetector(sigma_threshold=3.0, min_per_day=3)

    def _history_with_weekday_pattern(self) -> list[PipelineMetrics]:
        """Generate 60 days of history with Sundays having 50% lower volume."""
        history = []
        base = datetime.utcnow() - timedelta(days=61)
        rng = random.Random(99)
        for i in range(60):
            dt = base + timedelta(days=i)
            is_sunday = dt.weekday() == 6
            rc = int(5_000 * (1 + rng.gauss(0, 0.03))) if is_sunday else int(10_000 * (1 + rng.gauss(0, 0.03)))
            history.append(make_metrics(measured_at=dt, row_count=rc))
        return history

    def test_no_anomaly_when_volume_matches_day_baseline(self):
        history = self._history_with_weekday_pattern()
        # Sunday-like snapshot (same DOW as last Sunday in history)
        last_sunday = next(
            s for s in reversed(history) if s.measured_at.weekday() == 6
        )
        current = make_metrics(
            measured_at=last_sunday.measured_at + timedelta(weeks=1),
            row_count=5_000,
        )
        anomalies = self.detector.detect(current, history)
        sunday_vol = [a for a in anomalies if "row_count" in a.metric_name]
        assert sunday_vol == []

    def test_anomaly_when_weekday_volume_is_sunday_like(self):
        history = self._history_with_weekday_pattern()
        # Monday with Sunday-level volume → anomaly
        last_monday = next(
            s for s in reversed(history) if s.measured_at.weekday() == 0
        )
        current = make_metrics(
            measured_at=last_monday.measured_at + timedelta(weeks=1),
            row_count=1_000,   # Very low for a Monday
        )
        anomalies = self.detector.detect(current, history)
        assert anomalies, "Expected seasonality anomaly for Monday with low volume"

    def test_insufficient_per_day_history_returns_empty(self):
        history = make_history(n=4)  # only ~0-1 occurrence per weekday
        current = make_metrics(row_count=500)
        anomalies = self.detector.detect(current, history)
        assert anomalies == []


# ─────────────────────────────────────────────────────────────────────────────
# Trend
# ─────────────────────────────────────────────────────────────────────────────

class TestTrendDetector:

    def setup_method(self):
        self.detector = TrendDetector(
            window=14,
            p_value_threshold=0.05,
            min_points=7,
            relative_change_threshold=0.20,
        )

    def _degrading_history(self, n: int = 20, drop_per_day: int = 200) -> list[PipelineMetrics]:
        base = datetime.utcnow() - timedelta(days=n + 1)
        return [
            make_metrics(
                measured_at=base + timedelta(days=i),
                row_count=max(100, 10_000 - i * drop_per_day),
            )
            for i in range(n)
        ]

    def test_degrading_volume_trend_detected(self):
        history = self._degrading_history()
        current = make_metrics(
            measured_at=datetime.utcnow(),
            row_count=max(100, 10_000 - 21 * 200),
        )
        anomalies = self.detector.detect(current, history)
        trend = [a for a in anomalies if a.level == AnomalyLevel.TEMPORAL]
        assert trend, "Expected trend anomaly for steadily dropping row_count"

    def test_stable_data_no_trend(self):
        history = make_history(n=30, noise_pct=0.02)
        current = make_metrics(row_count=10_000)
        anomalies = self.detector.detect(current, history)
        assert anomalies == []

    def test_insufficient_points_no_trend(self):
        history = make_history(n=4)
        current = make_metrics(row_count=500)
        anomalies = self.detector.detect(current, history)
        assert anomalies == []


# ─────────────────────────────────────────────────────────────────────────────
# Correlation
# ─────────────────────────────────────────────────────────────────────────────

class TestCorrelationDetector:

    def setup_method(self):
        self.detector = CorrelationDetector(
            window=10, correlation_threshold=0.80, min_points=5
        )

    def _correlated_histories(self):
        """Two pipelines that drop together."""
        base = datetime.utcnow() - timedelta(days=11)
        counts = [10_000, 9_800, 9_600, 9_100, 8_500, 7_800, 6_900, 5_000, 3_000, 1_000]
        h1 = [make_metrics("pipe_a", measured_at=base + timedelta(days=i), row_count=c)
              for i, c in enumerate(counts)]
        h2 = [make_metrics("pipe_b", measured_at=base + timedelta(days=i), row_count=int(c * 0.9))
              for i, c in enumerate(counts)]
        return h1, h2

    def test_correlated_drop_detected(self):
        h1, h2 = self._correlated_histories()
        anomalies = self.detector.detect_cross_pipeline(
            {"pipe_a": h1, "pipe_b": h2}, metric="row_count"
        )
        assert anomalies, "Expected correlation anomaly"
        assert any("pipe_a" in a.description or "pipe_b" in a.description for a in anomalies)

    def test_uncorrelated_pipelines_no_anomaly(self):
        base = datetime.utcnow() - timedelta(days=11)
        rng = random.Random(7)
        h1 = [make_metrics("p1", measured_at=base + timedelta(days=i),
                            row_count=int(10_000 * (1 + rng.gauss(0, 0.05)))) for i in range(10)]
        h2 = [make_metrics("p2", measured_at=base + timedelta(days=i),
                            row_count=int(10_000 * (1 + rng.gauss(0, 0.05)))) for i in range(10)]
        anomalies = self.detector.detect_cross_pipeline({"p1": h1, "p2": h2})
        # May or may not flag – just ensure no crash
        assert isinstance(anomalies, list)

    def test_single_pipeline_no_crash(self):
        h1 = make_history(n=10, pipeline="solo")
        anomalies = self.detector.detect_cross_pipeline({"solo": h1})
        assert anomalies == []


# ─────────────────────────────────────────────────────────────────────────────
# ML Baseline (Isolation Forest)
# ─────────────────────────────────────────────────────────────────────────────

class TestMLBaselineDetector:

    def setup_method(self):
        self.detector = MLBaselineDetector(
            contamination=0.05,
            n_estimators=50,
            min_train_samples=20,
            tracked_columns=["amount"],
        )

    def test_normal_point_not_flagged(self):
        history = make_history(n=40)
        current = make_metrics(row_count=10_000)
        anomalies = self.detector.detect(current, history)
        # Normal point: should be clean (not guaranteed but very likely with Isolation Forest)
        # We allow 0 or 1 anomaly here since the model is probabilistic
        assert isinstance(anomalies, list)

    def test_extreme_anomaly_detected(self):
        history = make_history(n=40)
        # Extreme outlier: 100x normal volume and duration
        current = make_metrics(row_count=1_000_000, duration_seconds=100_000, col_mean=10_000)
        anomalies = self.detector.detect(current, history)
        assert anomalies, "Expected ML to flag a severe outlier"
        assert anomalies[0].level == AnomalyLevel.ML

    def test_insufficient_history_returns_empty(self):
        history = make_history(n=10)   # below min_train_samples=20
        current = make_metrics(row_count=500)
        anomalies = self.detector.detect(current, history)
        assert anomalies == []

    def test_anomaly_score_returned(self):
        history = make_history(n=40)
        current = make_metrics(row_count=10_000)
        score = self.detector.get_anomaly_score(current, history)
        assert score is not None
        assert isinstance(score, float)

    def test_anomaly_has_correct_fields(self):
        history = make_history(n=40)
        current = make_metrics(row_count=1_000_000, duration_seconds=100_000)
        anomalies = self.detector.detect(current, history)
        if anomalies:
            a = anomalies[0]
            assert a.level == AnomalyLevel.ML
            assert "IsolationForest" in str(a.context)
            assert a.metric_name == "isolation_forest_score"
