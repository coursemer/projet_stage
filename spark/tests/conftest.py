"""
Shared fixtures for spark/tests/.

Adds spark/metrics/ to sys.path so that `anomaly_detection` is importable
as a bare module name (matching the import style used in all test files and
in anomaly_detection/conftest.py).
"""
from __future__ import annotations

import os
import sys

# Make `anomaly_detection` importable without the full spark.metrics prefix
_METRICS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "metrics"))
if _METRICS_DIR not in sys.path:
    sys.path.insert(0, _METRICS_DIR)

# Also ensure project root is on path for `spark.*` imports
_BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _BASE_DIR not in sys.path:
    sys.path.insert(0, _BASE_DIR)

import random
from datetime import datetime, timedelta
from typing import List, Optional

import pytest

from anomaly_detection.models import PipelineMetrics


# ── Builders ──────────────────────────────────────────────────────────────────

def make_metrics(
    pipeline: str = "test_pipeline",
    measured_at: Optional[datetime] = None,
    row_count: Optional[int] = 10_000,
    duration_seconds: Optional[float] = 120.0,
    success: Optional[bool] = True,
    task_failures: Optional[int] = 0,
    col_mean: float = 100.0,
    col_std: float = 10.0,
    null_rate: float = 0.01,
    schema: Optional[dict] = None,
) -> PipelineMetrics:
    return PipelineMetrics(
        pipeline_name=pipeline,
        measured_at=measured_at or datetime.utcnow(),
        row_count=row_count,
        duration_seconds=duration_seconds,
        success=success,
        task_failures=task_failures,
        column_stats={
            "amount": {
                "mean":      col_mean,
                "std":       col_std,
                "null_rate": null_rate,
                "min":       col_mean - 3 * col_std,
                "max":       col_mean + 3 * col_std,
            }
        },
        schema_snapshot=schema or {"order_id": "int64", "amount": "float64", "customer_id": "int64"},
    )


def make_history(
    n: int = 30,
    pipeline: str = "test_pipeline",
    base_row_count: int = 10_000,
    base_duration: float = 120.0,
    noise_pct: float = 0.05,
    start_days_ago: int = 31,
) -> List[PipelineMetrics]:
    rng = random.Random(42)
    history = []
    for i in range(n):
        dt = datetime.utcnow() - timedelta(days=start_days_ago - i)
        rc  = int(base_row_count  * (1 + rng.gauss(0, noise_pct)))
        dur = base_duration * (1 + rng.gauss(0, noise_pct))
        history.append(make_metrics(
            pipeline=pipeline,
            measured_at=dt,
            row_count=max(1, rc),
            duration_seconds=max(1.0, dur),
        ))
    return history


# ── Pytest fixtures ───────────────────────────────────────────────────────────

@pytest.fixture
def normal_history() -> List[PipelineMetrics]:
    return make_history()


@pytest.fixture
def current_normal() -> PipelineMetrics:
    return make_metrics()


@pytest.fixture
def current_volume_drop() -> PipelineMetrics:
    return make_metrics(row_count=500)


@pytest.fixture
def current_volume_spike() -> PipelineMetrics:
    return make_metrics(row_count=200_000)


@pytest.fixture
def current_duration_spike() -> PipelineMetrics:
    return make_metrics(duration_seconds=3_600)


@pytest.fixture
def current_null_rate_spike() -> PipelineMetrics:
    m = make_metrics()
    m.column_stats["amount"]["null_rate"] = 0.60
    return m


@pytest.fixture
def current_mean_drift() -> PipelineMetrics:
    return make_metrics(col_mean=500.0)


@pytest.fixture
def schema_dropped_column() -> PipelineMetrics:
    return make_metrics(schema={"order_id": "int64", "amount": "float64"})


@pytest.fixture
def schema_type_change() -> PipelineMetrics:
    return make_metrics(schema={"order_id": "string", "amount": "float64", "customer_id": "int64"})
