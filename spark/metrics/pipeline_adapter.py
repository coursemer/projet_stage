"""
Bridge entre MetricPoints SQLite et PipelineMetrics pour les détecteurs ML.

Convertit la série temporelle brute du store en snapshots PipelineMetrics
groupés par pipeline et par jour. Utilisé par run_detection_ml() dans
run_collector.py et par l'endpoint /api/v1/alerts/detect-ml dans api.py.

Usage :
    from spark.metrics.storage        import get_store
    from spark.metrics.pipeline_adapter import build_pipeline_snapshots

    store     = get_store()
    snapshots = build_pipeline_snapshots(store)
    for pipeline, (current, history) in snapshots.items():
        print(pipeline, current.row_count, len(history))
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from spark.metrics.storage import SQLiteMetricsStore
from spark.metrics.anomaly_detection.models import PipelineMetrics


# ── Pipeline name extraction ──────────────────────────────────────────────────

def _pipeline_name(name: str, source: str, tags: dict) -> str:
    """Extracts the logical pipeline name from a metric row."""
    if source == "airflow":
        dag_id = tags.get("dag_id") or tags.get("dag")
        if dag_id:
            return str(dag_id)
        return name.split(".")[0] if "." in name else name
    if source == "dbt":
        model = tags.get("model")
        if model:
            return str(model)
        return "dbt_project"
    # spark — tags contain {"job": "ingest_sales", "run_date": "..."}
    job = tags.get("job")
    if job:
        return str(job)
    # fallback: first component of metric name
    return name.split(".")[0] if "." in name else name


def _metric_suffix(name: str, source: str) -> str:
    """Returns the metric suffix used as key in the day snapshot dict."""
    # For all sources, use the part after the first dot as the suffix key.
    # e.g. "job.rows_output" → "rows_output"
    #      "dag.duration_seconds" → "duration_seconds"
    #      "dbt.model.status" → "model.status"
    parts = name.split(".", 1)
    return parts[1] if len(parts) > 1 else name


# ── Group rows by (pipeline, day) ─────────────────────────────────────────────

def _group_by_pipeline_and_day(
    rows: List[dict],
) -> Dict[str, Dict[str, Dict[str, object]]]:
    """
    Returns {pipeline: {YYYY-MM-DD: {metric_suffix: value, _source, _ts}}}.
    When multiple rows exist for (pipeline, day, suffix), the most recent wins.
    """
    groups: Dict[str, Dict[str, Dict[str, object]]] = {}
    for row in sorted(rows, key=lambda r: r.get("ts", "")):
        source  = row.get("source", "spark")
        name    = row.get("name", "")
        value   = row.get("value", 0.0)
        tags    = row.get("tags") or {}
        ts      = row.get("ts", "")

        pipeline = _pipeline_name(name, source, tags)
        suffix   = _metric_suffix(name, source)
        # Prefer run_date tag (Spark emitter) over ts for day grouping
        day      = tags.get("run_date") or (ts[:10] if len(ts) >= 10 else "unknown")

        day_data = groups.setdefault(pipeline, {}).setdefault(day, {})
        day_data[suffix]    = value
        day_data["_source"] = source
        day_data["_ts"]     = ts

    return groups


# ── Convert one day snapshot to PipelineMetrics ───────────────────────────────

def _to_pipeline_metrics(
    pipeline_name: str,
    day: str,
    metrics: Dict[str, object],
) -> PipelineMetrics:
    ts_str = str(metrics.get("_ts") or f"{day}T12:00:00+00:00")
    try:
        measured_at = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        measured_at = datetime.now(timezone.utc)

    # Volume — prefer rows_output, fallback to rows_input
    row_count: Optional[int] = None
    for key in ("rows_output", "rows_input"):
        if key in metrics:
            row_count = int(float(metrics[key]))  # type: ignore[arg-type]
            break

    # Performance — duration
    duration: Optional[float] = None
    for key in ("duration_seconds",):
        if key in metrics:
            duration = float(metrics[key])  # type: ignore[arg-type]
            break

    # Success flag — derived from tags.status or dag.status metric
    success: Optional[bool] = None
    for key in ("dag.status", "model.status"):
        if key in metrics:
            success = float(metrics[key]) >= 0.5  # type: ignore[arg-type]
            break

    # Task failures: try_number > 1 means at least one retry
    task_failures: Optional[int] = None
    if "try_number" in metrics:
        tries = int(float(metrics["try_number"]))  # type: ignore[arg-type]
        task_failures = max(0, tries - 1)

    # Column stats — build from rejection_rate if present
    column_stats: Dict[str, Dict[str, float]] = {}
    if "rejection_rate_pct" in metrics:
        rate = float(metrics["rejection_rate_pct"])  # type: ignore[arg-type]
        column_stats["rejection_rate"] = {
            "mean": rate,
            "std":  0.0,
            "null_rate": rate / 100.0,
            "min":  rate,
            "max":  rate,
        }

    _skip = {"rows_output", "rows_input", "duration_seconds", "rejection_rate_pct",
             "dag.status", "model.status", "try_number"}
    extra = {
        k: v for k, v in metrics.items()
        if not k.startswith("_") and k not in _skip
    }

    return PipelineMetrics(
        pipeline_name=pipeline_name,
        measured_at=measured_at,
        row_count=row_count,
        duration_seconds=duration,
        success=success,
        task_failures=task_failures,
        column_stats=column_stats,
        extra={k: str(v) for k, v in extra.items()},
    )


# ── Public API ────────────────────────────────────────────────────────────────

def build_pipeline_snapshots(
    store: SQLiteMetricsStore,
    limit: int = 10_000,
) -> Dict[str, Tuple[PipelineMetrics, List[PipelineMetrics]]]:
    """
    For each pipeline in the store, returns (current, history).

    current : PipelineMetrics for the most recent day seen
    history : List[PipelineMetrics] for all earlier days, sorted ascending

    Only pipelines with at least 2 day-snapshots (current + ≥1 history) are
    returned, as the ML detectors need at least a minimal history.
    """
    rows   = store.query(limit=limit)
    groups = _group_by_pipeline_and_day(rows)

    result: Dict[str, Tuple[PipelineMetrics, List[PipelineMetrics]]] = {}
    for pipeline, day_map in groups.items():
        sorted_days = sorted(day_map.keys())
        snapshots = [
            _to_pipeline_metrics(pipeline, day, day_map[day])
            for day in sorted_days
        ]
        if not snapshots:
            continue
        # current = most recent day; history = all earlier days (may be empty)
        result[pipeline] = (snapshots[-1], snapshots[:-1])

    return result
