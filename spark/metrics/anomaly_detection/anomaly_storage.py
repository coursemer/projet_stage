"""
Storage Adapters
-----------------
Two adapters for persisting metrics and anomaly events:

  - InfluxDBStorage  : for InfluxDB (time-series)
  - TimescaleStorage : for TimescaleDB / PostgreSQL

Both implement the same StorageBase interface so the rest of the
codebase doesn't care which backend is used.
"""
from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import List, Optional

from .models import Anomaly, AnomalyLevel, PipelineMetrics, Severity

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Abstract base
# ─────────────────────────────────────────────────────────────────────────────

class StorageBase(ABC):

    @abstractmethod
    def save_metrics(self, metrics: PipelineMetrics) -> None: ...

    @abstractmethod
    def save_anomaly(self, anomaly: Anomaly) -> None: ...

    @abstractmethod
    def get_history(
        self,
        pipeline_name: str,
        days: int = 30,
    ) -> List[PipelineMetrics]: ...

    @abstractmethod
    def get_anomaly_history(
        self,
        pipeline_name: str,
        days: int = 30,
    ) -> List[Anomaly]: ...


# ─────────────────────────────────────────────────────────────────────────────
# InfluxDB adapter
# ─────────────────────────────────────────────────────────────────────────────

class InfluxDBStorage(StorageBase):
    """
    Store metrics and anomalies in InfluxDB 2.x using the official Python client.

    Connection setup:
        storage = InfluxDBStorage(
            url="http://localhost:8086",
            token="my-token",
            org="my-org",
            bucket="data-trust",
        )
    """

    def __init__(
        self, url: str, token: str, org: str, bucket: str = "data-trust"
    ) -> None:
        try:
            from influxdb_client import InfluxDBClient, WriteOptions  # type: ignore
            from influxdb_client.client.write_api import SYNCHRONOUS   # type: ignore
        except ImportError:
            raise ImportError("Install influxdb-client: pip install influxdb-client")

        self._client = InfluxDBClient(url=url, token=token, org=org)
        self._write_api = self._client.write_api(write_options=SYNCHRONOUS)
        self._query_api = self._client.query_api()
        self._bucket = bucket
        self._org = org

    # ------------------------------------------------------------------

    def save_metrics(self, metrics: PipelineMetrics) -> None:
        from influxdb_client import Point  # type: ignore

        p = (
            Point("pipeline_metrics")
            .tag("pipeline", metrics.pipeline_name)
            .time(metrics.measured_at)
        )

        if metrics.row_count is not None:
            p = p.field("row_count", float(metrics.row_count))
        if metrics.duration_seconds is not None:
            p = p.field("duration_seconds", metrics.duration_seconds)
        if metrics.success is not None:
            p = p.field("success", int(metrics.success))
        if metrics.task_failures is not None:
            p = p.field("task_failures", float(metrics.task_failures))

        # Flatten column stats
        for col, stat_dict in metrics.column_stats.items():
            for stat, val in stat_dict.items():
                p = p.field(f"col_{col}_{stat}", float(val))

        self._write_api.write(bucket=self._bucket, org=self._org, record=p)
        logger.debug("Saved metrics for %s", metrics.pipeline_name)

    def save_anomaly(self, anomaly: Anomaly) -> None:
        from influxdb_client import Point  # type: ignore

        p = (
            Point("anomaly_events")
            .tag("pipeline",  anomaly.pipeline_name)
            .tag("level",     anomaly.level)
            .tag("severity",  anomaly.severity)
            .tag("metric",    anomaly.metric_name)
            .field("anomaly_id",      anomaly.id)
            .field("description",     anomaly.description)
            .field("severity_score",  float(anomaly.severity_score))
            .field("is_recurring",    int(anomaly.is_recurring))
            .field("context_json",    json.dumps(anomaly.context))
            .time(anomaly.detected_at)
        )

        if anomaly.observed_value is not None:
            p = p.field("observed_value", anomaly.observed_value)
        if anomaly.expected_value is not None:
            p = p.field("expected_value", anomaly.expected_value)
        if anomaly.deviation_sigma is not None:
            p = p.field("deviation_sigma", anomaly.deviation_sigma)

        self._write_api.write(bucket=self._bucket, org=self._org, record=p)
        logger.debug("Saved anomaly %s", anomaly.id)

    def get_history(
        self,
        pipeline_name: str,
        days: int = 30,
    ) -> List[PipelineMetrics]:
        flux = f"""
        from(bucket: "{self._bucket}")
          |> range(start: -{days}d)
          |> filter(fn: (r) => r._measurement == "pipeline_metrics")
          |> filter(fn: (r) => r.pipeline == "{pipeline_name}")
          |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
          |> sort(columns: ["_time"])
        """
        tables = self._query_api.query(flux, org=self._org)
        results = []
        for table in tables:
            for record in table.records:
                vals = record.values
                m = PipelineMetrics(
                    pipeline_name=pipeline_name,
                    measured_at=record.get_time(),
                    row_count=int(vals["row_count"]) if "row_count" in vals else None,
                    duration_seconds=vals.get("duration_seconds"),
                    success=bool(vals["success"]) if "success" in vals else None,
                    task_failures=int(vals["task_failures"]) if "task_failures" in vals else None,
                )
                results.append(m)
        return results

    def get_anomaly_history(
        self,
        pipeline_name: str,
        days: int = 30,
    ) -> List[Anomaly]:
        flux = f"""
        from(bucket: "{self._bucket}")
          |> range(start: -{days}d)
          |> filter(fn: (r) => r._measurement == "anomaly_events")
          |> filter(fn: (r) => r.pipeline == "{pipeline_name}")
          |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
          |> sort(columns: ["_time"])
        """
        tables = self._query_api.query(flux, org=self._org)
        results = []
        for table in tables:
            for record in table.records:
                vals = record.values
                try:
                    a = Anomaly(
                        id=vals.get("anomaly_id", "unknown"),
                        pipeline_name=pipeline_name,
                        detected_at=record.get_time(),
                        level=vals.get("level", AnomalyLevel.VOLUME),
                        severity=vals.get("severity", Severity.MEDIUM),
                        description=vals.get("description", ""),
                        metric_name=vals.get("metric", "unknown"),
                        observed_value=vals.get("observed_value"),
                        expected_value=vals.get("expected_value"),
                        deviation_sigma=vals.get("deviation_sigma"),
                        severity_score=float(vals.get("severity_score", 0.0)),
                        context=json.loads(vals.get("context_json", "{}")),
                    )
                    results.append(a)
                except Exception as exc:
                    logger.warning("Could not deserialize anomaly record: %s", exc)
        return results


# ─────────────────────────────────────────────────────────────────────────────
# TimescaleDB adapter (PostgreSQL)
# ─────────────────────────────────────────────────────────────────────────────

METRICS_TABLE = """
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    time            TIMESTAMPTZ NOT NULL,
    pipeline_name   TEXT NOT NULL,
    row_count       BIGINT,
    duration_seconds FLOAT,
    success         BOOLEAN,
    task_failures   INT,
    column_stats    JSONB,
    extra           JSONB
);
SELECT create_hypertable('pipeline_metrics', 'time', if_not_exists => TRUE);
"""

ANOMALY_TABLE = """
CREATE TABLE IF NOT EXISTS anomaly_events (
    time              TIMESTAMPTZ NOT NULL,
    anomaly_id        TEXT NOT NULL,
    pipeline_name     TEXT NOT NULL,
    level             TEXT NOT NULL,
    severity          TEXT NOT NULL,
    severity_score    FLOAT NOT NULL DEFAULT 0,
    metric_name       TEXT NOT NULL,
    description       TEXT NOT NULL,
    observed_value    FLOAT,
    expected_value    FLOAT,
    deviation_sigma   FLOAT,
    is_recurring      BOOLEAN DEFAULT FALSE,
    occurrence_count  INT DEFAULT 1,
    context           JSONB
);
SELECT create_hypertable('anomaly_events', 'time', if_not_exists => TRUE);
"""


class TimescaleStorage(StorageBase):
    """
    Store metrics and anomalies in TimescaleDB (PostgreSQL extension).

    Connection setup:
        storage = TimescaleStorage(
            dsn="postgresql://user:pass@localhost:5432/data_trust"
        )
    """

    def __init__(self, dsn: str) -> None:
        try:
            import psycopg2  # type: ignore
        except ImportError:
            raise ImportError("Install psycopg2-binary: pip install psycopg2-binary")

        self._dsn = dsn
        self._ensure_tables()

    def _conn(self):
        import psycopg2
        return psycopg2.connect(self._dsn)

    def _ensure_tables(self) -> None:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(METRICS_TABLE)
                cur.execute(ANOMALY_TABLE)
            conn.commit()

    # ------------------------------------------------------------------

    def save_metrics(self, metrics: PipelineMetrics) -> None:
        sql = """
        INSERT INTO pipeline_metrics
          (time, pipeline_name, row_count, duration_seconds, success,
           task_failures, column_stats, extra)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (
                    metrics.measured_at,
                    metrics.pipeline_name,
                    metrics.row_count,
                    metrics.duration_seconds,
                    metrics.success,
                    metrics.task_failures,
                    json.dumps(metrics.column_stats),
                    json.dumps(metrics.extra),
                ))
            conn.commit()

    def save_anomaly(self, anomaly: Anomaly) -> None:
        sql = """
        INSERT INTO anomaly_events
          (time, anomaly_id, pipeline_name, level, severity, severity_score,
           metric_name, description, observed_value, expected_value,
           deviation_sigma, is_recurring, occurrence_count, context)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (
                    anomaly.detected_at,
                    anomaly.id,
                    anomaly.pipeline_name,
                    anomaly.level,
                    anomaly.severity,
                    anomaly.severity_score,
                    anomaly.metric_name,
                    anomaly.description,
                    anomaly.observed_value,
                    anomaly.expected_value,
                    anomaly.deviation_sigma,
                    anomaly.is_recurring,
                    anomaly.occurrence_count,
                    json.dumps(anomaly.context),
                ))
            conn.commit()

    def get_history(
        self,
        pipeline_name: str,
        days: int = 30,
    ) -> List[PipelineMetrics]:
        sql = """
        SELECT time, row_count, duration_seconds, success, task_failures,
               column_stats, extra
        FROM pipeline_metrics
        WHERE pipeline_name = %s AND time > NOW() - INTERVAL '%s days'
        ORDER BY time ASC
        """
        results = []
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (pipeline_name, days))
                for row in cur.fetchall():
                    t, rc, dur, success, tf, col_stats, extra = row
                    m = PipelineMetrics(
                        pipeline_name=pipeline_name,
                        measured_at=t,
                        row_count=rc,
                        duration_seconds=dur,
                        success=success,
                        task_failures=tf,
                        column_stats=col_stats or {},
                        extra=extra or {},
                    )
                    results.append(m)
        return results

    def get_anomaly_history(
        self,
        pipeline_name: str,
        days: int = 30,
    ) -> List[Anomaly]:
        sql = """
        SELECT time, anomaly_id, level, severity, severity_score, metric_name,
               description, observed_value, expected_value, deviation_sigma,
               is_recurring, occurrence_count, context
        FROM anomaly_events
        WHERE pipeline_name = %s AND time > NOW() - INTERVAL '%s days'
        ORDER BY time ASC
        """
        results = []
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (pipeline_name, days))
                for row in cur.fetchall():
                    (t, aid, level, sev, score, metric, desc,
                     obs, exp, sigma, recurring, count, ctx) = row
                    a = Anomaly(
                        id=aid,
                        pipeline_name=pipeline_name,
                        detected_at=t,
                        level=level,
                        severity=sev,
                        severity_score=float(score or 0.0),
                        metric_name=metric,
                        description=desc,
                        observed_value=obs,
                        expected_value=exp,
                        deviation_sigma=sigma,
                        is_recurring=recurring,
                        occurrence_count=count or 1,
                        context=ctx or {},
                    )
                    results.append(a)
        return results
