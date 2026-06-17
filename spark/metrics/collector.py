"""
Collecteurs de métriques pour Airflow, Spark et dbt.

AirflowCollector  — lit airflow.db (SQLite) ou les fichiers de run JSON
SparkCollector    — lit spark/data/metrics/**/*.json (émis par SparkMetricsEmitter)
DbtCollector      — lit dbt/target/run_results.json

SparkMetricsEmitter — context-manager utilisé dans les jobs Spark pour
                      émettre automatiquement durée, lignes in/out, statut.

Usage collecteur :
    from spark.metrics.collector import AirflowCollector, SparkCollector, DbtCollector
    from spark.metrics.storage   import get_store

    store = get_store()
    for Cls in [AirflowCollector, SparkCollector, DbtCollector]:
        points = Cls().collect()
        store.write(points)
        print(f"{Cls.__name__}: {len(points)} points")

Usage émetteur :
    from spark.metrics.collector import SparkMetricsEmitter
    with SparkMetricsEmitter("ingest_sales", run_date="2026-04-08") as emitter:
        # ... logique Spark ...
        emitter.record(rows_input=50_000, rows_output=48_000, rows_rejected=2_000)
"""

from __future__ import annotations

import json
import os
import sqlite3
import time
from datetime import datetime, timezone
from typing import Optional
BASE_DIR     = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import sys as _sys
if BASE_DIR not in _sys.path:
    _sys.path.insert(0, BASE_DIR)

try:
    from .storage import MetricPoint
except ImportError:
    from spark.metrics.storage import MetricPoint
METRICS_DIR  = os.path.join(BASE_DIR, "spark", "data", "metrics")
DBT_RESULTS  = os.path.join(BASE_DIR, "dbt", "target", "run_results.json")

# Chemins courants pour airflow.db selon l'environnement
_AIRFLOW_DB_CANDIDATES = [
    os.path.join(BASE_DIR, "airflow.db"),
    os.path.join(BASE_DIR, "logs", "airflow.db"),
    "/tmp/airflow.db",
    os.path.expanduser("~/airflow/airflow.db"),
]


# ── Airflow ───────────────────────────────────────────────────────────────────

class AirflowCollector:
    """
    Extrait les métriques depuis la base SQLite d'Airflow.

    Métriques produites :
      airflow.dag.duration_seconds   — durée totale d'un DagRun (end - start)
      airflow.dag.status             — 1.0 = success, 0.0 = failed/other
      airflow.task.duration_seconds  — durée d'une TaskInstance
      airflow.task.try_number        — nombre de tentatives
    """

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or self._find_db()

    @staticmethod
    def _find_db() -> Optional[str]:
        for path in _AIRFLOW_DB_CANDIDATES:
            if os.path.exists(path):
                return path
        return None

    def collect(self, limit: int = 200) -> list[MetricPoint]:
        if not self.db_path:
            return []
        try:
            return self._collect_from_db(limit)
        except Exception:
            return []

    def _collect_from_db(self, limit: int) -> list[MetricPoint]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        points: list[MetricPoint] = []

        # ── DagRun metrics ────────────────────────────────────────────────────
        try:
            rows = conn.execute(f"""
                SELECT dag_id, run_id, execution_date, start_date, end_date, state
                FROM dag_run
                WHERE start_date IS NOT NULL
                ORDER BY start_date DESC
                LIMIT {limit}
            """).fetchall()

            for r in rows:
                ts = r["end_date"] or r["start_date"] or datetime.now(timezone.utc).isoformat()
                tags = {"dag_id": r["dag_id"], "run_id": str(r["run_id"] or "")}

                if r["start_date"] and r["end_date"]:
                    try:
                        start = datetime.fromisoformat(str(r["start_date"]).replace("Z", "+00:00"))
                        end   = datetime.fromisoformat(str(r["end_date"]).replace("Z", "+00:00"))
                        duration = (end - start).total_seconds()
                        points.append(MetricPoint(
                            source="airflow", name="dag.duration_seconds",
                            value=duration, ts=ts, tags=tags,
                        ))
                    except (ValueError, TypeError):
                        pass

                status_val = 1.0 if str(r["state"]) == "success" else 0.0
                points.append(MetricPoint(
                    source="airflow", name="dag.status",
                    value=status_val, ts=ts, tags={**tags, "state": str(r["state"])},
                ))
        except sqlite3.OperationalError:
            pass

        # ── TaskInstance metrics ──────────────────────────────────────────────
        try:
            rows = conn.execute(f"""
                SELECT dag_id, task_id, execution_date, start_date, end_date,
                       duration, state, try_number
                FROM task_instance
                WHERE start_date IS NOT NULL
                ORDER BY start_date DESC
                LIMIT {limit}
            """).fetchall()

            for r in rows:
                ts   = r["end_date"] or r["start_date"] or datetime.now(timezone.utc).isoformat()
                tags = {
                    "dag_id":  r["dag_id"],
                    "task_id": r["task_id"],
                    "state":   str(r["state"]),
                }
                if r["duration"] is not None:
                    points.append(MetricPoint(
                        source="airflow", name="task.duration_seconds",
                        value=float(r["duration"]), ts=ts, tags=tags,
                    ))
                if r["try_number"] is not None:
                    points.append(MetricPoint(
                        source="airflow", name="task.try_number",
                        value=float(r["try_number"]), ts=ts, tags=tags,
                    ))
        except sqlite3.OperationalError:
            pass

        conn.close()
        return points


# ── Spark ─────────────────────────────────────────────────────────────────────

class SparkCollector:
    """
    Lit les fichiers JSON émis par SparkMetricsEmitter dans spark/data/metrics/.

    Structure attendue : spark/data/metrics/{job_name}/{run_date}.json
    """

    def __init__(self, metrics_dir: str = METRICS_DIR):
        self.metrics_dir = metrics_dir

    def collect(self) -> list[MetricPoint]:
        if not os.path.isdir(self.metrics_dir):
            return []

        points: list[MetricPoint] = []
        for job_name in os.listdir(self.metrics_dir):
            job_dir = os.path.join(self.metrics_dir, job_name)
            if not os.path.isdir(job_dir):
                continue
            for fname in sorted(os.listdir(job_dir)):
                if not fname.endswith(".json"):
                    continue
                fpath = os.path.join(job_dir, fname)
                try:
                    with open(fpath, encoding="utf-8") as f:
                        data = json.load(f)
                    points.extend(self._parse_run_file(data, job_name))
                except (json.JSONDecodeError, KeyError):
                    continue
        return points

    @staticmethod
    def _parse_run_file(data: dict, job_name: str) -> list[MetricPoint]:
        ts   = data.get("ts") or data.get("end_ts") or datetime.now(timezone.utc).isoformat()
        tags = {"job": job_name, "run_date": data.get("run_date", ""), "status": data.get("status", "ok")}
        pts  = []

        field_map = {
            "duration_seconds": "job.duration_seconds",
            "rows_input":       "job.rows_input",
            "rows_output":      "job.rows_output",
            "rows_rejected":    "job.rows_rejected",
        }
        for field, metric_name in field_map.items():
            if field in data and data[field] is not None:
                pts.append(MetricPoint(
                    source="spark", name=metric_name,
                    value=float(data[field]), ts=ts, tags=tags,
                    extra={k: v for k, v in data.items()
                           if k not in ("ts", "end_ts", "run_date", "status", field)
                           and isinstance(v, (int, float, str))},
                ))

        # Taux de rejet calculé si possible
        if "rows_input" in data and "rows_rejected" in data and data["rows_input"]:
            rejection_rate = round(data["rows_rejected"] / data["rows_input"] * 100, 2)
            pts.append(MetricPoint(
                source="spark", name="job.rejection_rate_pct",
                value=rejection_rate, ts=ts, tags=tags,
            ))
        return pts


# ── dbt ───────────────────────────────────────────────────────────────────────

class DbtCollector:
    """
    Lit dbt/target/run_results.json (artefact standard dbt Core).

    Métriques produites :
      dbt.model.execution_time   — durée d'exécution du modèle (secondes)
      dbt.model.status           — 1.0 = success, 0.0 = error/fail
      dbt.test.status            — 1.0 = pass, 0.0 = fail/error/warn
      dbt.run.pass_rate          — pourcentage de résultats réussis
    """

    def __init__(self, results_path: str = DBT_RESULTS):
        self.results_path = results_path

    def collect(self) -> list[MetricPoint]:
        if not os.path.exists(self.results_path):
            return []
        try:
            with open(self.results_path, encoding="utf-8") as f:
                data = json.load(f)
            return self._parse(data)
        except (json.JSONDecodeError, KeyError):
            return []

    @staticmethod
    def _parse(data: dict) -> list[MetricPoint]:
        results   = data.get("results", [])
        generated = data.get("metadata", {}).get("generated_at") or datetime.now(timezone.utc).isoformat()
        points: list[MetricPoint] = []

        n_ok = 0
        for result in results:
            unique_id = result.get("unique_id", "")
            node_type = unique_id.split(".")[0] if "." in unique_id else "unknown"
            node_name = unique_id.rsplit(".", 1)[-1]
            status    = result.get("status", "")
            timing    = result.get("execution_time") or 0.0
            ts        = generated

            tags = {"node": node_name, "node_type": node_type, "status": status}

            if node_type in ("model", "seed", "snapshot"):
                points.append(MetricPoint(
                    source="dbt", name="model.execution_time",
                    value=float(timing), ts=ts, tags=tags,
                ))
                is_ok = 1.0 if status == "success" else 0.0
                if is_ok:
                    n_ok += 1
                points.append(MetricPoint(
                    source="dbt", name="model.status",
                    value=is_ok, ts=ts, tags=tags,
                ))

            elif node_type == "test":
                is_pass = 1.0 if status == "pass" else 0.0
                if is_pass:
                    n_ok += 1
                points.append(MetricPoint(
                    source="dbt", name="test.status",
                    value=is_pass, ts=ts, tags=tags,
                ))

        if results:
            pass_rate = round(n_ok / len(results) * 100, 2)
            points.append(MetricPoint(
                source="dbt", name="run.pass_rate",
                value=pass_rate, ts=generated,
                tags={"total": len(results), "passed": n_ok},
            ))
        return points


# ── SparkMetricsEmitter ───────────────────────────────────────────────────────

class SparkMetricsEmitter:
    """
    Context-manager à utiliser dans les jobs Spark pour émettre des métriques.

    Écrit un fichier JSON dans spark/data/metrics/{job_name}/{run_date}.json
    et l'insère aussi dans le store SQLite.

    Usage :
        with SparkMetricsEmitter("ingest_sales", run_date="2026-04-08") as em:
            df = spark.read.csv(...)
            em.record(rows_input=df.count())
            # ... traitement ...
            em.record(rows_output=clean.count(), rows_rejected=bad.count())
    """

    def __init__(
        self,
        job_name:    str,
        run_date:    Optional[str] = None,
        metrics_dir: str = METRICS_DIR,
        store=None,
    ):
        self.job_name    = job_name
        self.run_date    = run_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        self.metrics_dir = metrics_dir
        self._store      = store
        self._start      = 0.0
        self._data: dict = {}

    def __enter__(self) -> "SparkMetricsEmitter":
        self._start = time.monotonic()
        self._data  = {"run_date": self.run_date, "job": self.job_name, "status": "running"}
        return self

    def record(self, **kwargs) -> None:
        """Met à jour les métriques en cours de job."""
        self._data.update(kwargs)

    def __exit__(self, exc_type, *_) -> None:
        duration = round(time.monotonic() - self._start, 3)
        self._data["duration_seconds"] = duration
        self._data["status"]   = "error" if exc_type else "ok"
        self._data["ts"]       = datetime.now(timezone.utc).isoformat()
        self._data["end_ts"]   = self._data["ts"]

        job_dir = os.path.join(self.metrics_dir, self.job_name)
        os.makedirs(job_dir, exist_ok=True)
        out_path = os.path.join(job_dir, f"{self.run_date}.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(self._data, f, indent=2, ensure_ascii=False)

        # Persistance immédiate dans le store SQLite
        if self._store is None:
            try:
                from .storage import get_store
                self._store = get_store()
            except Exception:
                return
        try:
            pts = SparkCollector._parse_run_file(self._data, self.job_name)
            self._store.write(pts)
        except Exception:
            pass

