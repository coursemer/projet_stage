"""
Gestionnaire d'alertes — stockage SQLite et consultation.

Les alertes produites par AnomalyDetector sont persistées dans la même base
SQLite que les métriques (table `alerts`).

Schema :
    alerts(id, ts, source, metric_name, algorithm, severity, value,
           expected, details, tags_json, acknowledged)

Usage :
    from spark.metrics.alert_manager import AlertManager
    from spark.metrics.anomaly_detector import AnomalyDetector
    from spark.metrics.storage import get_store

    store   = get_store()
    mgr     = AlertManager(store.db_path)
    alerts  = AnomalyDetector(store).run()
    n       = mgr.save(alerts)
    print(f"{n} alertes sauvegardées")

    recent  = mgr.query(severity="critical", limit=20)
    mgr.acknowledge(alert_id=3)
    summary = mgr.summary()
"""

from __future__ import annotations

import json
import os
import sqlite3
from datetime import datetime, timezone
from typing import Optional
from spark.metrics.anomaly_detection.models import DetectionResult, Severity


BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import sys
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

try:
    from .storage import DEFAULT_DB
    from .anomaly_detector import AnomalyAlert
except ImportError:
    from spark.metrics.storage import DEFAULT_DB
    from spark.metrics.anomaly_detector import AnomalyAlert


class AlertManager:
    """Persistance et consultation des alertes d'anomalies."""

    def __init__(self, db_path: str = DEFAULT_DB):
        self.db_path = db_path
        self._init_table()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_table(self) -> None:
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS alerts (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts           TEXT    NOT NULL,
                    source       TEXT    NOT NULL,
                    metric_name  TEXT    NOT NULL,
                    algorithm    TEXT    NOT NULL,
                    severity     TEXT    NOT NULL,
                    value        REAL,
                    expected     TEXT,
                    details      TEXT,
                    tags         TEXT    DEFAULT '{}',
                    acknowledged INTEGER DEFAULT 0
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_alert_severity ON alerts(severity)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_alert_ts ON alerts(ts)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_alert_source ON alerts(source)")
            conn.commit()

    # ── Écriture ──────────────────────────────────────────────────────────────

    def save(self, alerts: list[AnomalyAlert]) -> int:
        """Insère une liste d'alertes. Retourne le nombre inséré."""
        if not alerts:
            return 0
        rows = [
            (
                a.ts, a.source, a.metric_name, a.algorithm,
                a.severity, a.value, a.expected, a.details,
                json.dumps(a.tags),
            )
            for a in alerts
        ]
        with self._connect() as conn:
            conn.executemany(
                """INSERT INTO alerts
                   (ts, source, metric_name, algorithm, severity, value,
                    expected, details, tags)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                rows,
            )
            conn.commit()
        return len(alerts)

    # ── Lecture ───────────────────────────────────────────────────────────────

    def query(
        self,
        source:       Optional[str] = None,
        severity:     Optional[str] = None,
        metric_name:  Optional[str] = None,
        algorithm:    Optional[str] = None,
        acknowledged: Optional[bool] = None,
        from_ts:      Optional[str] = None,
        to_ts:        Optional[str] = None,
        limit:        int = 200,
    ) -> list[dict]:
        conditions, params = [], []
        if source:
            conditions.append("source = ?"); params.append(source)
        if severity:
            conditions.append("severity = ?"); params.append(severity)
        if metric_name:
            conditions.append("metric_name = ?"); params.append(metric_name)
        if algorithm:
            conditions.append("algorithm = ?"); params.append(algorithm)
        if acknowledged is not None:
            conditions.append("acknowledged = ?"); params.append(int(acknowledged))
        if from_ts:
            conditions.append("ts >= ?"); params.append(from_ts)
        if to_ts:
            conditions.append("ts <= ?"); params.append(to_ts)

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        sql   = f"SELECT * FROM alerts {where} ORDER BY ts DESC LIMIT ?"
        params.append(limit)

        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def summary(self) -> dict:
        """Comptage par sévérité + métriques les plus alertées."""
        with self._connect() as conn:
            by_sev = conn.execute("""
                SELECT severity, COUNT(*) as count
                FROM alerts WHERE acknowledged = 0
                GROUP BY severity ORDER BY count DESC
            """).fetchall()

            top_metrics = conn.execute("""
                SELECT metric_name, source, COUNT(*) as count
                FROM alerts WHERE acknowledged = 0
                GROUP BY metric_name, source ORDER BY count DESC LIMIT 10
            """).fetchall()

            total = conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
            unack = conn.execute(
                "SELECT COUNT(*) FROM alerts WHERE acknowledged = 0"
            ).fetchone()[0]

        return {
            "total":          total,
            "unacknowledged": unack,
            "by_severity":    [dict(r) for r in by_sev],
            "top_metrics":    [dict(r) for r in top_metrics],
        }

    def count(self, acknowledged: Optional[bool] = None) -> int:
        where = "" if acknowledged is None else f"WHERE acknowledged = {int(acknowledged)}"
        with self._connect() as conn:
            return conn.execute(f"SELECT COUNT(*) FROM alerts {where}").fetchone()[0]

    # ── Actions ───────────────────────────────────────────────────────────────

    def acknowledge(self, alert_id: int) -> bool:
        """Marque une alerte comme traitée. Retourne True si trouvée."""
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE alerts SET acknowledged = 1 WHERE id = ?", (alert_id,)
            )
            conn.commit()
        return cur.rowcount > 0

    def acknowledge_all(
        self,
        source:      Optional[str] = None,
        severity:    Optional[str] = None,
        metric_name: Optional[str] = None,
    ) -> int:
        """Marque plusieurs alertes comme traitées. Retourne le nombre modifié."""
        conditions, params = ["acknowledged = 0"], []
        if source:
            conditions.append("source = ?"); params.append(source)
        if severity:
            conditions.append("severity = ?"); params.append(severity)
        if metric_name:
            conditions.append("metric_name = ?"); params.append(metric_name)
        with self._connect() as conn:
            cur = conn.execute(
                f"UPDATE alerts SET acknowledged = 1 WHERE {' AND '.join(conditions)}",
                params,
            )
            conn.commit()
        return cur.rowcount

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> dict:
        d = dict(row)
        d["tags"] = json.loads(d.get("tags") or "{}")
        d["acknowledged"] = bool(d.get("acknowledged", 0))
        return d
