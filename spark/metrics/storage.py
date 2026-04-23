"""
Stockage time-series des métriques pipeline.

Backend principal : SQLite (toujours disponible, aucune dépendance externe).
Backend optionnel : InfluxDB 2.x via influxdb-client (si Docker est démarré).

Schema SQLite :
    metric_points(id, ts, source, name, tags_json, value, extra_json)

Usage :
    store = get_store()                          # SQLite par défaut
    store = get_store(backend="influxdb")        # InfluxDB (Docker requis)

    pt = MetricPoint(source="spark", name="ingest_sales.rows_output", value=48000)
    store.write([pt])
    rows = store.query(source="spark", limit=100)
"""

from __future__ import annotations

import json
import os
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DEFAULT_DB = os.path.join(BASE_DIR, "spark", "data", "metrics.db")

# InfluxDB defaults (docker-compose)
INFLUX_URL    = os.environ.get("INFLUX_URL",    "http://localhost:8086")
INFLUX_TOKEN  = os.environ.get("INFLUX_TOKEN",  "data-trust-token-2026")
INFLUX_ORG    = os.environ.get("INFLUX_ORG",    "data-trust")
INFLUX_BUCKET = os.environ.get("INFLUX_BUCKET", "metrics")


@dataclass
class MetricPoint:
    """Un point de métrique horodaté."""
    source: str                          # 'airflow' | 'spark' | 'dbt'
    name:   str                          # ex. 'ingest_sales.duration_seconds'
    value:  float
    ts:     str          = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    tags:   dict         = field(default_factory=dict)   # {dag_id: ..., job: ...}
    extra:  dict         = field(default_factory=dict)   # champs libres


# ── SQLite ────────────────────────────────────────────────────────────────────

class SQLiteMetricsStore:
    """Stockage time-series dans une base SQLite locale."""

    def __init__(self, db_path: str = DEFAULT_DB):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS metric_points (
                    id       INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts       TEXT    NOT NULL,
                    source   TEXT    NOT NULL,
                    name     TEXT    NOT NULL,
                    tags     TEXT    DEFAULT '{}',
                    value    REAL,
                    extra    TEXT    DEFAULT '{}'
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_source ON metric_points(source)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_name   ON metric_points(name)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ts     ON metric_points(ts)")
            conn.commit()

    # ── Écriture ──────────────────────────────────────────────────────────────

    def write(self, points: list[MetricPoint]) -> int:
        """Insère une liste de MetricPoints. Retourne le nombre insérés."""
        if not points:
            return 0
        rows = [
            (p.ts, p.source, p.name, json.dumps(p.tags), p.value, json.dumps(p.extra))
            for p in points
        ]
        with self._connect() as conn:
            conn.executemany(
                "INSERT INTO metric_points(ts,source,name,tags,value,extra) VALUES(?,?,?,?,?,?)",
                rows,
            )
            conn.commit()
        return len(points)

    # ── Lecture ───────────────────────────────────────────────────────────────

    def query(
        self,
        source:  Optional[str] = None,
        name:    Optional[str] = None,
        from_ts: Optional[str] = None,
        to_ts:   Optional[str] = None,
        limit:   int = 500,
    ) -> list[dict]:
        """Retourne des points filtrés, triés par ts décroissant."""
        conditions, params = [], []
        if source:
            conditions.append("source = ?"); params.append(source)
        if name:
            conditions.append("name = ?"); params.append(name)
        if from_ts:
            conditions.append("ts >= ?"); params.append(from_ts)
        if to_ts:
            conditions.append("ts <= ?"); params.append(to_ts)

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        sql = f"SELECT * FROM metric_points {where} ORDER BY ts DESC LIMIT ?"
        params.append(limit)

        with self._connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def summary(self) -> list[dict]:
        """Dernière valeur de chaque (source, name) + stats."""
        sql = """
            SELECT source, name,
                   COUNT(*)        AS count,
                   AVG(value)      AS avg_value,
                   MIN(value)      AS min_value,
                   MAX(value)      AS max_value,
                   MAX(ts)         AS last_ts,
                   (SELECT value FROM metric_points m2
                    WHERE m2.source=m1.source AND m2.name=m1.name
                    ORDER BY ts DESC LIMIT 1) AS last_value
            FROM metric_points m1
            GROUP BY source, name
            ORDER BY source, name
        """
        with self._connect() as conn:
            rows = conn.execute(sql).fetchall()
        return [dict(r) for r in rows]

    def sources(self) -> list[str]:
        with self._connect() as conn:
            rows = conn.execute("SELECT DISTINCT source FROM metric_points ORDER BY source").fetchall()
        return [r[0] for r in rows]

    def count(self) -> int:
        with self._connect() as conn:
            return conn.execute("SELECT COUNT(*) FROM metric_points").fetchone()[0]

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> dict:
        d = dict(row)
        d["tags"]  = json.loads(d.get("tags")  or "{}")
        d["extra"] = json.loads(d.get("extra") or "{}")
        return d


# ── InfluxDB (optionnel) ──────────────────────────────────────────────────────

class InfluxDBMetricsStore:
    """
    Export vers InfluxDB 2.x (service Docker).
    Nécessite : pip install influxdb-client
    Si InfluxDB n'est pas joignable, lève une RuntimeError explicite.
    """

    def __init__(
        self,
        url:    str = INFLUX_URL,
        token:  str = INFLUX_TOKEN,
        org:    str = INFLUX_ORG,
        bucket: str = INFLUX_BUCKET,
    ):
        try:
            from influxdb_client import InfluxDBClient, Point  # type: ignore[import]
            from influxdb_client.client.write_api import SYNCHRONOUS  # type: ignore[import]
        except ImportError as exc:
            raise RuntimeError(
                "influxdb-client non installé. "
                "Exécutez : pip install influxdb-client"
            ) from exc

        self._client   = InfluxDBClient(url=url, token=token, org=org)
        self._write_api = self._client.write_api(write_options=SYNCHRONOUS)
        self._query_api = self._client.query_api()
        self._bucket    = bucket
        self._org       = org
        self._Point     = Point

    def write(self, points: list[MetricPoint]) -> int:
        records = []
        for p in points:
            pt = (
                self._Point(p.name)
                .tag("source", p.source)
                .field("value", p.value)
                .time(p.ts)
            )
            for k, v in p.tags.items():
                pt = pt.tag(k, str(v))
            for k, v in p.extra.items():
                if isinstance(v, (int, float)):
                    pt = pt.field(k, v)
            records.append(pt)
        self._write_api.write(bucket=self._bucket, org=self._org, record=records)
        return len(records)

    def query(
        self,
        source:  Optional[str] = None,
        name:    Optional[str] = None,
        from_ts: Optional[str] = None,
        to_ts:   Optional[str] = None,
        limit:   int = 500,
    ) -> list[dict]:
        range_start = from_ts or "-30d"
        range_stop  = to_ts   or "now()"
        flux = f'''
            from(bucket: "{self._bucket}")
              |> range(start: {range_start}, stop: {range_stop})
        '''
        if name:
            flux += f'  |> filter(fn: (r) => r._measurement == "{name}")\n'
        if source:
            flux += f'  |> filter(fn: (r) => r.source == "{source}")\n'
        flux += f'  |> limit(n: {limit})\n'

        tables = self._query_api.query(flux, org=self._org)
        results = []
        for table in tables:
            for record in table.records:
                results.append({
                    "ts":     record.get_time().isoformat(),
                    "source": record.values.get("source", ""),
                    "name":   record.get_measurement(),
                    "value":  record.get_value(),
                    "tags":   {},
                    "extra":  {},
                })
        return results


# ── Factory ───────────────────────────────────────────────────────────────────

def get_store(
    backend: str = "sqlite",
    db_path: str = DEFAULT_DB,
    **kwargs: Any,
) -> SQLiteMetricsStore | InfluxDBMetricsStore:
    """
    Retourne le store selon le backend demandé.

    backend = 'sqlite'   → SQLiteMetricsStore (défaut, sans dépendance)
    backend = 'influxdb' → InfluxDBMetricsStore (nécessite Docker)
    """
    if backend == "influxdb":
        return InfluxDBMetricsStore(**kwargs)
    return SQLiteMetricsStore(db_path=db_path)
