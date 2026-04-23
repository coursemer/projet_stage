"""
Tests — Semaine 7 : Collecte & Centralisation des métriques

T1 : storage_sqlite        — écriture/lecture SQLite, index, schéma
T2 : storage_query_filters — filtres source/name/ts/limit
T3 : storage_summary       — agrégats (count, avg, min, max, last_value)
T4 : spark_emitter         — SparkMetricsEmitter écrit JSON + persiste dans SQLite
T5 : spark_collector       — SparkCollector lit les fichiers JSON
T6 : dbt_collector         — DbtCollector parse run_results.json
T7 : airflow_collector     — AirflowCollector sur une DB simulée
T8 : run_collector_cli     — CLI run_collector.py --json
T9 : api_endpoints         — FastAPI health / metrics / summary / push / history
"""

import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from spark.metrics.storage   import MetricPoint, SQLiteMetricsStore
from spark.metrics.collector import (
    AirflowCollector, SparkCollector, DbtCollector, SparkMetricsEmitter,
)

PYTHON = sys.executable
results: dict[str, str] = {}


# ══════════════════════════════════════════════════════════════════════════════
# T1 — SQLite : écriture / lecture de base
# ══════════════════════════════════════════════════════════════════════════════

print("=" * 60)
print("T1 — storage_sqlite")
print("=" * 60)

try:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    store = SQLiteMetricsStore(db_path=db_path)
    assert store.count() == 0, "Base vide initialement"

    pts = [
        MetricPoint(source="spark",   name="ingest.rows_output",    value=48_000),
        MetricPoint(source="spark",   name="ingest.duration_seconds", value=12.5),
        MetricPoint(source="airflow", name="dag.duration_seconds",    value=30.0,
                    tags={"dag_id": "sales_analytics"}),
        MetricPoint(source="dbt",     name="model.execution_time",    value=2.3,
                    tags={"node": "stg_sales"}),
        MetricPoint(source="dbt",     name="run.pass_rate",           value=100.0),
    ]
    n = store.write(pts)
    assert n == 5, f"5 points attendus, obtenu {n}"
    assert store.count() == 5

    rows = store.query()
    assert len(rows) == 5
    for r in rows:
        assert "ts" in r and "source" in r and "name" in r and "value" in r
        assert isinstance(r["tags"],  dict)
        assert isinstance(r["extra"], dict)

    print(f"  {n} points insérés  ✓")
    print(f"  Schéma des rows vérifié  ✓")
    results["T1-storage_sqlite"] = "OK"
except Exception as e:
    import traceback; traceback.print_exc()
    results["T1-storage_sqlite"] = "FAILED"
finally:
    os.unlink(db_path)


# ══════════════════════════════════════════════════════════════════════════════
# T2 — SQLite : filtres de requête
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 60)
print("T2 — storage_query_filters")
print("=" * 60)

try:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    store = SQLiteMetricsStore(db_path=db_path)
    store.write([
        MetricPoint(source="spark",   name="job.rows_output",      value=1000),
        MetricPoint(source="spark",   name="job.duration_seconds",  value=5.0),
        MetricPoint(source="airflow", name="dag.status",            value=1.0),
        MetricPoint(source="dbt",     name="model.execution_time",  value=3.3),
    ])

    # Filtre par source
    spark_rows = store.query(source="spark")
    assert len(spark_rows) == 2, f"2 points spark attendus, obtenu {len(spark_rows)}"
    assert all(r["source"] == "spark" for r in spark_rows)

    # Filtre par name
    dur_rows = store.query(name="job.duration_seconds")
    assert len(dur_rows) == 1

    # Filtre limit
    limited = store.query(limit=2)
    assert len(limited) == 2

    # Sources disponibles
    srcs = store.sources()
    assert set(srcs) == {"spark", "airflow", "dbt"}

    print(f"  Filtre source=spark       : {len(spark_rows)} résultats  ✓")
    print(f"  Filtre name=dur_seconds   : {len(dur_rows)} résultats  ✓")
    print(f"  Filtre limit=2            : {len(limited)} résultats  ✓")
    print(f"  Sources : {srcs}  ✓")
    results["T2-storage_query_filters"] = "OK"
except Exception as e:
    import traceback; traceback.print_exc()
    results["T2-storage_query_filters"] = "FAILED"
finally:
    os.unlink(db_path)


# ══════════════════════════════════════════════════════════════════════════════
# T3 — SQLite : summary
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 60)
print("T3 — storage_summary")
print("=" * 60)

try:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    store = SQLiteMetricsStore(db_path=db_path)
    store.write([
        MetricPoint(source="spark", name="job.rows_output", value=1000),
        MetricPoint(source="spark", name="job.rows_output", value=2000),
        MetricPoint(source="spark", name="job.rows_output", value=3000),
        MetricPoint(source="dbt",   name="run.pass_rate",   value=90.0),
        MetricPoint(source="dbt",   name="run.pass_rate",   value=95.0),
    ])

    summary = store.summary()
    assert len(summary) == 2, f"2 métriques distinctes attendues, obtenu {len(summary)}"

    rows_metric = next(r for r in summary if r["name"] == "job.rows_output")
    assert rows_metric["count"] == 3
    assert rows_metric["avg_value"] == 2000.0
    assert rows_metric["min_value"] == 1000.0
    assert rows_metric["max_value"] == 3000.0
    assert rows_metric["last_value"] == 3000.0

    print(f"  {len(summary)} métriques dans le summary  ✓")
    print(f"  avg={rows_metric['avg_value']}, min={rows_metric['min_value']}, "
          f"max={rows_metric['max_value']}  ✓")
    results["T3-storage_summary"] = "OK"
except Exception as e:
    import traceback; traceback.print_exc()
    results["T3-storage_summary"] = "FAILED"
finally:
    os.unlink(db_path)


# ══════════════════════════════════════════════════════════════════════════════
# T4 — SparkMetricsEmitter
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 60)
print("T4 — spark_emitter")
print("=" * 60)

try:
    tmpdir = tempfile.mkdtemp()
    metrics_dir = os.path.join(tmpdir, "metrics")
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    store = SQLiteMetricsStore(db_path=db_path)

    with SparkMetricsEmitter("ingest_sales", run_date="2026-04-08",
                             metrics_dir=metrics_dir, store=store) as em:
        em.record(rows_input=50_000)
        time.sleep(0.01)   # durée non nulle
        em.record(rows_output=48_000, rows_rejected=2_000)

    # Vérifier le fichier JSON
    json_path = os.path.join(metrics_dir, "ingest_sales", "2026-04-08.json")
    assert os.path.exists(json_path), "Fichier JSON non créé"
    with open(json_path) as f:
        data = json.load(f)
    assert data["rows_input"]    == 50_000
    assert data["rows_output"]   == 48_000
    assert data["rows_rejected"] == 2_000
    assert data["status"]        == "ok"
    assert data["duration_seconds"] > 0

    # Vérifier la persistance SQLite
    rows = store.query(source="spark")
    assert len(rows) > 0, "Aucun point Spark dans SQLite"
    names = {r["name"] for r in rows}
    assert "job.rows_output" in names
    assert "job.rejection_rate_pct" in names

    print(f"  JSON créé : {json_path}  ✓")
    print(f"  rows_input={data['rows_input']:,}  rows_output={data['rows_output']:,}  "
          f"rejection={data['rows_rejected']:,}  ✓")
    print(f"  {len(rows)} points dans SQLite, noms: {sorted(names)}  ✓")
    results["T4-spark_emitter"] = "OK"
except Exception as e:
    import traceback; traceback.print_exc()
    results["T4-spark_emitter"] = "FAILED"
finally:
    shutil.rmtree(tmpdir, ignore_errors=True)
    os.unlink(db_path)


# ══════════════════════════════════════════════════════════════════════════════
# T5 — SparkCollector : lecture des fichiers JSON
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 60)
print("T5 — spark_collector")
print("=" * 60)

try:
    tmpdir = tempfile.mkdtemp()
    metrics_dir = os.path.join(tmpdir, "metrics")

    # Créer des fichiers de métriques simulés
    jobs = [
        ("ingest_sales",    {"run_date": "2026-04-08", "rows_input": 50000, "rows_output": 48000,
                              "rows_rejected": 2000, "duration_seconds": 12.5, "status": "ok",
                              "ts": "2026-04-08T10:00:00+00:00"}),
        ("clean_sales",     {"run_date": "2026-04-08", "rows_input": 48000, "rows_output": 47500,
                              "rows_rejected": 500,  "duration_seconds": 8.2,  "status": "ok",
                              "ts": "2026-04-08T10:15:00+00:00"}),
        ("aggregate_sales", {"run_date": "2026-04-08", "rows_input": 47500, "rows_output": 150,
                              "rows_rejected": 0,    "duration_seconds": 4.1,  "status": "ok",
                              "ts": "2026-04-08T10:25:00+00:00"}),
    ]
    for job_name, data in jobs:
        job_dir = os.path.join(metrics_dir, job_name)
        os.makedirs(job_dir, exist_ok=True)
        with open(os.path.join(job_dir, "2026-04-08.json"), "w") as f:
            json.dump(data, f)

    collector = SparkCollector(metrics_dir=metrics_dir)
    points    = collector.collect()

    assert len(points) > 0, "Aucun point collecté"
    sources = {p.source for p in points}
    assert sources == {"spark"}
    names = {p.name for p in points}
    assert "job.duration_seconds" in names
    assert "job.rows_output"      in names
    assert "job.rejection_rate_pct" in names

    print(f"  {len(points)} points collectés depuis {len(jobs)} jobs  ✓")
    print(f"  Métriques : {sorted(names)}  ✓")
    results["T5-spark_collector"] = "OK"
except Exception as e:
    import traceback; traceback.print_exc()
    results["T5-spark_collector"] = "FAILED"
finally:
    shutil.rmtree(tmpdir, ignore_errors=True)


# ══════════════════════════════════════════════════════════════════════════════
# T6 — DbtCollector
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 60)
print("T6 — dbt_collector")
print("=" * 60)

try:
    tmpdir = tempfile.mkdtemp()
    results_path = os.path.join(tmpdir, "run_results.json")

    run_results = {
        "metadata": {"generated_at": "2026-04-08T10:30:00+00:00"},
        "results": [
            {"unique_id": "model.project.stg_sales",         "status": "success", "execution_time": 1.23},
            {"unique_id": "model.project.int_sales_daily",   "status": "success", "execution_time": 2.45},
            {"unique_id": "model.project.sales_summary",     "status": "success", "execution_time": 0.87},
            {"unique_id": "test.project.not_null_order_id",  "status": "pass",    "execution_time": 0.12},
            {"unique_id": "test.project.unique_order_id",    "status": "pass",    "execution_time": 0.09},
            {"unique_id": "test.project.accepted_values_channel", "status": "fail", "execution_time": 0.05},
        ],
    }
    with open(results_path, "w") as f:
        json.dump(run_results, f)

    collector = DbtCollector(results_path=results_path)
    points    = collector.collect()

    assert len(points) > 0
    names = {p.name for p in points}
    assert "model.execution_time" in names
    assert "model.status"         in names
    assert "test.status"          in names
    assert "run.pass_rate"        in names

    model_pts = [p for p in points if p.name == "model.status"]
    assert all(p.value == 1.0 for p in model_pts), "Tous les modèles doivent être success"

    pass_rate_pt = next(p for p in points if p.name == "run.pass_rate")
    # 5 success/pass, 1 fail → 5/6 * 100 ≈ 83.33
    assert 80 <= pass_rate_pt.value <= 90, f"Pass rate inattendu: {pass_rate_pt.value}"

    print(f"  {len(points)} points collectés  ✓")
    print(f"  Métriques : {sorted(names)}  ✓")
    print(f"  Pass rate : {pass_rate_pt.value}%  ✓")
    results["T6-dbt_collector"] = "OK"
except Exception as e:
    import traceback; traceback.print_exc()
    results["T6-dbt_collector"] = "FAILED"
finally:
    shutil.rmtree(tmpdir, ignore_errors=True)


# ══════════════════════════════════════════════════════════════════════════════
# T7 — AirflowCollector (DB simulée)
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 60)
print("T7 — airflow_collector")
print("=" * 60)

try:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        airflow_db = f.name

    # Créer une mini base Airflow simulée
    conn = sqlite3.connect(airflow_db)
    conn.execute("""
        CREATE TABLE dag_run (
            dag_id TEXT, run_id TEXT,
            execution_date TEXT, start_date TEXT, end_date TEXT, state TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE task_instance (
            dag_id TEXT, task_id TEXT, execution_date TEXT,
            start_date TEXT, end_date TEXT, duration REAL,
            state TEXT, try_number INTEGER
        )
    """)
    conn.executemany(
        "INSERT INTO dag_run VALUES (?,?,?,?,?,?)",
        [
            ("sales_analytics", "run1", "2026-04-08",
             "2026-04-08T10:00:00", "2026-04-08T10:05:30", "success"),
            ("customer_360", "run2", "2026-04-08",
             "2026-04-08T11:00:00", "2026-04-08T11:08:00", "success"),
            ("inventory_scd2", "run3", "2026-04-08",
             "2026-04-08T12:00:00", "2026-04-08T12:03:00", "failed"),
        ],
    )
    conn.executemany(
        "INSERT INTO task_instance VALUES (?,?,?,?,?,?,?,?)",
        [
            ("sales_analytics", "ingest_sales_raw", "2026-04-08",
             "2026-04-08T10:00:00", "2026-04-08T10:00:45", 45.0, "success", 1),
            ("sales_analytics", "clean_and_validate_sales", "2026-04-08",
             "2026-04-08T10:01:00", "2026-04-08T10:02:30", 90.0, "success", 1),
            ("inventory_scd2", "snapshot_inventory", "2026-04-08",
             "2026-04-08T12:00:00", "2026-04-08T12:02:00", 120.0, "failed", 3),
        ],
    )
    conn.commit()
    conn.close()

    collector = AirflowCollector(db_path=airflow_db)
    points    = collector.collect()

    assert len(points) > 0, "Aucun point collecté depuis la DB Airflow simulée"
    names = {p.name for p in points}
    assert "dag.duration_seconds" in names
    assert "dag.status"           in names
    assert "task.duration_seconds" in names
    assert "task.try_number"      in names

    # Vérifier les durées des DagRuns
    dag_dur = [p for p in points if p.name == "dag.duration_seconds"]
    assert len(dag_dur) == 3
    durations = sorted(p.value for p in dag_dur)
    assert abs(durations[0] - 180.0) < 1   # inventory_scd2 : 3 min
    assert abs(durations[1] - 330.0) < 1   # sales_analytics : 5m30s
    assert abs(durations[2] - 480.0) < 1   # customer_360 : 8 min

    print(f"  {len(points)} points collectés  ✓")
    print(f"  Métriques : {sorted(names)}  ✓")
    print(f"  Durées DagRun (s) : {[round(p.value) for p in dag_dur]}  ✓")
    results["T7-airflow_collector"] = "OK"
except Exception as e:
    import traceback; traceback.print_exc()
    results["T7-airflow_collector"] = "FAILED"
finally:
    os.unlink(airflow_db)


# ══════════════════════════════════════════════════════════════════════════════
# T8 — CLI run_collector.py
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 60)
print("T8 — run_collector_cli")
print("=" * 60)

try:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    tmpdir      = tempfile.mkdtemp()
    metrics_dir = os.path.join(tmpdir, "metrics")

    # Créer des fichiers Spark pour que le collecteur ait quelque chose à lire
    job_dir = os.path.join(metrics_dir, "ingest_test")
    os.makedirs(job_dir, exist_ok=True)
    with open(os.path.join(job_dir, "2026-04-08.json"), "w") as f:
        json.dump({"run_date": "2026-04-08", "rows_input": 1000, "rows_output": 950,
                   "rows_rejected": 50, "duration_seconds": 3.0, "status": "ok",
                   "ts": "2026-04-08T10:00:00+00:00"}, f)

    # Patcher temporairement METRICS_DIR via une variable d'env n'est pas pratique ;
    # on teste uniquement que le CLI répond correctement en JSON (sans crash).
    cli = os.path.join(BASE_DIR, "spark", "metrics", "run_collector.py")
    r = subprocess.run(
        [PYTHON, cli, "--db", db_path, "--json", "--source", "spark"],
        capture_output=True, text=True, env={**os.environ, "PYTHONPATH": BASE_DIR},
    )
    assert r.returncode == 0, f"CLI a échoué (code {r.returncode})\n{r.stderr[-500:]}"
    output = json.loads(r.stdout.strip())
    assert "collected" in output and "total" in output
    print(f"  CLI exit code 0  ✓")
    print(f"  JSON output : {output}  ✓")
    results["T8-run_collector_cli"] = "OK"
except Exception as e:
    import traceback; traceback.print_exc()
    results["T8-run_collector_cli"] = "FAILED"
finally:
    os.unlink(db_path)
    shutil.rmtree(tmpdir, ignore_errors=True)


# ══════════════════════════════════════════════════════════════════════════════
# T9 — API FastAPI (test client sans serveur réseau)
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 60)
print("T9 — api_endpoints")
print("=" * 60)

try:
    try:
        from fastapi.testclient import TestClient
    except ImportError:
        raise RuntimeError("fastapi[testclient] non installé — T9 ignoré")

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    from spark.metrics.api import create_app
    app    = create_app(db_path=db_path)
    client = TestClient(app)

    # ── /health ───────────────────────────────────────────────────────────────
    r = client.get("/api/v1/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["total_metrics"] == 0
    print(f"  GET /health            200  ✓  total_metrics={body['total_metrics']}")

    # ── POST /push ────────────────────────────────────────────────────────────
    payload = [
        {"source": "spark",   "name": "ingest.rows_output",    "value": 48000},
        {"source": "airflow", "name": "dag.duration_seconds",  "value": 330.0,
         "tags": {"dag_id": "sales_analytics"}},
        {"source": "dbt",     "name": "run.pass_rate",         "value": 100.0},
    ]
    r = client.post("/api/v1/metrics/push", json=payload)
    assert r.status_code == 201
    assert r.json()["inserted"] == 3
    print(f"  POST /push             201  ✓  inserted=3")

    # ── GET /metrics ──────────────────────────────────────────────────────────
    r = client.get("/api/v1/metrics")
    assert r.status_code == 200
    assert r.json()["count"] == 3
    print(f"  GET /metrics           200  ✓  count=3")

    # ── GET /metrics?source=spark ─────────────────────────────────────────────
    r = client.get("/api/v1/metrics?source=spark")
    assert r.status_code == 200
    assert r.json()["count"] == 1
    print(f"  GET /metrics?source=spark  200  ✓  count=1")

    # ── GET /sources ──────────────────────────────────────────────────────────
    r = client.get("/api/v1/metrics/sources")
    assert r.status_code == 200
    assert set(r.json()["sources"]) == {"spark", "airflow", "dbt"}
    print(f"  GET /sources           200  ✓  sources={r.json()['sources']}")

    # ── GET /summary ──────────────────────────────────────────────────────────
    r = client.get("/api/v1/metrics/summary")
    assert r.status_code == 200
    assert r.json()["count"] == 3
    print(f"  GET /summary           200  ✓  count=3")

    # ── GET /history/{name} ───────────────────────────────────────────────────
    r = client.get("/api/v1/metrics/history/ingest.rows_output")
    assert r.status_code == 200
    body = r.json()
    assert body["name"]  == "ingest.rows_output"
    assert body["count"] == 1
    assert body["series"][0]["value"] == 48000
    print(f"  GET /history/ingest.rows_output  200  ✓  value=48000")

    # ── GET /history 404 ─────────────────────────────────────────────────────
    r = client.get("/api/v1/metrics/history/non.existent.metric")
    assert r.status_code == 404
    print(f"  GET /history/non.existent  404  ✓")

    # ── POST /push vide ───────────────────────────────────────────────────────
    r = client.post("/api/v1/metrics/push", json=[])
    assert r.status_code == 400
    print(f"  POST /push []          400  ✓")

    results["T9-api_endpoints"] = "OK"
except Exception as e:
    import traceback; traceback.print_exc()
    results["T9-api_endpoints"] = "FAILED"
finally:
    try:
        os.unlink(db_path)
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# RÉSUMÉ
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 60)
print("RÉSUMÉ")
print("=" * 60)
all_ok = True
for label, status in results.items():
    icon = "✅" if status == "OK" else "❌"
    print(f"  {icon}  {label:<35} {status}")
    if status != "OK":
        all_ok = False
print("=" * 60)
print("RÉSULTAT GLOBAL :", "✅ TOUS OK" if all_ok else "❌ ÉCHECS DÉTECTÉS")
