"""
Tests Semaine 9 — Tableau de bord & Intégration

T1  dashboard_empty_db       — génère un HTML valide même sans données
T2  dashboard_with_metrics   — les métriques apparaissent dans le HTML
T3  dashboard_with_alerts    — les alertes apparaissent avec sévérité colorée
T4  dashboard_sections       — toutes les sections attendues sont présentes
T5  dashboard_overwrite       — une 2e génération écrase la première
T6  dag_importable            — le DAG data_trust_monitoring s'importe sans erreur
T7  dashboard_cli_runs        — l'exécution en CLI retourne code 0
T8  full_pipeline_integration — collect → detect → dashboard bout en bout
"""

from __future__ import annotations

import importlib
import importlib.util
import json
import os
import subprocess
import sys
import tempfile

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from spark.metrics.storage          import SQLiteMetricsStore, MetricPoint
from spark.metrics.alert_manager    import AlertManager
from spark.metrics.anomaly_detector import AnomalyDetector, AnomalyAlert
from spark.metrics.dashboard        import generate

results: dict[str, str] = {}

PYTHON = sys.executable


def ok(name: str):
    results[name] = "OK"
    print(f"  ✅  {name:<45} OK")


def fail(name: str, err):
    results[name] = f"FAIL: {err}"
    print(f"  ❌  {name:<45} FAIL: {err}")
    raise AssertionError(err)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_store(db_path: str) -> SQLiteMetricsStore:
    return SQLiteMetricsStore(db_path=db_path)


def _populate(store: SQLiteMetricsStore) -> None:
    pts = [
        MetricPoint(source="spark", name="job.rows_output",       value=48000.0),
        MetricPoint(source="spark", name="job.rejection_rate_pct", value=2.5),
        MetricPoint(source="dbt",   name="run.pass_rate",          value=95.0),
    ]
    store.write(pts)


def _add_alerts(mgr: AlertManager) -> None:
    alerts = [
        AnomalyAlert(
            metric_name="job.rejection_rate_pct", source="spark",
            algorithm="threshold", severity="critical", value=25.0,
            expected="<= 20.0", details="valeur 25 > max autorisé 20",
        ),
        AnomalyAlert(
            metric_name="job.duration_seconds", source="spark",
            algorithm="zscore", severity="warning", value=350.0,
            expected="μ=200 ± 3σ", details="z-score=4.10",
        ),
    ]
    mgr.save(alerts)


# ── Tests ─────────────────────────────────────────────────────────────────────

def t1_dashboard_empty_db():
    with tempfile.TemporaryDirectory() as tmp:
        db   = os.path.join(tmp, "metrics.db")
        out  = os.path.join(tmp, "dashboard.html")
        path = generate(db_path=db, out_path=out)
        assert os.path.isfile(path), "Fichier HTML non créé"
        html = open(path, encoding="utf-8").read()
        assert "Data Trust" in html, "Titre absent"
        assert "<!DOCTYPE html>" in html
    ok("T1-dashboard_empty_db")


def t2_dashboard_with_metrics():
    with tempfile.TemporaryDirectory() as tmp:
        db  = os.path.join(tmp, "metrics.db")
        out = os.path.join(tmp, "dashboard.html")
        store = _make_store(db)
        _populate(store)
        path = generate(db_path=db, out_path=out)
        html = open(path, encoding="utf-8").read()
        assert "job.rows_output"        in html, "métrique spark absente"
        assert "run.pass_rate"          in html, "métrique dbt absente"
        assert "48000"                  in html, "valeur absente"
    ok("T2-dashboard_with_metrics")


def t3_dashboard_with_alerts():
    with tempfile.TemporaryDirectory() as tmp:
        db  = os.path.join(tmp, "metrics.db")
        out = os.path.join(tmp, "dashboard.html")
        store = _make_store(db)
        _populate(store)
        mgr   = AlertManager(db_path=db)
        _add_alerts(mgr)
        path  = generate(db_path=db, out_path=out)
        html  = open(path, encoding="utf-8").read()
        assert "CRITICAL" in html,              "badge critical absent"
        assert "WARNING"  in html,              "badge warning absent"
        assert "rejection_rate_pct" in html,    "métrique alerte absente"
        assert "#e74c3c"  in html,              "couleur critical absente"
    ok("T3-dashboard_with_alerts")


def t4_dashboard_sections():
    with tempfile.TemporaryDirectory() as tmp:
        db  = os.path.join(tmp, "metrics.db")
        out = os.path.join(tmp, "dashboard.html")
        store = _make_store(db)
        _populate(store)
        path = generate(db_path=db, out_path=out)
        html = open(path, encoding="utf-8").read()
        assert "Métriques"          in html, "section métriques absente"
        assert "Alertes"            in html, "section alertes absente"
        assert "Points collectés"   in html, "carte compteur absente"
        assert "Sources actives"    in html, "carte sources absente"
    ok("T4-dashboard_sections")


def t5_dashboard_overwrite():
    with tempfile.TemporaryDirectory() as tmp:
        db  = os.path.join(tmp, "metrics.db")
        out = os.path.join(tmp, "dashboard.html")
        store = _make_store(db)
        _populate(store)
        generate(db_path=db, out_path=out)
        mtime1 = os.path.getmtime(out)

        # Ajouter une métrique et régénérer
        store.write([MetricPoint(source="airflow", name="dag.status", value=1.0)])
        generate(db_path=db, out_path=out)
        html = open(out, encoding="utf-8").read()
        assert "airflow" in html, "2e génération n'a pas écrasé"
    ok("T5-dashboard_overwrite")


def t6_dag_importable():
    dag_path = os.path.join(BASE_DIR, "dags", "data_trust_monitoring.py")
    assert os.path.isfile(dag_path), f"Fichier DAG introuvable : {dag_path}"
    spec   = importlib.util.spec_from_file_location("data_trust_monitoring", dag_path)
    module = importlib.util.module_from_spec(spec)
    # On ne charge pas (requiert airflow.sdk) — vérification syntaxe uniquement
    with open(dag_path, encoding="utf-8") as f:
        source = f.read()
    compile(source, dag_path, "exec")
    assert "data_trust_monitoring" in source, "dag_id absent"
    assert "collect_metrics"       in source, "tâche collect absente"
    assert "detect_anomalies"      in source, "tâche detect absente"
    assert "generate_dashboard"    in source, "tâche dashboard absente"
    ok("T6-dag_importable")


def t7_dashboard_cli_runs():
    with tempfile.TemporaryDirectory() as tmp:
        db  = os.path.join(tmp, "m.db")
        out = os.path.join(tmp, "d.html")
        r = subprocess.run(
            [PYTHON, os.path.join(BASE_DIR, "spark", "metrics", "dashboard.py"),
             "--db", db, "--out", out],
            capture_output=True, text=True,
            env={**os.environ, "PYTHONPATH": BASE_DIR},
        )
        assert r.returncode == 0, f"CLI exit {r.returncode}: {r.stderr[-300:]}"
        assert os.path.isfile(out), "HTML non créé par la CLI"
        assert "Dashboard généré" in r.stdout, "Message de confirmation absent"
    ok("T7-dashboard_cli_runs")


def t8_full_pipeline_integration():
    with tempfile.TemporaryDirectory() as tmp:
        db  = os.path.join(tmp, "metrics.db")
        out = os.path.join(tmp, "dashboard.html")

        # 1 — Écrire des métriques simulant plusieurs runs
        store = _make_store(db)
        for i, v in enumerate([45000, 46000, 47000, 48000, 49000, 75000]):
            from datetime import datetime, timezone, timedelta
            ts = (datetime.now(timezone.utc) - timedelta(days=5 - i)).isoformat()
            store.write([MetricPoint(source="spark", name="job.rows_output", value=float(v), ts=ts)])
        store.write([MetricPoint(source="spark", name="job.rejection_rate_pct", value=25.0)])

        # 2 — Détection
        detector = AnomalyDetector(store)
        alerts   = detector.run()
        mgr      = AlertManager(db_path=db)
        n_saved  = mgr.save(alerts)

        # 3 — Dashboard
        path = generate(db_path=db, out_path=out)
        html = open(path, encoding="utf-8").read()

        assert os.path.isfile(path),     "Dashboard non généré"
        assert n_saved > 0,              "Aucune alerte détectée ni sauvegardée"
        assert "job.rows_output" in html, "Métrique absente du dashboard"
    ok("T8-full_pipeline_integration")


# ── Runner ────────────────────────────────────────────────────────────────────

TESTS = [
    ("T1", "dashboard_empty_db",       t1_dashboard_empty_db),
    ("T2", "dashboard_with_metrics",   t2_dashboard_with_metrics),
    ("T3", "dashboard_with_alerts",    t3_dashboard_with_alerts),
    ("T4", "dashboard_sections",       t4_dashboard_sections),
    ("T5", "dashboard_overwrite",      t5_dashboard_overwrite),
    ("T6", "dag_importable",           t6_dag_importable),
    ("T7", "dashboard_cli_runs",       t7_dashboard_cli_runs),
    ("T8", "full_pipeline_integration",t8_full_pipeline_integration),
]

if __name__ == "__main__":
    print("\n" + "─" * 60)
    print("  Tests Semaine 9 — Dashboard & Intégration")
    print("─" * 60)
    for tid, name, fn in TESTS:
        print(f"{tid} — {name}")
    print()

    n_ok = n_fail = 0
    for tid, name, fn in TESTS:
        try:
            fn()
            n_ok += 1
        except Exception as exc:
            fail(f"{tid}-{name}", exc)
            n_fail += 1

    print()
    print("─" * 60)
    print(f"  Résultat : {n_ok}/{len(TESTS)} tests réussis")
    if n_fail:
        print(f"  ❌ {n_fail} ÉCHEC(S)")
        sys.exit(1)
    else:
        print("  ✅ Tous les tests passent")
    print("─" * 60)
