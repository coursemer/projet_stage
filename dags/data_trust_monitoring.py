"""
DAG – Data Trust Monitoring (quotidien)

Collecte les métriques de toutes les sources, détecte les anomalies et
génère un tableau de bord HTML après chaque run du pipeline principal.

Flow :
    collect_metrics → detect_anomalies → generate_dashboard → print_alert_summary

Schedule : 07:00 UTC (après sales_analytics_daily à 06:00)
"""

import os
from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT_DIR  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
METRICS_CLI  = os.path.join(PROJECT_DIR, "spark", "metrics", "run_collector.py")
DASHBOARD_PY = os.path.join(PROJECT_DIR, "spark", "metrics", "dashboard.py")
DB_PATH      = os.path.join(PROJECT_DIR, "spark", "data", "metrics.db")
DASH_PATH    = os.path.join(PROJECT_DIR, "spark", "data", "dashboard.html")

default_args = {
    "owner":           "data-trust",
    "depends_on_past": False,
    "retries":         1,
    "retry_delay":     timedelta(minutes=3),
}

with DAG(
    dag_id="data_trust_monitoring",
    default_args=default_args,
    description="Collecte métriques + détection anomalies + dashboard HTML",
    schedule="0 7 * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["monitoring", "data-trust", "alerts"],
) as dag:

    # ── ÉTAPE 1 : Collecte des métriques ─────────────────────────────────────
    collect_metrics = BashOperator(
        task_id="collect_metrics",
        bash_command=(
            f"python {METRICS_CLI} "
            f"--db {DB_PATH} "
            "--source all "
            "--json"
        ),
    )

    # ── ÉTAPE 2 : Détection des anomalies ────────────────────────────────────
    detect_anomalies = BashOperator(
        task_id="detect_anomalies",
        bash_command=(
            f"python {METRICS_CLI} "
            f"--db {DB_PATH} "
            "--source all "
            "--detect "
            "--json"
        ),
    )

    # ── ÉTAPE 3 : Génération du dashboard HTML ────────────────────────────────
    generate_dashboard = BashOperator(
        task_id="generate_dashboard",
        bash_command=(
            f"python {DASHBOARD_PY} "
            f"--db {DB_PATH} "
            f"--out {DASH_PATH}"
        ),
    )

    # ── ÉTAPE 4 : Résumé des alertes (log Airflow) ────────────────────────────
    def _print_alert_summary(**context):
        import sys
        sys.path.insert(0, PROJECT_DIR)
        from spark.metrics.alert_manager import AlertManager
        mgr = AlertManager(db_path=DB_PATH)
        s   = mgr.summary()
        print(f"\n{'─'*55}")
        print(f"  Data Trust — Résumé des alertes")
        print(f"  Total : {s['total']}  |  Non traitées : {s['unacknowledged']}")
        print(f"{'─'*55}")
        for row in s["by_severity"]:
            icons = {"critical": "🔴", "warning": "🟡", "info": "🔵"}
            icon  = icons.get(row["severity"], "⚪")
            print(f"  {icon} {row['severity']:<10} {row['count']}")
        if s["top_metrics"]:
            print(f"\n  Top métriques alertées :")
            for row in s["top_metrics"][:5]:
                print(f"    {row['source']}/{row['metric_name']} — {row['count']} alertes")
        print(f"{'─'*55}")
        print(f"  Dashboard : {DASH_PATH}")

    print_alert_summary = PythonOperator(
        task_id="print_alert_summary",
        python_callable=_print_alert_summary,
    )

    # ── DAG flow ──────────────────────────────────────────────────────────────
    collect_metrics >> detect_anomalies >> generate_dashboard >> print_alert_summary
