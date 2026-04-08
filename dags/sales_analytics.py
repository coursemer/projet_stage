"""
Pipeline 1 – Sales Analytics (batch quotidien)
Schedule : every day at 06:00 UTC
Flow     : Ingestion → Nettoyage → Agrégations → Quality Check

Étape 1 – ingest_sales_raw      : Spark lit les CSV bruts, applique le schéma, écrit Parquet staging
Étape 2 – clean_and_validate    : Spark nettoie, Pandera valide, écrit Parquet staging_clean
Étape 3 – aggregate_sales       : Spark calcule les KPIs quotidiens, écrit Parquet mart
Étape 4 – dbt_staging           : dbt run stg_sales (vue SQL)
Étape 5 – dbt_intermediate      : dbt run int_sales_daily (vue SQL)
Étape 6 – dbt_mart              : dbt run sales_summary (table SQL)
Étape 7 – quality_check         : dbt test sur toute la chaîne sales
"""

import os
from datetime import datetime, timedelta
from airflow.sdk import DAG
from airflow.operators.bash import BashOperator

# ── Paths ─────────────────────────────────────────────────────────────────────
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SPARK_JOBS  = os.path.join(PROJECT_DIR, "spark", "jobs")
DBT_DIR     = os.path.join(PROJECT_DIR, "dbt")

# ── Default args ──────────────────────────────────────────────────────────────
default_args = {
    "owner":           "data-team",
    "depends_on_past": False,
    "retries":         2,
    "retry_delay":     timedelta(minutes=5),
}

# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="sales_analytics_daily",
    default_args=default_args,
    description="Batch quotidien – ingestion, nettoyage, agrégations des ventes",
    schedule="0 6 * * *",          # Airflow 3.x: use 'schedule' not 'schedule_interval'
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["sales", "pipeline-1", "batch"],
) as dag:

    # ── ÉTAPE 1 : Ingestion ───────────────────────────────────────────────────
    ingest_sales_raw = BashOperator(
        task_id="ingest_sales_raw",
        bash_command=(
            f"python {SPARK_JOBS}/ingest_sales.py "
            "--date {{ ds }} "
            f"--source {PROJECT_DIR}/spark/data/raw/sales "
            f"--dest   {PROJECT_DIR}/spark/data/staging/sales"
        ),
    )

    # ── ÉTAPE 2 : Nettoyage + validation Pandera ──────────────────────────────
    clean_and_validate = BashOperator(
        task_id="clean_and_validate_sales",
        bash_command=(
            f"python {SPARK_JOBS}/clean_sales.py "
            "--date {{ ds }} "
            f"--source {PROJECT_DIR}/spark/data/staging/sales "
            f"--dest   {PROJECT_DIR}/spark/data/staging/sales_clean"
        ),
    )

    # ── ÉTAPE 3 : Agrégations Spark ───────────────────────────────────────────
    aggregate_sales = BashOperator(
        task_id="aggregate_sales",
        bash_command=(
            f"python {SPARK_JOBS}/aggregate_sales.py "
            "--date {{ ds }} "
            f"--source {PROJECT_DIR}/spark/data/staging/sales_clean "
            f"--dest   {PROJECT_DIR}/spark/data/marts/sales"
        ),
    )

    # ── ÉTAPE 4 : dbt – staging ───────────────────────────────────────────────
    dbt_staging = BashOperator(
        task_id="dbt_run_stg_sales",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt run --select staging.stg_sales "
            "--vars '{\"run_date\": \"{{ ds }}\"}'"
        ),
    )

    # ── ÉTAPE 5 : dbt – intermediate ──────────────────────────────────────────
    dbt_intermediate = BashOperator(
        task_id="dbt_run_int_sales_daily",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt run --select intermediate.int_sales_daily "
            "--vars '{\"run_date\": \"{{ ds }}\"}'"
        ),
    )

    # ── ÉTAPE 6 : dbt – mart ──────────────────────────────────────────────────
    dbt_mart = BashOperator(
        task_id="dbt_run_sales_summary",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt run --select marts.sales_summary "
            "--vars '{\"run_date\": \"{{ ds }}\"}'"
        ),
    )

    # ── ÉTAPE 7 : Quality gate ────────────────────────────────────────────────
    quality_check = BashOperator(
        task_id="dbt_test_sales",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt test --select stg_sales int_sales_daily sales_summary"
        ),
    )

    # ── DAG flow ──────────────────────────────────────────────────────────────
    # Spark pipeline runs first (independent of dbt)
    ingest_sales_raw >> clean_and_validate >> aggregate_sales

    # dbt chain starts after ingestion is clean (staging data is ready)
    clean_and_validate >> dbt_staging >> dbt_intermediate >> dbt_mart

    # Quality check waits for both Spark mart and dbt mart
    [aggregate_sales, dbt_mart] >> quality_check
