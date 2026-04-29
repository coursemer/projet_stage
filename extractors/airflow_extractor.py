import os
import requests
import psycopg2
from datetime import datetime, timedelta

METRICS_API = os.getenv("METRICS_API_URL", "http://metrics-api:8000")

def get_airflow_conn():
    return psycopg2.connect(
        host=os.getenv("AIRFLOW_DB_HOST", "postgres"),
        dbname=os.getenv("AIRFLOW_DB_NAME", "airflow"),
        user=os.getenv("AIRFLOW_DB_USER", "airflow"),
        password=os.getenv("AIRFLOW_DB_PASSWORD", "airflow"),
    )

def post_metric(source, pipeline_name, metric_name, metric_value, timestamp=None):
    payload = {
        "source": source,
        "pipeline_name": pipeline_name,
        "metric_name": metric_name,
        "metric_value": float(metric_value),
        "timestamp": (timestamp or datetime.utcnow()).isoformat(),
    }
    try:
        r = requests.post(f"{METRICS_API}/metrics", json=payload, timeout=5)
        r.raise_for_status()
    except Exception as e:
        print(f"[airflow_extractor] Failed to post metric: {e}")

def extract_airflow_metrics(lookback_hours: int = 24):
    """
    Pulls DAG run metrics from Airflow's metadata DB for the last N hours
    and sends them to the central metrics API.
    Metrics extracted:
      - dag_success   : 1.0 if success, 0.0 if failed
      - dag_duration  : total run duration in seconds
    Also pulls per-task duration via task_instance.
    """
    since = datetime.utcnow() - timedelta(hours=lookback_hours)
    conn = get_airflow_conn()
    try:
        with conn.cursor() as cur:
            # --- DAG-level metrics ---
            cur.execute("""
                SELECT dag_id,
                       state,
                       execution_date,
                       end_date - start_date AS duration
                FROM dag_run
                WHERE start_date >= %s
                  AND state IN ('success', 'failed')
            """, (since,))
            for dag_id, state, exec_date, duration in cur.fetchall():
                duration_sec = duration.total_seconds() if duration else 0.0
                post_metric(
                    source="airflow",
                    pipeline_name=dag_id,
                    metric_name="dag_success",
                    metric_value=1.0 if state == "success" else 0.0,
                    timestamp=exec_date,
                )
                post_metric(
                    source="airflow",
                    pipeline_name=dag_id,
                    metric_name="dag_duration_seconds",
                    metric_value=duration_sec,
                    timestamp=exec_date,
                )

            # --- Task-level duration ---
            cur.execute("""
                SELECT dag_id,
                       task_id,
                       state,
                       start_date,
                       end_date - start_date AS duration
                FROM task_instance
                WHERE start_date >= %s
                  AND state IN ('success', 'failed', 'upstream_failed')
            """, (since,))
            for dag_id, task_id, state, start_date, duration in cur.fetchall():
                duration_sec = duration.total_seconds() if duration else 0.0
                post_metric(
                    source="airflow",
                    pipeline_name=f"{dag_id}.{task_id}",
                    metric_name="task_duration_seconds",
                    metric_value=duration_sec,
                    timestamp=start_date,
                )
                post_metric(
                    source="airflow",
                    pipeline_name=f"{dag_id}.{task_id}",
                    metric_name="task_success",
                    metric_value=1.0 if state == "success" else 0.0,
                    timestamp=start_date,
                )
    finally:
        conn.close()

    print(f"[airflow_extractor] Done — lookback {lookback_hours}h")

if __name__ == "__main__":
    extract_airflow_metrics()