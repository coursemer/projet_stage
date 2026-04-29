import psycopg2
import requests

def extract_airflow_metrics():
    conn = psycopg2.connect(
        host="localhost",
        database="airflow",
        user="user",
        password="password"
    )

    cur = conn.cursor()
    cur.execute("""
        SELECT dag_id, state, execution_date
        FROM dag_run
    """)

    rows = cur.fetchall()

    for row in rows:
        metric = {
            "source": "airflow",
            "pipeline_name": row[0],
            "metric_name": "status",
            "metric_value": 1 if row[1] == "success" else 0,
            "timestamp": str(row[2])
        }

        requests.post("http://localhost:8000/metrics", json=metric)

    conn.close()