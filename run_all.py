"""
Run all extractors in sequence.
Plug this into an Airflow DAG or a cron job.
"""
from airflow_extractor import extract_airflow_metrics
from dbt_extractor    import extract_dbt_metrics
from spark_extractor  import extract_spark_metrics_from_history
import logging

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    print("=== Running all extractors ===")
    extract_airflow_metrics(lookback_hours=1)
    extract_dbt_metrics()
    # spark history is optional — only if you have the history server
    # extract_spark_metrics_from_history(spark=None, job_name="etl_orders")
    print("=== All done ===")