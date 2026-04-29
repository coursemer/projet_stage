import os
import time
import requests
from datetime import datetime
from functools import wraps
from pyspark.sql import SparkSession

METRICS_API = os.getenv("METRICS_API_URL", "http://metrics-api:8000")

def post_metric(pipeline_name, metric_name, metric_value):
    payload = {
        "source": "spark",
        "pipeline_name": pipeline_name,
        "metric_name": metric_name,
        "metric_value": float(metric_value),
        "timestamp": datetime.utcnow().isoformat(),
    }
    try:
        r = requests.post(f"{METRICS_API}/metrics", json=payload, timeout=5)
        r.raise_for_status()
    except Exception as e:
        print(f"[spark_extractor] Failed to post metric: {e}")

def track_spark_job(job_name: str):
    """
    Decorator — wrap any PySpark function to auto-collect:
      - job_duration_seconds
      - records_processed
      - job_success  (1.0 / 0.0)

    Usage:
        @track_spark_job("etl_orders")
        def run(spark):
            df = spark.read.parquet(...)
            ...
            return df   # return the final DataFrame to count rows
    """
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            start = time.time()
            success = 0.0
            records = 0
            try:
                result = fn(*args, **kwargs)
                success = 1.0
                # if the job returns a DataFrame, count its rows
                if hasattr(result, "count"):
                    records = result.count()
                return result
            except Exception as e:
                print(f"[spark_extractor] Job '{job_name}' raised: {e}")
                raise
            finally:
                duration = time.time() - start
                post_metric(job_name, "job_duration_seconds", duration)
                post_metric(job_name, "records_processed", records)
                post_metric(job_name, "job_success", success)
                print(f"[spark_extractor] {job_name}: {duration:.2f}s | "
                      f"{records} records | success={success}")
        return wrapper
    return decorator

def extract_spark_metrics_from_history(spark: SparkSession, job_name: str):
    """
    Alternative: pull metrics from the Spark REST history server
    after a job completes (useful when you can't modify the job code).
    """
    history_url = os.getenv("SPARK_HISTORY_URL", "http://spark-history:18080")
    try:
        r = requests.get(f"{history_url}/api/v1/applications", timeout=5)
        apps = r.json()
        # Take the most recent app matching job_name
        matching = [a for a in apps if job_name in a.get("name", "")]
        if not matching:
            print(f"[spark_extractor] No history entry found for '{job_name}'")
            return
        app = matching[0]
        app_id = app["id"]
        attempts = app.get("attempts", [{}])
        latest = attempts[0]
        duration_ms = latest.get("duration", 0)
        post_metric(job_name, "job_duration_seconds", duration_ms / 1000)
        post_metric(job_name, "job_success",
                    1.0 if latest.get("lastUpdated") else 0.0)
        print(f"[spark_extractor] History pulled for app {app_id}")
    except Exception as e:
        print(f"[spark_extractor] History pull failed: {e}")


# --- Example usage (in your actual Spark job file) ---
# from extractors.spark_extractor import track_spark_job
#
# @track_spark_job("etl_orders")
# def run_etl(spark):
#     df = spark.read.parquet("s3://bucket/orders/")
#     df_clean = df.filter(df.status == "complete")
#     df_clean.write.mode("overwrite").parquet("s3://bucket/orders_clean/")
#     return df_clean
#
# if __name__ == "__main__":
#     spark = SparkSession.builder.appName("etl_orders").getOrCreate()
#     run_etl(spark)