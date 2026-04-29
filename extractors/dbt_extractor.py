import os
import sys
import json
import requests
from datetime import datetime, timezone
from pathlib import Path

METRICS_API    = os.getenv("METRICS_API_URL", "http://localhost:8090")
DBT_TARGET_DIR = os.getenv("DBT_TARGET_DIR", "./dbt/target")

# SQLite fallback — resolve project root regardless of cwd
_HERE     = Path(__file__).resolve().parent
_ROOT     = _HERE.parent
sys.path.insert(0, str(_ROOT))


def _sqlite_write(name: str, value: float, ts: str) -> None:
    from spark.metrics.storage import SQLiteMetricsStore, MetricPoint, DEFAULT_DB
    store = SQLiteMetricsStore(DEFAULT_DB)
    store.write([MetricPoint(source="dbt", name=name, value=value, ts=ts)])


def post_metric(pipeline_name: str, metric_name: str, metric_value: float, timestamp=None) -> None:
    ts      = (timestamp or datetime.now(timezone.utc)).isoformat()
    name    = f"{pipeline_name}.{metric_name}"
    payload = [{"source": "dbt", "name": name, "value": float(metric_value), "ts": ts}]
    try:
        r = requests.post(f"{METRICS_API}/api/v1/metrics/push", json=payload, timeout=5)
        r.raise_for_status()
    except Exception:
        # API not reachable — write directly to SQLite
        try:
            _sqlite_write(name, float(metric_value), ts)
        except Exception as e:
            print(f"[dbt_extractor] Could not store metric {name}: {e}")


def extract_dbt_metrics(target_dir: str = None) -> None:
    target           = Path(target_dir or DBT_TARGET_DIR)
    run_results_path = target / "run_results.json"

    if not run_results_path.exists():
        print(f"[dbt_extractor] run_results.json not found at {run_results_path}")
        return

    with open(run_results_path) as f:
        run_results = json.load(f)

    generated_at = run_results.get("metadata", {}).get("generated_at", datetime.now(timezone.utc).isoformat())
    try:
        run_ts = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
    except Exception:
        run_ts = datetime.now(timezone.utc)

    results = run_results.get("results", [])
    print(f"[dbt_extractor] Processing {len(results)} results from {run_results_path}")

    for result in results:
        unique_id = result.get("unique_id", "unknown")
        status    = result.get("status", "")
        timing    = result.get("timing", [])
        node_type = unique_id.split(".")[0]

        duration_sec = 0.0
        for t in timing:
            if t.get("name") == "execute":
                started  = t.get("started_at")
                finished = t.get("completed_at")
                if started and finished:
                    try:
                        s = datetime.fromisoformat(started.replace("Z", "+00:00"))
                        e = datetime.fromisoformat(finished.replace("Z", "+00:00"))
                        duration_sec = (e - s).total_seconds()
                    except Exception:
                        pass

        adapter_response = result.get("adapter_response", {})
        rows_affected    = adapter_response.get("rows_affected", 0) or 0

        parts      = unique_id.split(".")
        model_name = parts[-1] if len(parts) >= 3 else unique_id

        if node_type == "model":
            is_success = 1.0 if status == "success" else 0.0
            post_metric(model_name, "model_duration_seconds", duration_sec, run_ts)
            post_metric(model_name, "model_success",          is_success,   run_ts)
            if rows_affected:
                post_metric(model_name, "rows_affected", rows_affected, run_ts)
            print(f"  model {model_name}: {duration_sec:.2f}s | success={is_success} | rows={rows_affected}")

        elif node_type == "test":
            is_pass = 1.0 if status == "pass" else 0.0
            post_metric(model_name, "test_pass",             is_pass,      run_ts)
            post_metric(model_name, "test_duration_seconds", duration_sec, run_ts)
            print(f"  test  {model_name}: {'PASS' if is_pass else 'FAIL'}")

    print(f"[dbt_extractor] Done — {len(results)} results processed")


if __name__ == "__main__":
    extract_dbt_metrics()
