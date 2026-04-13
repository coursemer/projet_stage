"""
Smoke-tests – Pipelines 1, 2 & 3
  P1 – Sales Analytics   : ingest_sales → clean_sales → aggregate_sales
  P2 – Customer 360      : customer_360
  P3 – Inventory SCD2    : inventory_scd2 (run 1 : historique vide, run 2 : changement)
"""

import os
import shutil
import subprocess
import sys
import tempfile

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

BASE_DIR   = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR   = os.path.join(BASE_DIR, "spark", "data")
JOBS_DIR   = os.path.join(BASE_DIR, "spark", "jobs")
PYTHON     = sys.executable
SPARK_HOME = os.path.join(BASE_DIR, "airflow_env", "lib", "python3.9",
                          "site-packages", "pyspark")
env  = {**os.environ, "SPARK_HOME": SPARK_HOME}
SKIP = ("WARN", "Using ", "Setting ", "To adjust", "26/")


# ── Utilitaires ───────────────────────────────────────────────────────────────

def _fresh_cwd():
    """Répertoire temporaire isolé pour éviter les conflits Derby entre jobs."""
    return tempfile.mkdtemp(prefix="spark_test_")


def write_parquet(records: list[dict], path: str):
    """Écrit un Parquet via pandas/pyarrow (pas de JVM, pas de conflit Derby)."""
    shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path, exist_ok=True)
    df = pd.DataFrame(records)
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False),
                   os.path.join(path, "part-00000.parquet"))


def run_job(job, args):
    result = subprocess.run(
        [PYTHON, os.path.join(JOBS_DIR, job)] + args,
        capture_output=True, text=True, env=env,
        cwd=_fresh_cwd(),
    )
    for line in result.stdout.splitlines():
        if not any(line.startswith(p) for p in SKIP):
            print(line)
    if result.returncode != 0:
        for line in result.stderr.splitlines()[-30:]:
            print(line, file=sys.stderr)
        raise RuntimeError(f"{job} a échoué (code {result.returncode})")


def show_parquet(path, label):
    script = f"""
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("show").master("local[*]").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
df = spark.read.parquet("{path}")
print(f"{label} → {{df.count()}} lignes")
df.show(truncate=False)
spark.stop()
"""
    r = subprocess.run([PYTHON, "-c", script], capture_output=True, text=True,
                       env=env, cwd=_fresh_cwd())
    for line in r.stdout.splitlines():
        if not any(line.startswith(p) for p in SKIP):
            print(line)


def spark_write(script):
    """Exécute un script Spark inline (seed Parquet via Spark). Quitte si échec."""
    r = subprocess.run([PYTHON, "-c", script], capture_output=True, text=True,
                       env=env, cwd=_fresh_cwd())
    if r.returncode != 0:
        print(r.stderr[-2000:], file=sys.stderr)
        sys.exit(1)
    for line in r.stdout.splitlines():
        if not any(line.startswith(p) for p in SKIP):
            print(line)


results = {}

RUN_DATE = "2026-04-08"


# ══════════════════════════════════════════════════════════════════════════════
# SEED DATA
# ══════════════════════════════════════════════════════════════════════════════

print("=" * 60)
print("SEED – Création des données de test")
print("=" * 60)

# ── CSV bruts pour Pipeline 1 ─────────────────────────────────────────────────
raw_sales_dir = os.path.join(DATA_DIR, "raw", "sales", f"date={RUN_DATE}")
shutil.rmtree(raw_sales_dir, ignore_errors=True)
os.makedirs(raw_sales_dir, exist_ok=True)
with open(os.path.join(raw_sales_dir, "sales.csv"), "w") as f:
    f.write("order_id,customer_id,product_id,sale_date,quantity,unit_price,channel,region,currency\n")
    f.write(f"ORD-001,c1,p1,{RUN_DATE},2,10.0,ONLINE,NORTH,EUR\n")
    f.write(f"ORD-002,c2,p2,{RUN_DATE},1,20.0,STORE,SOUTH,EUR\n")
    f.write(f"ORD-003,c1,p3,{RUN_DATE},3,5.0,MOBILE,EAST,USD\n")
    f.write(f"ORD-001,c1,p1,{RUN_DATE},2,10.0,ONLINE,NORTH,EUR\n")   # doublon → dédupliqué en étape 2
    f.write(f"BAD-999,c3,p4,{RUN_DATE},-1,0.0,ONLINE,NORTH,EUR\n")   # invalide → rejetée en étape 1
print(f"  CSV bruts → {raw_sales_dir}")

# ── Parquet via pandas (pas de JVM → pas de conflit Derby) ────────────────────

# Sales mart pour Pipeline 2
write_parquet([
    {"order_id": "ORD-001", "customer_id": "c1", "product_id": "p1",
     "sale_date": "2026-04-01", "quantity": 2, "unit_price": 10.0,
     "channel": "ONLINE", "region": "NORTH", "currency": "EUR", "revenue": 20.0},
    {"order_id": "ORD-002", "customer_id": "c2", "product_id": "p2",
     "sale_date": "2026-04-01", "quantity": 1, "unit_price": 20.0,
     "channel": "STORE", "region": "SOUTH", "currency": "EUR", "revenue": 20.0},
    {"order_id": "ORD-003", "customer_id": "c1", "product_id": "p3",
     "sale_date": "2026-04-02", "quantity": 3, "unit_price": 5.0,
     "channel": "MOBILE", "region": "EAST", "currency": "USD", "revenue": 15.0},
], os.path.join(DATA_DIR, "marts", "sales"))
print("  Sales mart OK")

# CRM & Customers
write_parquet([{"customer_id": "c1", "segment": "gold"},
               {"customer_id": "c2", "segment": "silver"}],
              os.path.join(DATA_DIR, "raw", "crm"))
write_parquet([{"customer_id": "c1", "name": "Alice"},
               {"customer_id": "c2", "name": "Bob"}],
              os.path.join(DATA_DIR, "raw", "customers"))
print("  CRM & Customers OK")

# Inventaire initial
write_parquet([{"product_id": "prod_A", "stock": 100, "location": "Paris"},
               {"product_id": "prod_B", "stock": 50,  "location": "Lyon"}],
              os.path.join(DATA_DIR, "raw", "inventory"))
print("  Inventaire initial OK")


# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE 1 – Sales Analytics
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 60)
print("PIPELINE 1 – Sales Analytics")
print("=" * 60)

staging       = os.path.join(DATA_DIR, "staging", "sales")
staging_clean = os.path.join(DATA_DIR, "staging", "sales_clean")
mart_sales    = os.path.join(DATA_DIR, "marts",   "sales_agg")
for p in [staging, staging_clean, mart_sales]:
    shutil.rmtree(p, ignore_errors=True)

print("\n--- Étape 1 : ingest_sales ---")
try:
    run_job("ingest_sales.py", [
        "--date",   RUN_DATE,
        "--source", os.path.join(DATA_DIR, "raw", "sales"),
        "--dest",   staging,
    ])
    show_parquet(staging, "staging/sales")
    results["P1-ingest"] = "OK"
except RuntimeError as e:
    print(f"FAILED: {e}")
    results["P1-ingest"] = "FAILED"

print("\n--- Étape 2 : clean_sales ---")
try:
    run_job("clean_sales.py", [
        "--date",   RUN_DATE,
        "--source", staging,
        "--dest",   staging_clean,
    ])
    show_parquet(staging_clean, "staging/sales_clean")
    results["P1-clean"] = "OK"
except RuntimeError as e:
    print(f"FAILED: {e}")
    results["P1-clean"] = "FAILED"

print("\n--- Étape 3 : aggregate_sales ---")
try:
    run_job("aggregate_sales.py", [
        "--date",   RUN_DATE,
        "--source", staging_clean,
        "--dest",   mart_sales,
    ])
    show_parquet(mart_sales, "marts/sales_agg")
    results["P1-aggregate"] = "OK"
except RuntimeError as e:
    print(f"FAILED: {e}")
    results["P1-aggregate"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE 2 – Customer 360
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 60)
print("PIPELINE 2 – Customer 360")
print("=" * 60)

dest_c360 = os.path.join(DATA_DIR, "marts", "customer_360")
shutil.rmtree(dest_c360, ignore_errors=True)

print("\n--- customer_360 ---")
try:
    run_job("customer_360.py", [
        "--date",      RUN_DATE,
        "--sales",     os.path.join(DATA_DIR, "marts",  "sales"),
        "--crm",       os.path.join(DATA_DIR, "raw",    "crm"),
        "--customers", os.path.join(DATA_DIR, "raw",    "customers"),
        "--dest",      dest_c360,
    ])
    show_parquet(dest_c360, "marts/customer_360")
    results["P2-customer_360"] = "OK"
except RuntimeError as e:
    print(f"FAILED: {e}")
    results["P2-customer_360"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# PIPELINE 3 – Inventory SCD2
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 60)
print("PIPELINE 3 – Inventory SCD2")
print("=" * 60)

dest_scd2 = os.path.join(DATA_DIR, "marts", "inventory_scd2")
shutil.rmtree(dest_scd2, ignore_errors=True)

print("\n--- inventory_scd2 run-1 (historique vide) ---")
try:
    run_job("inventory_scd2.py", [
        "--date",    "2026-04-08",
        "--current", os.path.join(DATA_DIR, "raw",   "inventory"),
        "--history", dest_scd2,
        "--dest",    dest_scd2,
    ])
    show_parquet(dest_scd2, "marts/inventory_scd2 run-1")
    results["P3-scd2-run1"] = "OK"
except RuntimeError as e:
    print(f"FAILED: {e}")
    results["P3-scd2-run1"] = "FAILED"

# Mise à jour de l'inventaire via pandas (pas de JVM → pas de conflit Derby)
print("\n--- inventory_scd2 run-2 (stock prod_A : 100 → 80) ---")
write_parquet([{"product_id": "prod_A", "stock": 80, "location": "Paris"},
               {"product_id": "prod_B", "stock": 50, "location": "Lyon"}],
              os.path.join(DATA_DIR, "raw", "inventory"))
print("  Inventaire mis à jour OK")

try:
    run_job("inventory_scd2.py", [
        "--date",    "2026-04-09",
        "--current", os.path.join(DATA_DIR, "raw",   "inventory"),
        "--history", dest_scd2,
        "--dest",    dest_scd2,
    ])
    show_parquet(
        dest_scd2,
        "marts/inventory_scd2 run-2  (attendu : 3 lignes — 1 fermée + 1 nouvelle + 1 inchangée)",
    )
    results["P3-scd2-run2"] = "OK"
except RuntimeError as e:
    print(f"FAILED: {e}")
    results["P3-scd2-run2"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# RÉSUMÉ
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 60)
print("RÉSUMÉ")
print("=" * 60)
all_ok = True
for label, status in results.items():
    icon = "✅" if status == "OK" else "❌"
    print(f"  {icon}  {label:<25} {status}")
    if status != "OK":
        all_ok = False
print("=" * 60)
print("RÉSULTAT GLOBAL :", "✅ TOUS OK" if all_ok else "❌ ÉCHECS DÉTECTÉS")
