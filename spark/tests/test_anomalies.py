"""
Smoke-tests — Données & Anomalie Engineering
  T1 : generate_datasets    — 4 CSV générés, comptes et colonnes corrects
  T2 : injector_reproducibility — deux runs avec même seed → même sortie
  T3 : injector_report_schema   — rapport JSON complet et cohérent
  T4 : all_anomaly_types        — chaque type injecte bien des lignes
  T5 : controlled_failure_pipeline — job Spark détecte les anomalies
"""

import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile

import pandas as pd

BASE_DIR   = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SPARK_HOME = os.path.join(BASE_DIR, "airflow_env", "lib", "python3.9",
                          "site-packages", "pyspark")
PYTHON     = sys.executable
env        = {**os.environ, "SPARK_HOME": SPARK_HOME, "PYTHONPATH": BASE_DIR}
SKIP       = ("WARN", "Using ", "Setting ", "To adjust", "26/")

DATA_DIR   = os.path.join(BASE_DIR, "spark", "data")
GEN_SCRIPT = os.path.join(BASE_DIR, "spark", "generate_datasets.py")
INJ_SCRIPT = os.path.join(BASE_DIR, "spark", "anomaly_injector.py")
CFP_SCRIPT = os.path.join(BASE_DIR, "spark", "jobs", "controlled_failure_pipeline.py")

RAW_DIR    = os.path.join(DATA_DIR, "raw", "anomaly_test")
STAGING    = os.path.join(DATA_DIR, "staging", "anomaly_test")

results: dict[str, str] = {}


def run_cmd(cmd, label=""):
    r = subprocess.run(cmd, capture_output=True, text=True, env=env,
                       cwd=tempfile.mkdtemp(prefix="spark_test_"))
    for line in r.stdout.splitlines():
        if not any(line.startswith(p) for p in SKIP):
            print(line)
    if r.returncode != 0:
        for line in r.stderr.splitlines()[-20:]:
            print(line, file=sys.stderr)
        raise RuntimeError(f"{label or cmd[0]} a échoué (code {r.returncode})")


def md5(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


# ══════════════════════════════════════════════════════════════════════════════
# T1 — Génération des datasets
# ══════════════════════════════════════════════════════════════════════════════

print("=" * 60)
print("T1 — generate_datasets")
print("=" * 60)
shutil.rmtree(RAW_DIR, ignore_errors=True)

try:
    run_cmd([PYTHON, GEN_SCRIPT, "--seed", "42", "--dest", RAW_DIR], "generate_datasets")

    expected = {
        "customers.csv": (2_000, ["customer_id", "name", "email", "age", "segment", "country", "signup_date"]),
        "products.csv":  (500,   ["product_id", "name", "category", "brand", "unit_cost", "list_price", "launch_date"]),
        "sales.csv":     (50_000, ["order_id", "customer_id", "product_id", "sale_date", "quantity",
                                   "unit_price", "channel", "region", "currency", "revenue"]),
        "inventory.csv": (500,   ["product_id", "warehouse", "stock_qty", "reorder_point", "last_restock_date"]),
    }
    for fname, (expected_rows, expected_cols) in expected.items():
        path = os.path.join(RAW_DIR, fname)
        assert os.path.exists(path), f"{fname} manquant"
        df = pd.read_csv(path)
        assert len(df) == expected_rows, f"{fname} : attendu {expected_rows} lignes, obtenu {len(df)}"
        for col in expected_cols:
            assert col in df.columns, f"{fname} : colonne '{col}' manquante"
        # Aucune colonne entièrement nulle
        for col in df.columns:
            assert df[col].notna().any(), f"{fname}.{col} est entièrement nul"
        print(f"  {fname:<20} {len(df):>7,} lignes  {len(df.columns)} colonnes  OK")

    results["T1-generate_datasets"] = "OK"
except Exception as e:
    print(f"  FAILED: {e}")
    results["T1-generate_datasets"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T2 — Reproductibilité de l'injecteur
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 60)
print("T2 — injector_reproducibility")
print("=" * 60)

sales_csv = os.path.join(RAW_DIR, "sales.csv")
out_a = os.path.join(RAW_DIR, "sales_dirty_a.csv")
out_b = os.path.join(RAW_DIR, "sales_dirty_b.csv")
rep_a = os.path.join(RAW_DIR, "report_a.json")
rep_b = os.path.join(RAW_DIR, "report_b.json")

try:
    for out, rep in [(out_a, rep_a), (out_b, rep_b)]:
        run_cmd([
            PYTHON, INJ_SCRIPT,
            "--input",  sales_csv,
            "--output", out,
            "--report", rep,
            "--seed", "42", "--rate", "0.03",
            "--types", "nulls,duplicates,out_of_range",
        ], "anomaly_injector")

    assert md5(out_a) == md5(out_b), "Les deux runs avec seed=42 produisent des fichiers différents"
    print("  md5 run-A == md5 run-B  ✓")
    results["T2-reproducibility"] = "OK"
except Exception as e:
    print(f"  FAILED: {e}")
    results["T2-reproducibility"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T3 — Schéma du rapport JSON
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 60)
print("T3 — injector_report_schema")
print("=" * 60)

try:
    with open(rep_a) as f:
        report = json.load(f)

    required_keys = ["seed", "injection_rate", "total_rows_input", "total_rows_output",
                     "generated_at", "anomalies_injected", "total_anomalous_rows"]
    for k in required_keys:
        assert k in report, f"Clé manquante dans le rapport : '{k}'"

    assert report["total_anomalous_rows"] <= report["total_rows_input"], \
        "total_anomalous_rows > total_rows_input — impossible"
    assert report["total_rows_output"] >= report["total_rows_input"], \
        "total_rows_output < total_rows_input (les duplicates doivent augmenter le total)"
    assert len(report["anomalies_injected"]) > 0, "Aucune anomalie dans le rapport"

    for entry in report["anomalies_injected"]:
        for k in ["type", "row_indices", "count", "description"]:
            assert k in entry, f"Clé manquante dans une entrée d'anomalie : '{k}'"
        assert entry["count"] > 0, "count == 0 pour une entrée d'injection"

    print(f"  Clés requises     : {len(required_keys)}/{len(required_keys)}  ✓")
    print(f"  Entrées d'anomalie: {len(report['anomalies_injected'])}  ✓")
    print(f"  total_rows_input  : {report['total_rows_input']:,}")
    print(f"  total_rows_output : {report['total_rows_output']:,}  (duplicats inclus)")
    print(f"  total_anomalous   : {report['total_anomalous_rows']:,}")
    results["T3-report_schema"] = "OK"
except Exception as e:
    print(f"  FAILED: {e}")
    results["T3-report_schema"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T4 — Tous les types d'anomalies injectés
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 60)
print("T4 — all_anomaly_types")
print("=" * 60)

all_types_out = os.path.join(RAW_DIR, "sales_all_anomalies.csv")
all_types_rep = os.path.join(RAW_DIR, "report_all.json")

try:
    run_cmd([
        PYTHON, INJ_SCRIPT,
        "--input",  sales_csv,
        "--output", all_types_out,
        "--report", all_types_rep,
        "--seed", "0", "--rate", "0.02",
        # tous les types (défaut)
    ], "anomaly_injector all types")

    with open(all_types_rep) as f:
        rep_all = json.load(f)

    injected_types = {e["type"] for e in rep_all["anomalies_injected"]}
    if BASE_DIR not in sys.path:
        sys.path.insert(0, BASE_DIR)
    from spark.anomaly_injector import ALL_TYPES  # type: ignore
    for atype in ALL_TYPES:
        assert atype in injected_types, f"Type '{atype}' non trouvé dans le rapport"
        count = sum(e["count"] for e in rep_all["anomalies_injected"] if e["type"] == atype)
        assert count > 0, f"Type '{atype}' : count == 0"
        print(f"  {atype:<28} {count:>5} lignes injectées  ✓")

    results["T4-all_types"] = "OK"
except Exception as e:
    print(f"  FAILED: {e}")
    results["T4-all_types"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T5 — Controlled Failure Pipeline (Spark)
# ══════════════════════════════════════════════════════════════════════════════

print("\n" + "=" * 60)
print("T5 — controlled_failure_pipeline")
print("=" * 60)

shutil.rmtree(STAGING, ignore_errors=True)
manifest_path = os.path.join(RAW_DIR, "detection_manifest.json")

try:
    run_cmd([
        PYTHON, CFP_SCRIPT,
        "--date",   "2026-04-09",
        "--input",  all_types_out,
        "--report", all_types_rep,
        "--dest",   STAGING,
    ], "controlled_failure_pipeline")

    assert os.path.exists(manifest_path), "detection_manifest.json non créé"
    with open(manifest_path) as f:
        manifest = json.load(f)

    required = ["total_rows_raw", "total_rows_passed", "total_rows_caught",
                "global_rejection_rate_pct", "anomaly_detection_summary"]
    for k in required:
        assert k in manifest, f"Clé manquante dans le manifest : '{k}'"

    assert manifest["total_rows_raw"] > 0
    assert manifest["total_rows_passed"] <= manifest["total_rows_raw"]
    assert manifest["total_rows_caught"] >= 0
    assert manifest["global_rejection_rate_pct"] > 0, \
        "Taux de rejet == 0 alors que des anomalies ont été injectées"

    # Au moins nulls et negative_values devraient être attrapées
    caught = {e["anomaly_type"]: e["caught"]
              for e in manifest["anomaly_detection_summary"]}
    for detectable in ["nulls", "negative_values", "out_of_range"]:
        assert caught.get(detectable, 0) > 0, \
            f"Type '{detectable}' n'a pas été détecté alors qu'il devrait l'être"

    print(f"  Taux de rejet global    : {manifest['global_rejection_rate_pct']}%")
    print(f"  Lignes rejetées         : {manifest['total_rows_caught']:,}")
    print(f"  Lignes passées          : {manifest['total_rows_passed']:,}")
    for e in manifest["anomaly_detection_summary"]:
        print(f"  {e['anomaly_type']:<28} attrapé={e['caught']:>5} "
              f"({e['detection_rate_pct']}%)")

    # Vérifier que le Parquet de sortie existe
    assert any(f.endswith(".parquet") for _, _, files in os.walk(STAGING) for f in files), \
        "Aucun fichier Parquet dans le répertoire de sortie"
    print(f"  Parquet de sortie       : {STAGING}  ✓")
    results["T5-controlled_failure"] = "OK"
except Exception as e:
    import traceback; traceback.print_exc()
    print(f"  FAILED: {e}")
    results["T5-controlled_failure"] = "FAILED"


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
