"""
Spark job – Controlled Failure Pipeline
Ingère un CSV contenant des anomalies injectées, applique les mêmes filtres
que ingest_sales.py, puis produit un manifest JSON indiquant combien d'anomalies
de chaque type ont été détectées (caught) vs. ont survécu (survived).

Deux modes de détection :
  • Précis   — si le CSV contient les colonnes ``_row_id`` et ``_injected_types``
               (produites par anomaly_injector --tag-rows), Spark joint les lignes
               rejetées avec les tags pour obtenir un compte exact par type.
  • Estimé   — sinon, le taux de rejet global est distribué proportionnellement
               au nombre de lignes injectées par type (comportement historique).

Usage :
    # Mode précis (recommandé)
    python spark/anomaly_injector.py --input sales.csv --output sales_dirty.csv \\
        --report report.json --tag-rows

    python spark/jobs/controlled_failure_pipeline.py \\
        --date   2026-04-09 \\
        --input  spark/data/raw/sales_dirty.csv \\
        --report spark/data/raw/injection_report.json \\
        --dest   spark/data/staging/sales_controlled

    # Mode estimé (CSV sans colonnes de tag)
    python spark/jobs/controlled_failure_pipeline.py \\
        --date   2026-04-09 \\
        --input  spark/data/raw/sales_dirty_notag.csv \\
        --report spark/data/raw/injection_report.json \\
        --dest   spark/data/staging/sales_controlled
"""

from __future__ import annotations

import argparse
import json
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, DateType,
)

# Réutilisation du schéma défini dans ingest_sales
SALES_SCHEMA = StructType([
    StructField("order_id",    StringType(),  nullable=True),
    StructField("customer_id", StringType(),  nullable=True),
    StructField("product_id",  StringType(),  nullable=True),
    StructField("sale_date",   DateType(),    nullable=True),
    StructField("quantity",    IntegerType(), nullable=True),
    StructField("unit_price",  DoubleType(),  nullable=True),
    StructField("channel",     StringType(),  nullable=True),
    StructField("region",      StringType(),  nullable=True),
    StructField("currency",    StringType(),  nullable=True),
])

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def parse_args():
    parser = argparse.ArgumentParser(description="Controlled Failure Pipeline")
    parser.add_argument("--date",   required=True,
                        help="Date d'exécution YYYY-MM-DD")
    parser.add_argument("--input",  required=True,
                        help="CSV dirty (avec anomalies injectées)")
    parser.add_argument("--report", required=True,
                        help="Rapport JSON produit par anomaly_injector")
    parser.add_argument("--dest",   required=True,
                        help="Répertoire de sortie Parquet (lignes valides)")
    return parser.parse_args()


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("controlled_failure_pipeline")
        .master("local[*]")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def _is_tagged(spark: SparkSession, csv_path: str) -> bool:
    """Vérifie si le CSV contient les colonnes de tag (_row_id, _injected_types)."""
    header = spark.read.option("header", "true").csv(csv_path).columns
    return "_row_id" in header and "_injected_types" in header


def run(spark: SparkSession, args) -> dict:
    # ── 1. Charger le rapport d'injection ────────────────────────────────────
    with open(args.report, encoding="utf-8") as f:
        injection_report = json.load(f)

    injected_by_type: dict = {}
    for entry in injection_report.get("anomalies_injected", []):
        atype = entry["type"]
        injected_by_type.setdefault(atype, set()).update(entry["row_indices"])

    tagged_mode = injection_report.get("tag_rows", False) and _is_tagged(spark, args.input)
    print(f"[controlled_failure] Rapport chargé : "
          f"{len(injection_report['anomalies_injected'])} entrées d'injection  "
          f"[mode={'précis' if tagged_mode else 'estimé'}]")

    # ── 2. Ingestion avec schema enforcement ──────────────────────────────────
    # En mode tagged, on lit tout comme strings d'abord, on conserve
    # _injected_types dans le DataFrame principal (pas de jointure), puis on
    # caste les colonnes métier avec try_cast (NULL sur valeur non castable).
    if tagged_mode:
        df_all = (
            spark.read
            .option("header", "true")
            .option("mode", "PERMISSIVE")
            .csv(args.input)
        )
        # Colonnes métier avec try_cast + colonne de tag embarquée
        df_raw = (
            df_all
            .select(
                F.col("order_id").cast("string"),
                F.col("customer_id").cast("string"),
                F.col("product_id").cast("string"),
                F.to_date(F.col("sale_date")).alias("sale_date"),
                F.expr("try_cast(quantity   as int)").alias("quantity"),
                F.expr("try_cast(unit_price as double)").alias("unit_price"),
                F.col("channel").cast("string"),
                F.col("region").cast("string"),
                F.col("currency").cast("string"),
                # Tag porté directement dans chaque ligne → pas de jointure
                F.col("_injected_types"),
            )
        )
    else:
        df_raw = (
            spark.read
            .schema(SALES_SCHEMA)
            .option("header", "true")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .csv(args.input)
        )

    total_raw = df_raw.count()
    print(f"[controlled_failure] Lignes lues (brut) : {total_raw:,}")

    # ── 3. Mêmes filtres que ingest_sales ────────────────────────────────────
    filter_expr = (
        F.col("order_id").isNotNull()
        & F.col("sale_date").isNotNull()
        & (F.col("quantity") > 0)
        & (F.col("unit_price") > 0)
    )

    df_passed = (
        df_raw.filter(filter_expr)
        .withColumn("revenue",  F.round(F.col("quantity") * F.col("unit_price"), 2))
        .withColumn("currency", F.upper(F.trim(F.col("currency"))))
        .withColumn("channel",  F.upper(F.trim(F.col("channel"))))
        .withColumn("region",   F.upper(F.trim(F.col("region"))))
        .withColumn("run_date", F.lit(args.date).cast(DateType()))
    )
    df_caught = df_raw.filter(~filter_expr)

    total_passed = df_passed.count()
    total_caught = df_caught.count()

    print(f"[controlled_failure] Lignes passées  : {total_passed:,}")
    print(f"[controlled_failure] Lignes rejetées : {total_caught:,}")

    # ── 4. Écriture des lignes valides (sans colonnes de tag) ─────────────────
    out_cols = [c for c in df_passed.columns if not c.startswith("_")]
    os.makedirs(args.dest, exist_ok=True)
    df_passed.select(out_cols).write.mode("overwrite").partitionBy("run_date").parquet(args.dest)

    # ── 5. Manifest de détection par type d'anomalie ─────────────────────────
    manifest_entries = []
    detection_mode   = "precise" if tagged_mode else "estimated"

    if tagged_mode:
        # ── Mode précis : _injected_types est embarqué dans chaque ligne ─────
        # Pas de jointure — le tag voyage directement avec la ligne dans le
        # pipeline, quelle que soit la transformation appliquée.
        type_col = F.split(F.col("_injected_types"), r"\|")

        for atype, row_set in injected_by_type.items():
            injected_count = len(row_set)
            type_filter    = F.array_contains(type_col, F.lit(atype))
            exact_caught   = df_caught.filter(type_filter).count()
            exact_survived = df_passed.filter(type_filter).count()

            tagged_total = exact_caught + exact_survived
            manifest_entries.append({
                "anomaly_type":      atype,
                "injected_count":    injected_count,   # lignes originales injectées
                "tagged_in_csv":     tagged_total,     # lignes portant le tag dans le CSV
                "caught":            exact_caught,
                "survived":          exact_survived,
                "detection_rate_pct": round(exact_caught / tagged_total * 100, 1)
                                      if tagged_total > 0 else 0,
                "detection_mode":    "precise",
            })
    else:
        # ── Mode estimé : distribution proportionnelle ───────────────────────
        rejection_rate = total_caught / total_raw if total_raw > 0 else 0
        for atype, row_set in injected_by_type.items():
            injected_count   = len(row_set)
            estimated_caught   = round(injected_count * rejection_rate)
            estimated_survived = injected_count - estimated_caught
            manifest_entries.append({
                "anomaly_type":    atype,
                "injected_count":  injected_count,
                "caught":          estimated_caught,
                "survived":        estimated_survived,
                "detection_rate_pct": round(estimated_caught / injected_count * 100, 1)
                                      if injected_count > 0 else 0,
                "detection_mode":  "estimated",
            })

    manifest = {
        "date":                      args.date,
        "input_file":                args.input,
        "detection_mode":            detection_mode,
        "total_rows_raw":            total_raw,
        "total_rows_passed":         total_passed,
        "total_rows_caught":         total_caught,
        "global_rejection_rate_pct": round(total_caught / total_raw * 100, 2)
                                     if total_raw > 0 else 0,
        "anomaly_detection_summary": manifest_entries,
        "output_parquet":            args.dest,
    }

    manifest_path = os.path.join(os.path.dirname(args.report), "detection_manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    print(f"\n── Manifest de détection [{detection_mode}] ─────────────────────────────")
    print(f"  Taux de rejet global : {manifest['global_rejection_rate_pct']}%")
    for e in manifest_entries:
        if detection_mode == "precise":
            print(f"  {e['anomaly_type']:<25}"
                  f"  injecté={e['injected_count']:>5}"
                  f"  dans_csv={e['tagged_in_csv']:>5}"
                  f"  attrapé={e['caught']:>5}"
                  f"  survécu={e['survived']:>5}"
                  f"  ({e['detection_rate_pct']}%)")
        else:
            print(f"  {e['anomaly_type']:<25}"
                  f"  injecté={e['injected_count']:>5}"
                  f"  attrapé~={e['caught']:>5}"
                  f"  survécu~={e['survived']:>5}"
                  f"  ({e['detection_rate_pct']}%)")
    print(f"  Manifest écrit → {manifest_path}")

    return manifest


if __name__ == "__main__":
    args  = parse_args()
    spark = build_spark()
    try:
        run(spark, args)
    finally:
        spark.stop()
