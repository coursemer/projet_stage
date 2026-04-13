"""
Spark job – Inventory SCD2 (Historisation SCD Type 2)

Étapes :
1. Charger l'inventaire courant et l'historique
2. Détecter les changements (insert/update)
3. Appliquer la logique SCD2 (fermeture des anciens, ouverture des nouveaux)

Usage :
    python spark/jobs/inventory_scd2.py --date 2026-04-08
"""

import argparse
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def parse_args():
    parser = argparse.ArgumentParser(description="Inventory SCD2 Spark job")
    parser.add_argument("--date", required=True, help="Execution date YYYY-MM-DD")
    parser.add_argument("--current", default=os.path.join(BASE_DIR, "spark", "data", "raw", "inventory"), help="Current inventory path")
    parser.add_argument("--history", default=os.path.join(BASE_DIR, "spark", "data", "marts", "inventory_scd2"), help="Inventory SCD2 history path")
    parser.add_argument("--dest", default=os.path.join(BASE_DIR, "spark", "data", "marts", "inventory_scd2"), help="Output path")
    return parser.parse_args()

def main():
    args = parse_args()
    spark = SparkSession.builder.appName("inventory_scd2").master("local[*]").getOrCreate()

    # Charger les données
    current = spark.read.parquet(args.current)
    try:
        history = spark.read.parquet(args.history)
    except Exception:
        history = spark.createDataFrame([], current.schema)

    # Ajout des colonnes SCD2
    current = current.withColumn("scd2_start", F.lit(args.date).cast(TimestampType()))
    current = current.withColumn("scd2_end", F.lit(None).cast(TimestampType()))
    current = current.withColumn("scd2_active", F.lit(True))

    # Si historique vide, on écrit tout comme nouvelle version
    if history.rdd.isEmpty():
        current.write.mode("overwrite").parquet(args.dest)
        spark.stop()
        return

    # Jointure sur la clé métier (aliases pour éviter l'ambiguïté de colonnes)
    join_cols = ["product_id"]
    tracked_cols = ["stock", "location"]
    cur = current.alias("cur")
    hist = history.filter(F.col("scd2_active") == True).alias("hist")
    joined = cur.join(hist, join_cols, "outer")

    # Détection des changements
    cond_change = None
    for col in tracked_cols:
        c = (F.coalesce(F.col(f"cur.{col}"), F.lit("__NULL__")) != F.coalesce(F.col(f"hist.{col}"), F.lit("__NULL__")))
        cond_change = c if cond_change is None else (cond_change | c)

    out_cols = current.columns  # product_id, stock, location, scd2_start, scd2_end, scd2_active

    # Lignes à fermer (anciennes versions)
    to_close = joined.filter(cond_change & F.col("hist.scd2_active") & F.col("hist.product_id").isNotNull())
    closed = to_close.select(
        F.col("hist.product_id"),
        *[F.col(f"hist.{c}") for c in tracked_cols],
        F.col("hist.scd2_start"),
        F.lit(args.date).cast(TimestampType()).alias("scd2_end"),
        F.lit(False).alias("scd2_active"),
    )

    # Nouvelles lignes à ouvrir
    to_open = joined.filter(cond_change & F.col("cur.product_id").isNotNull())
    opened = to_open.select([F.col(f"cur.{c}") for c in out_cols])

    # Lignes inchangées
    unchanged = joined.filter(~cond_change & F.col("hist.scd2_active") & F.col("hist.product_id").isNotNull())
    unchanged = unchanged.select([F.col(f"hist.{c}") for c in out_cols])

    # Fusion finale sans doublons
    final = closed.unionByName(opened).unionByName(unchanged)
    final = final.dropDuplicates(["product_id", "scd2_start"])  # Unicité sur clé métier + début de validité
    final.write.mode("overwrite").parquet(args.dest)
    spark.stop()
    return

if __name__ == "__main__":
    main()
