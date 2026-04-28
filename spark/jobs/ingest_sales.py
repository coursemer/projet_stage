"""
Spark job – Sales Ingestion (Étape 1 : Ingestion)
Reads raw CSV from local data lake, enforces schema, writes partitioned Parquet to staging zone.

Usage:
    python spark/jobs/ingest_sales.py --date 2026-04-08
    python spark/jobs/ingest_sales.py --date 2026-04-08 --source spark/data/raw/sales --dest spark/data/staging/sales
"""

import argparse
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, DateType,
)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

try:
    from spark.metrics.collector import SparkMetricsEmitter
except ImportError:
    SparkMetricsEmitter = None  # type: ignore[assignment,misc]

# ── Schema enforcement ────────────────────────────────────────────────────────
SALES_SCHEMA = StructType([
    StructField("order_id",     StringType(),  nullable=True),
    StructField("customer_id",  StringType(),  nullable=True),
    StructField("product_id",   StringType(),  nullable=True),
    StructField("sale_date",    DateType(),    nullable=True),
    StructField("quantity",     IntegerType(), nullable=True),
    StructField("unit_price",   DoubleType(),  nullable=True),
    StructField("channel",      StringType(),  nullable=True),
    StructField("region",       StringType(),  nullable=True),
    StructField("currency",     StringType(),  nullable=True),
])

def parse_args():
    parser = argparse.ArgumentParser(description="Sales ingestion Spark job")
    parser.add_argument("--date",   required=True, help="Execution date YYYY-MM-DD")
    parser.add_argument("--source", default=os.path.join(BASE_DIR, "spark", "data", "raw", "sales"),
                        help="Source root path")
    parser.add_argument("--dest",   default=os.path.join(BASE_DIR, "spark", "data", "staging", "sales"),
                        help="Destination staging path")
    return parser.parse_args()


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("sales_ingestion")
        .master("local[*]")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.sql.shuffle.partitions", "4")   # low for local mode
        .getOrCreate()
    )


def ingest(spark: SparkSession, source: str, dest: str, run_date: str) -> int:
    raw_path = os.path.join(source, f"date={run_date}")
    print(f"[ingest_sales] Reading from: {raw_path}")

    df = (
        spark.read
        .schema(SALES_SCHEMA)
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(raw_path)
    )

    total_raw = df.count()
    print(f"[ingest_sales] Raw rows read: {total_raw:,}")

    df_clean = (
        df
        # Drop rows missing mandatory keys
        .filter(F.col("order_id").isNotNull() & F.col("sale_date").isNotNull())
        # Only keep rows with valid quantities and prices
        .filter(F.col("quantity") > 0)
        .filter(F.col("unit_price") > 0)
        # Derive revenue
        .withColumn("revenue", F.round(F.col("quantity") * F.col("unit_price"), 2))
        # Standardise string columns
        .withColumn("currency", F.upper(F.trim(F.col("currency"))))
        .withColumn("channel",  F.upper(F.trim(F.col("channel"))))
        .withColumn("region",   F.upper(F.trim(F.col("region"))))
        # Ingestion metadata
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("run_date",    F.lit(run_date).cast(DateType()))
    )

    total_clean = df_clean.count()
    rejected = total_raw - total_clean
    print(f"[ingest_sales] Clean rows: {total_clean:,} | Rejected: {rejected:,}")

    (
        df_clean
        .write
        .mode("overwrite")
        .partitionBy("run_date", "region")
        .parquet(dest)
    )

    print(f"[ingest_sales] Written {total_clean:,} rows → {dest}")
    return total_raw, total_clean, rejected


if __name__ == "__main__":
    args = parse_args()
    spark = build_spark()
    try:
        if SparkMetricsEmitter is not None:
            with SparkMetricsEmitter("ingest_sales", run_date=args.date) as em:
                rows_in, rows_out, rows_rej = ingest(spark, args.source, args.dest, args.date)
                em.record(rows_input=rows_in, rows_output=rows_out, rows_rejected=rows_rej)
        else:
            ingest(spark, args.source, args.dest, args.date)
    finally:
        spark.stop()
