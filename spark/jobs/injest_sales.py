"""
Spark job – Sales ingestion
Reads raw CSV/Parquet from the data lake, applies basic schema enforcement,
and writes partitioned Parquet to the staging zone.

Usage (called by Airflow SparkSubmitOperator):
    spark-submit ingest_sales.py --date 2024-01-15 \
        --source s3a://raw/sales/ --dest s3a://staging/sales/
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, DateType,
)

# ── Schema enforcement ────────────────────────────────────────────────────────
SALES_SCHEMA = StructType([
    StructField("order_id",      StringType(),  nullable=False),
    StructField("customer_id",   StringType(),  nullable=True),
    StructField("product_id",    StringType(),  nullable=True),
    StructField("sale_date",     DateType(),    nullable=False),
    StructField("quantity",      IntegerType(), nullable=True),
    StructField("unit_price",    DoubleType(),  nullable=True),
    StructField("channel",       StringType(),  nullable=True),
    StructField("region",        StringType(),  nullable=True),
    StructField("currency",      StringType(),  nullable=True),
])


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",   required=True, help="Execution date YYYY-MM-DD")
    parser.add_argument("--source", required=True, help="Source path (S3 / GCS / local)")
    parser.add_argument("--dest",   required=True, help="Destination staging path")
    return parser.parse_args()


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("sales_ingestion")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )


def ingest(spark: SparkSession, source: str, dest: str, run_date: str):
    # Read raw files for the given partition date
    raw_path = f"{source}/date={run_date}/"

    df = (
        spark.read
        .schema(SALES_SCHEMA)
        .option("header", "true")
        .option("mode", "PERMISSIVE")           # keep bad rows, flag them
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(raw_path)
    )

    # ── Basic cleaning ────────────────────────────────────────────────────────
    df_clean = (
        df
        # Drop rows with no order_id or sale_date (non-negotiable keys)
        .filter(F.col("order_id").isNotNull() & F.col("sale_date").isNotNull())
        # Derive revenue
        .withColumn("revenue", F.col("quantity") * F.col("unit_price"))
        # Standardise currency to uppercase
        .withColumn("currency", F.upper(F.trim(F.col("currency"))))
        # Add ingestion metadata
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("run_date",    F.lit(run_date).cast(DateType()))
    )

    # ── Write partitioned Parquet ─────────────────────────────────────────────
    (
        df_clean
        .write
        .mode("overwrite")
        .partitionBy("run_date", "region")
        .parquet(dest)
    )

    total = df_clean.count()
    print(f"[ingest_sales] Written {total:,} rows → {dest}")
    return total


if __name__ == "__main__":
    args = parse_args()
    spark = build_spark()
    try:
        ingest(spark, args.source, args.dest, args.date)
    finally:
        spark.stop()