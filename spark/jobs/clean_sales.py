"""
Spark job – Sales Cleaning & Validation (Étape 2 : Nettoyage)
Reads staged Parquet, applies Pandera schema validation, deduplicates, writes clean Parquet.

Usage:
    python spark/jobs/clean_sales.py --date 2026-04-08
    python spark/jobs/clean_sales.py --date 2026-04-08 --source spark/data/staging/sales --dest spark/data/staging/sales_clean
"""

import argparse
import os
import sys
import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

try:
    from spark.metrics.collector import SparkMetricsEmitter
except ImportError:
    SparkMetricsEmitter = None  # type: ignore[assignment,misc]

# ── Pandera schema ────────────────────────────────────────────────────────────
SALES_PANDERA_SCHEMA = DataFrameSchema(
    columns={
        "order_id":    Column(str,   Check.str_matches(r"^ORD-\d+$"), nullable=False),
        "customer_id": Column(str,   nullable=True),
        "product_id":  Column(str,   nullable=True),
        "quantity":    Column(int,   Check.greater_than(0), nullable=False),
        "unit_price":  Column(float, Check.greater_than(0.0), nullable=False),
        "revenue":     Column(float, Check.greater_than(0.0), nullable=False),
        "channel":     Column(str,   Check.isin(["ONLINE", "STORE", "MOBILE"]), nullable=True),
        "region":      Column(str,   Check.isin(["NORTH", "SOUTH", "EAST", "WEST"]), nullable=True),
        "currency":    Column(str,   Check.str_length(3, 3), nullable=True),
    },
    coerce=True,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Sales cleaning & validation Spark job")
    parser.add_argument("--date",   required=True, help="Execution date YYYY-MM-DD")
    parser.add_argument("--source", default=os.path.join(BASE_DIR, "spark", "data", "staging", "sales"),
                        help="Staged Parquet source path")
    parser.add_argument("--dest",   default=os.path.join(BASE_DIR, "spark", "data", "staging", "sales_clean"),
                        help="Cleaned Parquet destination path")
    return parser.parse_args()


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("sales_cleaning")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def validate_with_pandera(pdf: pd.DataFrame) -> pd.DataFrame:
    """Run Pandera validation on a pandas partition. Returns only valid rows."""
    try:
        validated = SALES_PANDERA_SCHEMA.validate(pdf, lazy=True)
        return validated
    except pa.errors.SchemaErrors as err:
        invalid_idx = err.failure_cases["index"].dropna().unique()
        valid = pdf.drop(index=invalid_idx, errors="ignore")
        print(f"[clean_sales] Pandera rejected {len(invalid_idx)} rows in partition")
        return valid


def clean(spark: SparkSession, source: str, dest: str, run_date: str) -> int:
    source_path = os.path.join(source)
    print(f"[clean_sales] Reading staged data from: {source_path}")

    df = spark.read.parquet(source_path)
    total_staged = df.count()
    print(f"[clean_sales] Staged rows: {total_staged:,}")

    # ── Spark-level cleaning ──────────────────────────────────────────────────
    df_spark = (
        df
        # Drop duplicates on order_id (keep first occurrence)
        .dropDuplicates(["order_id"])
        # Ensure revenue is consistent with quantity * unit_price (recalculate)
        .withColumn("revenue", F.round(F.col("quantity") * F.col("unit_price"), 2))
        # Fill missing customer_id with 'UNKNOWN'
        .withColumn("customer_id", F.coalesce(F.col("customer_id"), F.lit("UNKNOWN")))
        # Trim and upper all string columns
        .withColumn("order_id",   F.trim(F.col("order_id")))
        .withColumn("product_id", F.upper(F.trim(F.col("product_id"))))
        # Add cleaning metadata
        .withColumn("cleaned_at", F.current_timestamp())
    )

    # ── Pandera validation via pandas UDF ────────────────────────────────────
    # Convert to pandas for validation (works for moderate data volumes)
    pdf = df_spark.toPandas()
    pdf_validated = validate_with_pandera(pdf)

    total_clean = len(pdf_validated)
    rejected = total_staged - total_clean
    print(f"[clean_sales] Valid rows: {total_clean:,} | Rejected by Pandera: {rejected:,}")

    # Convert back to Spark and write
    df_clean = spark.createDataFrame(pdf_validated)

    (
        df_clean
        .write
        .mode("overwrite")
        .partitionBy("run_date", "region")
        .parquet(dest)
    )

    print(f"[clean_sales] Written {total_clean:,} clean rows → {dest}")
    return total_staged, total_clean, rejected


if __name__ == "__main__":
    args = parse_args()
    spark = build_spark()
    try:
        if SparkMetricsEmitter is not None:
            with SparkMetricsEmitter("clean_sales", run_date=args.date) as em:
                rows_in, rows_out, rows_rej = clean(spark, args.source, args.dest, args.date)
                em.record(rows_input=rows_in, rows_output=rows_out, rows_rejected=rows_rej)
        else:
            clean(spark, args.source, args.dest, args.date)
    finally:
        spark.stop()
