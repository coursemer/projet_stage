"""
Spark job – Sales Aggregation (Étape 3 : Agrégations)
Reads clean Parquet, computes daily KPIs by region & channel, writes mart Parquet.

KPIs computed:
  - nb_orders          : distinct order count
  - nb_customers       : distinct customer count
  - total_units        : sum of quantities sold
  - total_revenue      : sum of revenue (EUR)
  - avg_order_value    : average revenue per order
  - cumulative_revenue : running total per region (ordered by sale_date)

Usage:
    python spark/jobs/aggregate_sales.py --date 2026-04-08
    python spark/jobs/aggregate_sales.py --date 2026-04-08 --source spark/data/staging/sales_clean --dest spark/data/marts/sales
"""

import argparse
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def parse_args():
    parser = argparse.ArgumentParser(description="Sales aggregation Spark job")
    parser.add_argument("--date",   required=True, help="Execution date YYYY-MM-DD")
    parser.add_argument("--source", default=os.path.join(BASE_DIR, "spark", "data", "staging", "sales_clean"),
                        help="Clean Parquet source path")
    parser.add_argument("--dest",   default=os.path.join(BASE_DIR, "spark", "data", "marts", "sales"),
                        help="Mart Parquet destination path")
    return parser.parse_args()


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("sales_aggregation")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def aggregate(spark: SparkSession, source: str, dest: str, run_date: str) -> int:
    print(f"[aggregate_sales] Reading clean data from: {source}")

    df = spark.read.parquet(source)
    print(f"[aggregate_sales] Input rows: {df.count():,}")

    # ── Daily KPIs by sale_date / region / channel ────────────────────────────
    window_region = (
        Window
        .partitionBy("region")
        .orderBy("sale_date")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    df_agg = (
        df
        .groupBy("sale_date", "region", "channel")
        .agg(
            F.countDistinct("order_id")    .alias("nb_orders"),
            F.countDistinct("customer_id") .alias("nb_customers"),
            F.sum("quantity")              .alias("total_units"),
            F.round(F.sum("revenue"), 2)   .alias("total_revenue"),
            F.round(F.avg("revenue"), 2)   .alias("avg_order_value"),
        )
        # Running revenue total per region
        .withColumn(
            "cumulative_revenue_by_region",
            F.round(F.sum("total_revenue").over(window_region), 2),
        )
        # Audit column
        .withColumn("aggregated_at", F.current_timestamp())
        .withColumn("run_date", F.lit(run_date))
    )

    total_rows = df_agg.count()

    (
        df_agg
        .write
        .mode("overwrite")
        .partitionBy("run_date", "region")
        .parquet(dest)
    )

    print(f"[aggregate_sales] Written {total_rows:,} aggregated rows → {dest}")

    # ── Print summary to console ───────────────────────────────────────────────
    print("\n── Daily Sales Summary ──────────────────────────────────────────────")
    df_agg.orderBy("region", "channel").show(truncate=False)

    return total_rows


if __name__ == "__main__":
    args = parse_args()
    spark = build_spark()
    try:
        aggregate(spark, args.source, args.dest, args.date)
    finally:
        spark.stop()
