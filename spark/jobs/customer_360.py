"""
Spark job – Customer 360 (Joins multiples → Déduplication → Enrichissement)

Étapes :
1. Joindre les données clients, ventes, CRM
2. Dédupliquer sur customer_id
3. Enrichir avec des KPIs ou attributs

Usage :
    python spark/jobs/customer_360.py --date 2026-04-08
"""

import argparse
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def parse_args():
    parser = argparse.ArgumentParser(description="Customer 360 Spark job")
    parser.add_argument("--date", required=True, help="Execution date YYYY-MM-DD")
    parser.add_argument("--sales", default=os.path.join(BASE_DIR, "spark", "data", "marts", "sales"), help="Sales mart path")
    parser.add_argument("--crm", default=os.path.join(BASE_DIR, "spark", "data", "raw", "crm"), help="CRM data path")
    parser.add_argument("--customers", default=os.path.join(BASE_DIR, "spark", "data", "raw", "customers"), help="Customers data path")
    parser.add_argument("--dest", default=os.path.join(BASE_DIR, "spark", "data", "marts", "customer_360"), help="Output path")
    return parser.parse_args()

def main():
    args = parse_args()
    spark = SparkSession.builder.appName("customer_360").master("local[*]").getOrCreate()

    # Chargement des données
    sales = spark.read.parquet(args.sales)
    crm = spark.read.parquet(args.crm)
    customers = spark.read.parquet(args.customers)

    # Joins multiples
    df = sales.join(customers, "customer_id", "left")\
             .join(crm, "customer_id", "left")

    # Déduplication
    window = F.row_number().over(Window.partitionBy("customer_id").orderBy(F.col("sale_date").desc()))
    df = df.withColumn("rn", window).filter(F.col("rn") == 1).drop("rn")

    # Enrichissement (exemple : total achats)
    df = df.withColumn("total_sales", F.sum("revenue").over(Window.partitionBy("customer_id")))

    # Sauvegarde
    df.write.mode("overwrite").parquet(args.dest)
    spark.stop()

if __name__ == "__main__":
    main()
