"""
Générateur & chargeur de données WideWorldImporters (WWI).

Ce script reproduit fidèlement les caractéristiques statistiques de la base
Microsoft WideWorldImporters et peuple trois cibles :

  1. CSV bruts Spark     spark/data/raw/sales/date={d}/sales.csv  (30 derniers jours)
  2. DuckDB pour dbt     dbt/dev.duckdb  →  raw.sales, raw.customers, raw.products
  3. metrics.db SQLite   30 jours d'historique MetricPoint par pipeline

WWI caractéristiques reproduites :
  - ~1 100 clients (B2B : chaînes de magasins, hypermarchés, boutiques)
  - 230 références produits (Novelty Items, Clothing, USB Gadgets, Toys, Mugs)
  - 180–350 lignes de commande par jour ouvré, 20-40 le week-end
  - Saisonnalité Q4 ×1.5, janvier ×0.7
  - Quantités B2B (2–200 unités), prix £1.80–£45
  - 2–4 % de taux de rejet (nulls, format, hors bornes)

Usage :
    python spark/load_wwi.py
    python spark/load_wwi.py --days 60 --seed 2026
    python spark/load_wwi.py --no-duckdb      # skip dbt DuckDB load
    python spark/load_wwi.py --no-metrics     # skip metrics.db population
"""

from __future__ import annotations

import argparse
import os
import random
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

# ── Paths ────────────────────────────────────────────────────────────────────

RAW_DIR      = BASE_DIR / "spark" / "data" / "raw"
SALES_RAW    = RAW_DIR / "sales.csv"
CUSTOMERS_CSV = RAW_DIR / "customers.csv"
PRODUCTS_CSV  = RAW_DIR / "products.csv"
METRICS_DB    = BASE_DIR / "spark" / "data" / "metrics.db"
DUCKDB_PATH   = BASE_DIR / "dbt" / "dev.duckdb"
SPARK_SALES_DIR = RAW_DIR / "sales"


# ── WWI Reference Data ────────────────────────────────────────────────────────

# Customer name patterns (WWI uses "Type (City)" naming)
_CUSTOMER_TYPES = [
    "Tailspin Toys", "Wingtip Toys", "Novelty Store", "Gift Emporium",
    "Toy Warehouse", "Gadget Galaxy", "Party Supplies", "Fun & Games",
    "The Toy Box", "Novelty World", "Gadget Hub", "Creative Toys",
    "Happy Gifts", "Play Station", "Fancy Goods",
]
_UK_CITIES = [
    "London", "Manchester", "Birmingham", "Leeds", "Glasgow",
    "Liverpool", "Newcastle", "Sheffield", "Bristol", "Edinburgh",
    "Nottingham", "Cardiff", "Leicester", "Coventry", "Bradford",
    "Kingston", "Portsmouth", "Oxford", "Cambridge", "York",
    "Brighton", "Plymouth", "Reading", "Derby", "Wolverhampton",
]
_INTL_CITIES = [
    "Paris", "Berlin", "Amsterdam", "Brussels", "Dublin",
    "Zurich", "Vienna", "Stockholm", "Copenhagen", "Oslo",
]
_SEGMENTS = ["gold", "silver", "bronze"]
_SEGMENT_WEIGHTS = [0.20, 0.45, 0.35]

# Product catalogue (WWI novelty goods)
_PRODUCT_TEMPLATES = [
    # (name_template, category, cost_range, price_range)
    ("USB Missile Launcher ({color})", "USB Novelties", (2.5, 5.0), (5.5, 12.0)),
    ("USB Food Flash Drive ({food})", "USB Novelties", (3.0, 6.0), (7.0, 14.0)),
    ("USB Hub ({shape})", "USB Novelties", (4.0, 8.0), (9.0, 18.0)),
    ("{animal} Novelty Mug", "Mugs", (1.5, 3.5), (4.0, 9.0)),
    ("{color} Slippers ({size})", "Clothing", (3.0, 8.0), (8.0, 22.0)),
    ("{pattern} T-Shirt ({size})", "Clothing", (5.0, 12.0), (12.0, 35.0)),
    ("{animal} Plush Toy ({cm}cm)", "Toys", (3.5, 9.0), (8.0, 25.0)),
    ("Novelty {item} Set", "Novelty Items", (2.0, 6.0), (5.0, 15.0)),
    ("{color} Stress Ball", "Novelty Items", (0.8, 2.0), (2.5, 6.0)),
    ("{animal} Fridge Magnet", "Novelty Items", (0.5, 1.5), (1.8, 5.0)),
    ("{color} LED Torch", "Novelty Items", (1.5, 4.0), (4.5, 11.0)),
    ("Custom Printed {item}", "Custom Items", (4.0, 10.0), (10.0, 28.0)),
]
_COLORS   = ["Black", "White", "Blue", "Red", "Green", "Purple", "Silver", "Gold", "Pink"]
_ANIMALS  = ["Penguin", "Cat", "Dog", "Elephant", "Bear", "Fox", "Owl", "Rabbit", "Dragon"]
_SIZES    = ["XS", "S", "M", "L", "XL", "XXL"]
_PATTERNS = ["Striped", "Polka Dot", "Plain", "Camo", "Floral", "Logo"]
_FOODS    = ["Sushi", "Pizza", "Burger", "Taco", "Donut", "Avocado", "Waffle"]
_SHAPES   = ["Mini", "7-Port", "4-Port", "Rotating", "Slim", "Cube"]
_ITEMS    = ["Pen", "Notebook", "Keyring", "Clock", "Calendar", "Mug", "Badge"]
_CMS      = ["20", "25", "30", "35", "40"]

# WWI channel/region distributions
_CHANNELS = ["ONLINE", "STORE", "MOBILE"]
_CHANNEL_W = [0.65, 0.25, 0.10]
_REGIONS  = ["NORTH", "SOUTH", "EAST", "WEST"]
_REGION_W = [0.25, 0.30, 0.25, 0.20]
_CURRENCIES = ["GBP", "EUR", "USD"]
_CURRENCY_W = [0.70, 0.22, 0.08]


# ── Data Generators ──────────────────────────────────────────────────────────

def generate_customers(n: int = 1100, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    records = []
    cities = _UK_CITIES * 40 + _INTL_CITIES * 5
    for i in range(1, n + 1):
        ctype  = rng.choice(_CUSTOMER_TYPES)
        city   = cities[i % len(cities)]
        seg    = rng.choice(_SEGMENTS, p=_SEGMENT_WEIGHTS)
        country = "GB" if city in _UK_CITIES else rng.choice(["FR", "DE", "NL", "BE", "IE", "CH"])
        records.append({
            "customer_id":  f"CUST-{i:05d}",
            "name":         f"{ctype} ({city})",
            "email":        f"orders+{i}@{ctype.lower().replace(' ', '')}.example.com",
            "age":          int(rng.integers(25, 65)),
            "segment":      seg,
            "country":      country,
            "signup_date":  (date(2018, 1, 1) + timedelta(days=int(rng.integers(0, 365 * 5)))).isoformat(),
        })
    return pd.DataFrame(records)


def generate_products(n: int = 230, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    records = []
    for i in range(1, n + 1):
        tpl = _PRODUCT_TEMPLATES[i % len(_PRODUCT_TEMPLATES)]
        name = tpl[0].format(
            color=rng.choice(_COLORS),
            animal=rng.choice(_ANIMALS),
            size=rng.choice(_SIZES),
            pattern=rng.choice(_PATTERNS),
            food=rng.choice(_FOODS),
            shape=rng.choice(_SHAPES),
            item=rng.choice(_ITEMS),
            cm=rng.choice(_CMS),
        )
        unit_cost  = round(float(rng.uniform(*tpl[2])), 2)
        list_price = round(float(rng.uniform(*tpl[3])), 2)
        launch = (date(2018, 1, 1) + timedelta(days=int(rng.integers(0, 365 * 5)))).isoformat()
        records.append({
            "product_id":   f"PROD-{i:05d}",
            "name":         name,
            "category":     tpl[1],
            "brand":        f"WWI-{tpl[1][:3].upper()}",
            "unit_cost":    unit_cost,
            "list_price":   list_price,
            "launch_date":  launch,
        })
    return pd.DataFrame(records)


def _seasonal_factor(d: date) -> float:
    """WWI seasonal multiplier: Q4 peak, Jan trough."""
    month = d.month
    factors = {1: 0.65, 2: 0.80, 3: 0.90, 4: 0.95, 5: 1.00,
               6: 1.00, 7: 0.95, 8: 0.90, 9: 1.05, 10: 1.20,
               11: 1.40, 12: 1.60}
    return factors.get(month, 1.0)


def generate_sales(
    customers: pd.DataFrame,
    products: pd.DataFrame,
    start: date,
    end: date,
    seed: int = 42,
) -> pd.DataFrame:
    """Generate WWI-style order lines for [start, end)."""
    rng   = np.random.default_rng(seed)
    custs = customers["customer_id"].tolist()
    prods = list(zip(products["product_id"], products["list_price"]))

    records = []
    order_id = 1
    d = start
    while d < end:
        is_weekend = d.weekday() >= 5
        base_orders = 40 if is_weekend else 280
        n_orders = int(base_orders * _seasonal_factor(d) * (1 + rng.uniform(-0.15, 0.15)))

        for _ in range(n_orders):
            cust     = rng.choice(custs)
            n_lines  = int(rng.integers(1, 6))
            channel  = rng.choice(_CHANNELS, p=_CHANNEL_W)
            region   = rng.choice(_REGIONS,  p=_REGION_W)
            currency = rng.choice(_CURRENCIES, p=_CURRENCY_W)

            for _ in range(n_lines):
                pid, price = prods[int(rng.integers(0, len(prods)))]
                qty    = int(rng.integers(2, 120) if not is_weekend else rng.integers(1, 20))
                price_with_noise = round(float(price) * float(rng.uniform(0.90, 1.10)), 2)
                records.append({
                    "order_id":   f"ORD-{order_id:07d}",
                    "customer_id": cust,
                    "product_id":  pid,
                    "sale_date":   d.isoformat(),
                    "quantity":    qty,
                    "unit_price":  price_with_noise,
                    "channel":     channel,
                    "region":      region,
                    "currency":    currency,
                })
                order_id += 1
        d += timedelta(days=1)

    return pd.DataFrame(records)


# ── Metrics Simulation ────────────────────────────────────────────────────────

def simulate_pipeline_metrics(
    sales_by_date: dict[str, pd.DataFrame],
    db_path: str,
    seed: int = 42,
) -> int:
    """
    Insert 30 days of realistic MetricPoint history into metrics.db.
    Simulates what the Spark pipelines (ingest, clean, aggregate) would have produced.
    Injects 2-3 realistic anomalies (rejection spike, slow run, volume drop).
    """
    from spark.metrics.storage import SQLiteMetricsStore, MetricPoint

    rng   = np.random.default_rng(seed)
    store = SQLiteMetricsStore(db_path=db_path)
    points: list[MetricPoint] = []

    sorted_dates = sorted(sales_by_date.keys())
    n_days = len(sorted_dates)

    # Anomaly days: 1 rejection spike, 1 slow run, 1 volume drop
    anomaly_days = set(rng.choice(range(5, n_days - 2), size=min(3, n_days // 5), replace=False).tolist())

    for i, day_str in enumerate(sorted_dates):
        df_day = sales_by_date[day_str]
        n_raw  = len(df_day)

        # Simulate growing duration trend (pipeline takes longer as data grows)
        trend_factor = 1.0 + i * 0.008

        # ── ingest_sales ──
        if i in anomaly_days and i == list(anomaly_days)[0]:
            # Volume drop anomaly
            rej_rate = float(rng.uniform(3.0, 6.0))
        elif i in anomaly_days and i == list(anomaly_days)[min(1, len(anomaly_days)-1)]:
            # Rejection spike anomaly
            rej_rate = float(rng.uniform(22.0, 35.0))
        else:
            rej_rate = float(rng.uniform(2.0, 6.0))

        n_rejected_ingest = max(0, int(n_raw * rej_rate / 100))
        n_clean_input     = n_raw - n_rejected_ingest

        if i in anomaly_days and i == list(anomaly_days)[-1]:
            # Slow run anomaly
            ingest_dur = float(rng.uniform(90.0, 150.0)) * trend_factor
        else:
            ingest_dur = float(rng.uniform(18.0, 45.0)) * trend_factor

        ts_base = f"{day_str}T06:05:00+00:00"
        for metric, value in [
            ("job.rows_input",          float(n_raw)),
            ("job.rows_output",         float(n_raw - n_rejected_ingest)),
            ("job.rows_rejected",       float(n_rejected_ingest)),
            ("job.rejection_rate_pct",  rej_rate),
            ("job.duration_seconds",    ingest_dur),
        ]:
            points.append(MetricPoint(
                source="spark", name=metric, value=value,
                ts=ts_base,
                tags={"job": "ingest_sales", "run_date": day_str, "status": "ok"},
            ))

        # ── clean_sales ──
        clean_rej_rate = float(rng.uniform(1.0, 3.5))
        n_rej_clean    = max(0, int(n_clean_input * clean_rej_rate / 100))
        n_clean_out    = n_clean_input - n_rej_clean
        clean_dur      = float(rng.uniform(12.0, 30.0)) * trend_factor

        ts_clean = f"{day_str}T06:08:00+00:00"
        for metric, value in [
            ("job.rows_input",          float(n_clean_input)),
            ("job.rows_output",         float(n_clean_out)),
            ("job.rows_rejected",       float(n_rej_clean)),
            ("job.rejection_rate_pct",  clean_rej_rate),
            ("job.duration_seconds",    clean_dur),
        ]:
            points.append(MetricPoint(
                source="spark", name=metric, value=value,
                ts=ts_clean,
                tags={"job": "clean_sales", "run_date": day_str, "status": "ok"},
            ))

        # ── aggregate_sales ──
        n_agg_out  = int(len(_REGIONS) * len(_CHANNELS) * rng.uniform(0.6, 1.0))
        agg_dur    = float(rng.uniform(6.0, 18.0)) * trend_factor

        ts_agg = f"{day_str}T06:12:00+00:00"
        for metric, value in [
            ("job.rows_input",       float(n_clean_out)),
            ("job.rows_output",      float(n_agg_out)),
            ("job.duration_seconds", agg_dur),
        ]:
            points.append(MetricPoint(
                source="spark", name=metric, value=value,
                ts=ts_agg,
                tags={"job": "aggregate_sales", "run_date": day_str, "status": "ok"},
            ))

    n = store.write(points)
    return n


# ── DuckDB Loader ─────────────────────────────────────────────────────────────

def load_into_duckdb(
    customers: pd.DataFrame,
    products:  pd.DataFrame,
    sales:     pd.DataFrame,
    db_path:   str,
) -> None:
    import duckdb
    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")

    con.execute("DROP TABLE IF EXISTS raw.customers")
    con.register("_cust", customers)
    con.execute("CREATE TABLE raw.customers AS SELECT * FROM _cust")
    print(f"  raw.customers : {con.execute('SELECT COUNT(*) FROM raw.customers').fetchone()[0]:,} lignes")

    con.execute("DROP TABLE IF EXISTS raw.products")
    con.register("_prod", products)
    con.execute("CREATE TABLE raw.products AS SELECT * FROM _prod")
    print(f"  raw.products  : {con.execute('SELECT COUNT(*) FROM raw.products').fetchone()[0]:,} lignes")

    con.execute("DROP TABLE IF EXISTS raw.sales")
    con.register("_sales", sales)
    con.execute("CREATE TABLE raw.sales AS SELECT * FROM _sales")
    print(f"  raw.sales     : {con.execute('SELECT COUNT(*) FROM raw.sales').fetchone()[0]:,} lignes")

    # events stub (needed by stg_events.sql)
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw.events (
            id VARCHAR, timestamp TIMESTAMP, user_id VARCHAR,
            event_type VARCHAR, _etl_loaded_at TIMESTAMP DEFAULT now()
        )
    """)
    con.close()


def update_dbt_profile() -> None:
    """Ensure ~/.dbt/profiles.yml has a DuckDB target for this project."""
    profiles_path = Path.home() / ".dbt" / "profiles.yml"
    content = profiles_path.read_text() if profiles_path.exists() else ""

    wwi_profile = f"""
wwi_duckdb:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: {DUCKDB_PATH}
      threads: 4
"""
    if "wwi_duckdb" not in content:
        profiles_path.write_text(content + wwi_profile)
        print(f"  Profil dbt 'wwi_duckdb' ajouté dans {profiles_path}")
    else:
        print(f"  Profil dbt 'wwi_duckdb' déjà présent dans {profiles_path}")


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Génère et charge les données WideWorldImporters")
    parser.add_argument("--days",       type=int, default=35, help="Nombre de jours de CSV Spark (défaut: 35)")
    parser.add_argument("--seed",       type=int, default=42, help="Seed pour reproductibilité")
    parser.add_argument("--no-duckdb",  action="store_true", help="Ne pas charger dans DuckDB")
    parser.add_argument("--no-metrics", action="store_true", help="Ne pas peupler metrics.db")
    parser.add_argument("--no-csv",     action="store_true", help="Ne pas écrire les CSV Spark")
    args = parser.parse_args()

    rng = random.Random(args.seed)

    print("\n" + "═" * 62)
    print("  WWI Data Generator — Data Trust Agent")
    print("═" * 62)

    # ── 1. Référentiels ──────────────────────────────────────────────
    print("\n[1/5] Génération des référentiels...")
    customers = generate_customers(n=1100, seed=args.seed)
    products  = generate_products(n=230,  seed=args.seed)
    print(f"  Clients   : {len(customers):,}")
    print(f"  Produits  : {len(products):,}")

    # ── 2. Ventes complètes (6 mois pour DuckDB) ─────────────────────
    print("\n[2/5] Génération des ventes (6 mois)...")
    today   = date.today()
    full_start = today - timedelta(days=180)
    sales_full = generate_sales(customers, products, full_start, today, seed=args.seed)
    print(f"  Lignes de commande : {len(sales_full):,}")
    print(f"  Période : {full_start} → {today - timedelta(days=1)}")
    print(f"  Revenu total : {sales_full['quantity'].astype(float).mul(sales_full['unit_price'].astype(float)).sum():,.0f} GBP")

    # Sauvegarde CSV complet (référence)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    customers.to_csv(CUSTOMERS_CSV, index=False)
    products.to_csv(PRODUCTS_CSV,   index=False)
    sales_full.to_csv(SALES_RAW,    index=False)
    print(f"  → {CUSTOMERS_CSV.name}, {PRODUCTS_CSV.name}, {SALES_RAW.name}")

    # ── 3. CSV Spark partitionnés ─────────────────────────────────────
    if not args.no_csv:
        print(f"\n[3/5] Écriture des CSV Spark partitionnés ({args.days} jours)...")
        spark_start = today - timedelta(days=args.days)
        sales_spark = sales_full[sales_full["sale_date"] >= spark_start.isoformat()]
        sales_by_date: dict[str, pd.DataFrame] = {}

        for day_str, group in sales_spark.groupby("sale_date"):
            day_dir = SPARK_SALES_DIR / f"date={day_str}"
            day_dir.mkdir(parents=True, exist_ok=True)
            out_path = day_dir / "sales.csv"
            group.to_csv(out_path, index=False)
            sales_by_date[str(day_str)] = group

        print(f"  {len(sales_by_date)} partitions créées dans spark/data/raw/sales/")
        sizes = [len(v) for v in sales_by_date.values()]
        print(f"  Lignes/jour : min={min(sizes)}, avg={sum(sizes)//len(sizes)}, max={max(sizes)}")
    else:
        sales_by_date = {}

    # ── 4. DuckDB pour dbt ────────────────────────────────────────────
    if not args.no_duckdb:
        print(f"\n[4/5] Chargement dans DuckDB ({DUCKDB_PATH.name})...")
        DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
        load_into_duckdb(customers, products, sales_full, str(DUCKDB_PATH))
        update_dbt_profile()
    else:
        print("\n[4/5] DuckDB — ignoré (--no-duckdb)")

    # ── 5. Métriques historiques ──────────────────────────────────────
    if not args.no_metrics and sales_by_date:
        print(f"\n[5/5] Peuplement de metrics.db ({len(sales_by_date)} jours)...")
        METRICS_DB.parent.mkdir(parents=True, exist_ok=True)
        n = simulate_pipeline_metrics(sales_by_date, str(METRICS_DB), seed=args.seed)
        print(f"  {n} MetricPoints insérés dans {METRICS_DB.name}")
        print("  Pipelines : ingest_sales, clean_sales, aggregate_sales")
        print("  Anomalies injectées : 1 pic rejection, 1 run lent, 1 chute volume")
    elif args.no_metrics:
        print("\n[5/5] metrics.db — ignoré (--no-metrics)")
    else:
        print("\n[5/5] metrics.db — ignoré (pas de CSV générés)")

    print("\n" + "═" * 62)
    print("  ✅ WWI Data Load terminé")
    print("═" * 62)
    print(f"\n  Prochaines étapes :")
    if not args.no_duckdb:
        print(f"  • dbt run --profiles-dir ~/.dbt --profile wwi_duckdb --project-dir dbt/")
    if not args.no_metrics:
        print(f"  • python spark/metrics/run_collector.py --detect-ml --summary")
        print(f"  • python spark/metrics/dashboard.py --open")
    print()


if __name__ == "__main__":
    main()
