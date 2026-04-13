"""
Générateur de datasets réalistes
Produit 4 fichiers CSV dans spark/data/raw/ :
  - customers.csv   (2 000 lignes)
  - products.csv    (  500 lignes)
  - sales.csv       (50 000 lignes)
  - inventory.csv   (  500 lignes)

Sources supportées (--source) :
  synthetic       Génération 100 % synthétique (défaut)
  online-retail   Simule le schéma UCI Online Retail II (transactions UK e-commerce)
                  Si le package « kaggle » est installé et que ~/.kaggle/kaggle.json
                  existe, tente le téléchargement réel ; sinon génère un équivalent
                  synthétique fidèle au schéma et aux distributions du dataset.
  blend           Mélange 50/50 données synthétiques + données online-retail

Usage :
    python spark/generate_datasets.py
    python spark/generate_datasets.py --source online-retail --dest spark/data/raw/kaggle
    python spark/generate_datasets.py --source blend --seed 99
"""

import argparse
import os
import sys
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_DEST = os.path.join(BASE_DIR, "spark", "data", "raw")


# ── Helpers ───────────────────────────────────────────────────────────────────

def _rand_dates(rng, start: str, end: str, n: int) -> list:
    s = datetime.strptime(start, "%Y-%m-%d").toordinal()
    e = datetime.strptime(end,   "%Y-%m-%d").toordinal()
    return [datetime.fromordinal(int(d)).strftime("%Y-%m-%d")
            for d in rng.integers(s, e, n)]


def _fmt_id(prefix: str, n: int, width: int, rng) -> np.ndarray:
    nums = rng.integers(10 ** (width - 1), 10 ** width, n)
    return np.array([f"{prefix}-{v}" for v in nums])


# ── Generators ────────────────────────────────────────────────────────────────

def generate_customers(rng: np.random.Generator, n: int = 2_000) -> pd.DataFrame:
    segments  = rng.choice(["silver", "gold", "platinum"], n, p=[0.60, 0.30, 0.10])
    countries = rng.choice(["BE", "FR", "NL", "DE", "LU"], n, p=[0.40, 0.25, 0.15, 0.12, 0.08])
    ages      = np.clip(rng.normal(38, 12, n).astype(int), 18, 80)

    first_names = ["Alice", "Bob", "Claire", "David", "Emma", "François",
                   "Giulia", "Hugo", "Isabelle", "Julien", "Karim", "Laura",
                   "Marc", "Nina", "Olivier", "Pierre", "Quentin", "Rose",
                   "Sophie", "Thomas"]
    last_names  = ["Dupont", "Martin", "Bernard", "Leroy", "Moreau", "Simon",
                   "Laurent", "Lefebvre", "Michel", "Garcia", "David", "Petit",
                   "Robert", "Richard", "Durand", "Thomas", "Roux", "Vincent"]

    fn    = rng.choice(first_names, n)
    ln    = rng.choice(last_names,  n)
    names  = [f"{f} {l}" for f, l in zip(fn, ln)]
    emails = [f"{f.lower()}.{l.lower()}{rng.integers(10, 99)}@example.com"
              for f, l in zip(fn, ln)]

    return pd.DataFrame({
        "customer_id": _fmt_id("CUST", n, 5, rng),
        "name":        names,
        "email":       emails,
        "age":         ages,
        "segment":     segments,
        "country":     countries,
        "signup_date": _rand_dates(rng, "2022-01-01", "2026-01-01", n),
    })


def generate_products(rng: np.random.Generator, n: int = 500) -> pd.DataFrame:
    categories = rng.choice(
        ["Electronics", "Clothing", "Food", "Home"],
        n, p=[0.25, 0.30, 0.20, 0.25],
    )
    brands     = rng.choice(["BrandA", "BrandB", "BrandC", "BrandD", "BrandE", "BrandF"], n)
    unit_cost  = np.round(rng.uniform(2.0, 500.0, n), 2)
    list_price = np.round(unit_cost * rng.uniform(1.3, 2.5, n), 2)

    return pd.DataFrame({
        "product_id":  _fmt_id("PROD", n, 5, rng),
        "name":        [f"{b} {c} #{i+1}" for i, (b, c) in enumerate(zip(brands, categories))],
        "category":    categories,
        "brand":       brands,
        "unit_cost":   unit_cost,
        "list_price":  list_price,
        "launch_date": _rand_dates(rng, "2020-01-01", "2026-01-01", n),
    })


def generate_sales(
    rng: np.random.Generator,
    customers: pd.DataFrame,
    products:  pd.DataFrame,
    n: int = 50_000,
) -> pd.DataFrame:
    # Biais de récence : plus de ventes dans les 6 derniers mois
    end  = datetime(2026, 4, 9)
    span = (end - datetime(2024, 1, 1)).days
    raw_days   = np.clip(rng.exponential(scale=span / 3, size=n).astype(int), 0, span - 1)
    sale_dates = [(end - timedelta(days=int(d))).strftime("%Y-%m-%d") for d in raw_days]

    quantities = np.clip(rng.poisson(lam=2, size=n), 1, 20)
    channels   = rng.choice(["ONLINE", "STORE", "MOBILE"], n, p=[0.45, 0.35, 0.20])
    regions    = rng.choice(["NORTH", "SOUTH", "EAST", "WEST"], n, p=[0.30, 0.25, 0.25, 0.20])
    currencies = rng.choice(["EUR", "EUR", "EUR", "USD", "GBP"], n)

    cust_ids   = rng.choice(customers["customer_id"].values, n)
    prod_rows  = products.sample(n, replace=True, random_state=int(rng.integers(0, 9999))).reset_index(drop=True)
    unit_prices = np.round(prod_rows["list_price"].values * rng.uniform(0.85, 1.05, n), 2)
    revenues   = np.round(quantities * unit_prices, 2)

    order_nums = rng.choice(range(1_000_000, 9_999_999), n, replace=False)
    order_ids  = [f"ORD-{v}" for v in order_nums]

    return pd.DataFrame({
        "order_id":    order_ids,
        "customer_id": cust_ids,
        "product_id":  prod_rows["product_id"].values,
        "sale_date":   sale_dates,
        "quantity":    quantities,
        "unit_price":  unit_prices,
        "channel":     channels,
        "region":      regions,
        "currency":    currencies,
        "revenue":     revenues,
    })


def generate_inventory(rng: np.random.Generator, products: pd.DataFrame) -> pd.DataFrame:
    n         = len(products)
    stock_qty = rng.integers(0, 501, n)
    # 5 % de ruptures de stock naturelles
    stock_qty[rng.random(n) < 0.05] = 0

    return pd.DataFrame({
        "product_id":        products["product_id"].values,
        "warehouse":         rng.choice(["Paris", "Lyon", "Marseille", "Bordeaux"], n),
        "stock_qty":         stock_qty,
        "reorder_point":     rng.integers(10, 101, n),
        "last_restock_date": _rand_dates(rng, "2025-01-01", "2026-04-01", n),
    })


# ── Online Retail II (UCI / Kaggle) ───────────────────────────────────────────

# Constantes fidèles aux distributions réelles du dataset UCI Online Retail II
# (541 909 transactions, 8 colonnes, e-commerce UK 2009-2011)
_OR_COUNTRIES = [
    ("United Kingdom",  0.90), ("Germany", 0.02), ("France", 0.02),
    ("EIRE", 0.01), ("Spain", 0.01), ("Netherlands", 0.01),
    ("Belgium", 0.01), ("Switzerland", 0.005), ("Portugal", 0.005),
    ("Australia", 0.005), ("Norway", 0.003), ("Italy", 0.002),
    ("Channel Islands", 0.002), ("Finland", 0.001), ("Cyprus", 0.001),
]
_OR_DESCRIPTIONS = [
    "WHITE HANGING HEART T-LIGHT HOLDER", "REGENCY CAKESTAND 3 TIER",
    "JUMBO BAG RED RETROSPOT", "PARTY BUNTING", "LUNCH BAG RED RETROSPOT",
    "ASSORTED COLOUR BIRD ORNAMENT", "PACK OF 72 RETROSPOT CAKE CASES",
    "SMALL CERAMIC TOP STORAGE JAR", "STRAWBERRY CERAMIC TRINKET BOX",
    "SET OF 3 CAKE TINS PANTRY DESIGN", "HEART OF WICKER LARGE",
    "GLASS STAR FROSTED T-LIGHT HOLDER", "PAPER CHAIN KIT 50'S CHRISTMAS",
    "METAL SIGN TAKE IT OR LEAVE IT", "ALARM CLOCK BAKELIKE IVORY",
    "DOORMAT NEW MULTICOL GRAPHIC", "CHILDRENS CUTLERY DOLLY GIRL",
    "VINTAGE BILLBOARD CHRISTMAS BAR", "WOODEN PICTURE FRAME WHITE FINISH",
    "CERAMIC STORAGE JAR PANTRY DESIGN",
]


def generate_online_retail(rng: np.random.Generator, n: int = 50_000) -> pd.DataFrame:
    """
    Génère un dataset synthétique au format UCI Online Retail II.

    Colonnes : InvoiceNo, StockCode, Description, Quantity,
               InvoiceDate, UnitPrice, CustomerID, Country

    Ce format est identique au dataset Kaggle « Online Retail II »
    (https://www.kaggle.com/datasets/mashlyn/online-retail-ii-uci).
    """
    countries, probs = zip(*_OR_COUNTRIES)
    probs = np.array(probs)
    probs = probs / probs.sum()

    # InvoiceNo : format C commence par 'C' pour les retours (5 % de cancels)
    inv_nums  = rng.integers(489_000, 581_000, n)
    is_cancel = rng.random(n) < 0.05
    invoice_no = [f"C{v}" if c else str(v)
                  for v, c in zip(inv_nums, is_cancel)]

    # StockCode : 5 chiffres + lettre optionnelle
    stock_nums = rng.integers(10_000, 99_999, n)
    stock_sfx  = rng.choice(["", "A", "B", "C", "D", " "], n,
                             p=[0.70, 0.10, 0.08, 0.07, 0.03, 0.02])
    stock_code = [f"{num}{sfx}" for num, sfx in zip(stock_nums, stock_sfx)]

    descriptions = rng.choice(_OR_DESCRIPTIONS, n)

    # Quantity : retours négatifs, achats positifs
    qty_pos = np.clip(rng.poisson(lam=6, size=n), 1, 80).astype(int)
    qty     = np.where(is_cancel, -qty_pos, qty_pos)

    # InvoiceDate : entre 2009-12-01 et 2011-12-09
    start_ord = datetime(2009, 12, 1).toordinal()
    end_ord   = datetime(2011, 12, 9).toordinal()
    dates     = [datetime.fromordinal(int(d)).strftime("%Y-%m-%d %H:%M:%S")
                 for d in rng.integers(start_ord, end_ord, n)]

    unit_price = np.round(rng.exponential(scale=3.5, size=n).clip(0.001, 50.0), 2)
    # ~5 % de CustomerID nuls (invités non connectés)
    cust_ids   = rng.integers(12_000, 18_500, n).astype(float)
    cust_ids[rng.random(n) < 0.05] = np.nan
    cust_strs  = [str(int(c)) if not np.isnan(c) else "" for c in cust_ids]

    country = rng.choice(countries, n, p=probs)

    return pd.DataFrame({
        "InvoiceNo":   invoice_no,
        "StockCode":   stock_code,
        "Description": descriptions,
        "Quantity":    qty,
        "InvoiceDate": dates,
        "UnitPrice":   unit_price,
        "CustomerID":  cust_strs,
        "Country":     country,
    })


def _try_kaggle_download(dataset: str, dest: str) -> bool:
    """
    Tente de télécharger un dataset Kaggle via l'API officielle.
    Retourne True si le téléchargement a réussi, False sinon.
    """
    try:
        from kaggle.api.kaggle_api_extended import KaggleApiClient  # type: ignore[import]
        api = KaggleApiClient()
        api.authenticate()
        api.dataset_download_files(dataset, path=dest, unzip=True)
        print(f"  [Kaggle API] Dataset '{dataset}' téléchargé → {dest}")
        return True
    except ImportError:
        print("  [Kaggle API] package 'kaggle' non installé → mode synthétique",
              file=sys.stderr)
    except Exception as exc:
        print(f"  [Kaggle API] Échec téléchargement ({exc}) → mode synthétique",
              file=sys.stderr)
    return False


# ── Blending helper ───────────────────────────────────────────────────────────

def _blend_sales(synthetic: pd.DataFrame, online: pd.DataFrame,
                 rng: np.random.Generator) -> pd.DataFrame:
    """
    Fusionne les données synthétiques (schéma interne) et Online Retail
    (schéma UCI) en un seul DataFrame au schéma interne.

    Les colonnes Online Retail sont mappées vers le schéma sales interne :
      InvoiceNo  → order_id
      CustomerID → customer_id  (CUST-XXXXX si manquant)
      StockCode  → product_id
      InvoiceDate → sale_date  (date uniquement)
      Quantity   → quantity    (abs, retours exclus)
      UnitPrice  → unit_price
    """
    or_clean = online[online["Quantity"] > 0].copy()
    n_blend  = min(len(or_clean), len(synthetic) // 2)
    or_sample = or_clean.sample(n=n_blend, random_state=int(rng.integers(0, 9999)))

    channels  = rng.choice(["ONLINE", "STORE", "MOBILE"], n_blend, p=[0.45, 0.35, 0.20])
    regions   = rng.choice(["NORTH", "SOUTH", "EAST", "WEST"], n_blend)
    currencies = ["GBP"] * n_blend  # Online Retail est libellé en GBP

    cust_ids = [
        f"CUST-{c}" if c else f"CUST-{rng.integers(10000, 99999)}"
        for c in or_sample["CustomerID"].values
    ]
    prod_ids  = [f"PROD-{sc[:5].zfill(5)}" for sc in or_sample["StockCode"].values]
    sale_dates = [str(d)[:10] for d in or_sample["InvoiceDate"].values]
    qty       = or_sample["Quantity"].values.astype(int)
    up        = or_sample["UnitPrice"].values.astype(float)
    revenues  = np.round(qty * up, 2)
    order_nums = rng.choice(range(9_000_000, 9_999_999), n_blend, replace=False)
    order_ids  = [f"ORD-{v}" for v in order_nums]

    or_mapped = pd.DataFrame({
        "order_id":    order_ids,
        "customer_id": cust_ids,
        "product_id":  prod_ids,
        "sale_date":   sale_dates,
        "quantity":    qty,
        "unit_price":  up,
        "channel":     channels,
        "region":      regions,
        "currency":    currencies,
        "revenue":     revenues,
        "_source":     ["online-retail"] * n_blend,
    })

    syn_copy = synthetic.copy()
    syn_copy["_source"] = "synthetic"
    blended = pd.concat([syn_copy, or_mapped], ignore_index=True)
    blended = blended.sample(frac=1, random_state=int(rng.integers(0, 9999))).reset_index(drop=True)
    return blended


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Génère les datasets réalistes")
    parser.add_argument("--seed",   type=int, default=42,     help="Graine aléatoire (défaut: 42)")
    parser.add_argument("--dest",   default=DEFAULT_DEST,     help="Répertoire de sortie")
    parser.add_argument(
        "--source",
        choices=["synthetic", "online-retail", "blend"],
        default="synthetic",
        help=(
            "Source des données : "
            "'synthetic' (défaut) — génération 100%% synthétique ; "
            "'online-retail' — format UCI Online Retail II (Kaggle) ; "
            "'blend' — mélange 50/50 synthétique + online-retail"
        ),
    )
    args = parser.parse_args()

    os.makedirs(args.dest, exist_ok=True)
    rng = np.random.default_rng(args.seed)

    print(f"[generate_datasets] seed={args.seed}  source={args.source}  dest={args.dest}")

    # ── Toujours générer customers / products / inventory (schéma interne) ─────
    customers = generate_customers(rng)
    customers.to_csv(os.path.join(args.dest, "customers.csv"), index=False)
    print(f"  customers.csv   — {len(customers):>6,} lignes")

    products = generate_products(rng)
    products.to_csv(os.path.join(args.dest, "products.csv"), index=False)
    print(f"  products.csv    — {len(products):>6,} lignes")

    inventory = generate_inventory(rng, products)
    inventory.to_csv(os.path.join(args.dest, "inventory.csv"), index=False)
    print(f"  inventory.csv   — {len(inventory):>6,} lignes")

    # ── Sales selon la source demandée ─────────────────────────────────────────
    if args.source == "synthetic":
        sales = generate_sales(rng, customers, products)
        sales.to_csv(os.path.join(args.dest, "sales.csv"), index=False)
        print(f"  sales.csv       — {len(sales):>6,} lignes  [synthétique]")

    elif args.source == "online-retail":
        # Essai téléchargement Kaggle, sinon simulation
        kaggle_ok = _try_kaggle_download(
            "mashlyn/online-retail-ii-uci", args.dest)
        if not kaggle_ok:
            or_df = generate_online_retail(rng, n=541_909)
            or_df.to_csv(os.path.join(args.dest, "online_retail.csv"), index=False)
            print(f"  online_retail.csv — {len(or_df):>6,} lignes  [simulation UCI Online Retail II]")

    elif args.source == "blend":
        synthetic_sales = generate_sales(rng, customers, products)
        online_retail   = generate_online_retail(rng, n=50_000)
        blended         = _blend_sales(synthetic_sales, online_retail, rng)
        blended.to_csv(os.path.join(args.dest, "sales.csv"), index=False)
        online_retail.to_csv(os.path.join(args.dest, "online_retail_raw.csv"), index=False)
        n_syn = (blended["_source"] == "synthetic").sum()
        n_or  = (blended["_source"] == "online-retail").sum()
        print(f"  sales.csv       — {len(blended):>6,} lignes  "
              f"[blend: {n_syn:,} synthétique + {n_or:,} online-retail]")
        print(f"  online_retail_raw.csv — {len(online_retail):>6,} lignes  [UCI Online Retail II brut]")

    print("[generate_datasets] Terminé.")


if __name__ == "__main__":
    main()
