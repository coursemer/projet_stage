"""
inject_to_db.py — Injection d'anomalies contrôlées dans la base de métriques.

Injecte des anomalies dans le CSV de ventes, calcule les MetricPoints correspondants
et les écrit dans spark/data/metrics.db (SQLite local, remplace InfluxDB quand Docker est arrêté).

Usage :
    python inject_to_db.py                                        # mode interactif
    python inject_to_db.py --pipeline clean_sales \\
                           --types nulls,out_of_range \\
                           --rate 0.10 --seed 42
    python inject_to_db.py --list-types
    python inject_to_db.py --dry-run --types nulls,duplicates
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

import pandas as pd

from spark.anomaly_injector         import AnomalyInjector, ANOMALY_CATALOG, ALL_TYPES
from spark.metrics.storage          import MetricPoint, SQLiteMetricsStore, DEFAULT_DB
from spark.metrics.anomaly_detector import AnomalyDetector
from spark.metrics.alert_manager    import AlertManager

# ── Constantes ────────────────────────────────────────────────────────────────

RAW_SALES   = os.path.join(BASE_DIR, "spark", "data", "raw", "sales.csv")
PIPELINE_DESCRIPTIONS = {
    "ingest_sales":    "Ingestion brute — anomalies injectées sur les données sources",
    "clean_sales":     "Nettoyage — anomalies simulant une dégradation de la source upstream",
    "aggregate_sales": "Agrégation — anomalies simulant une corruption en cours de traitement",
}


def _hr(char: str = "─", width: int = 70) -> None:
    print(char * width)


# ══════════════════════════════════════════════════════════════════════════════
# Conversion DataFrame → MetricPoints
# ══════════════════════════════════════════════════════════════════════════════

def df_to_metric_points(
    df_clean: pd.DataFrame,
    df_dirty: pd.DataFrame,
    pipeline: str,
    run_date: str,
    duration_seconds: float,
    report: dict,
) -> list[MetricPoint]:
    """
    Calcule les MetricPoints à écrire dans le store depuis les DataFrames clean/dirty.

    Points émis :
      job.rows_output          — nombre de lignes après injection
      job.rows_rejected        — lignes rejetées (nulls sur clés, type_mismatch…)
      job.rejection_rate_pct   — taux de rejet %
      job.duration_seconds     — durée simulée
      job.null_rate_pct        — taux de valeurs nulles global %
      job.duplicate_rate_pct   — taux de doublons %
      job.<col>.null_rate      — taux de nulls par colonne numérique
      job.<col>.mean           — moyenne par colonne numérique
    """
    ts  = datetime.now(timezone.utc).isoformat()
    tags = {"job": pipeline, "run_date": run_date}
    pts: list[MetricPoint] = []

    n_clean = len(df_clean)
    n_dirty = len(df_dirty)

    # Lignes rejetées = nulls sur colonnes clés (order_id, customer_id, product_id)
    key_cols = [c for c in ["order_id", "customer_id", "product_id"] if c in df_dirty.columns]
    n_rejected = int(df_dirty[key_cols].isna().any(axis=1).sum()) if key_cols else 0
    rejection_rate = round(n_rejected / max(n_dirty, 1) * 100, 4)

    # Taux de doublons
    n_dupes = int(df_dirty.duplicated().sum())
    dup_rate = round(n_dupes / max(n_dirty, 1) * 100, 4)

    # Taux de nulls global
    null_rate_pct = round(df_dirty.isna().mean().mean() * 100, 4)

    pts += [
        MetricPoint(source="spark", name="job.rows_output",        value=float(n_dirty),       ts=ts, tags=tags),
        MetricPoint(source="spark", name="job.rows_rejected",       value=float(n_rejected),    ts=ts, tags=tags),
        MetricPoint(source="spark", name="job.rejection_rate_pct",  value=rejection_rate,       ts=ts, tags=tags),
        MetricPoint(source="spark", name="job.duration_seconds",    value=duration_seconds,     ts=ts, tags=tags),
        MetricPoint(source="spark", name="job.null_rate_pct",       value=null_rate_pct,        ts=ts, tags=tags),
        MetricPoint(source="spark", name="job.duplicate_rate_pct",  value=dup_rate,             ts=ts, tags=tags),
    ]

    # Stats par colonne numérique
    num_cols = df_dirty.select_dtypes(include="number").columns.tolist()
    for col in num_cols[:5]:   # max 5 colonnes pour ne pas surcharger
        series = pd.to_numeric(df_dirty[col], errors="coerce").dropna()
        if len(series) > 0:
            pts.append(MetricPoint(
                source="spark",
                name=f"job.{col}.null_rate",
                value=round(df_dirty[col].isna().mean() * 100, 4),
                ts=ts, tags=tags,
            ))
            pts.append(MetricPoint(
                source="spark",
                name=f"job.{col}.mean",
                value=round(float(series.mean()), 4),
                ts=ts, tags=tags,
            ))

    # Métrique d'injection (trace de l'événement)
    injected_list  = report.get("anomalies_injected", [])
    total_injected = sum(len(r.get("row_indices", [])) for r in injected_list)
    pts.append(MetricPoint(
        source="spark",
        name="job.anomalies_injected",
        value=float(total_injected),
        ts=ts,
        tags={**tags, "types": ",".join(sorted({r["type"] for r in injected_list}))},
        extra={"n_events": len(injected_list)},
    ))

    return pts


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def run(
    pipeline:   str,
    types:      list[str],
    rate:       float,
    seed:       int,
    db_path:    str,
    dry_run:    bool,
    nrows:      int,
    verbose:    bool,
) -> None:

    run_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    _hr("═")
    print(f"  INJECTION → base de métriques SQLite")
    print(f"  Pipeline  : {pipeline}")
    print(f"  Types     : {', '.join(types)}")
    print(f"  Taux      : {rate:.0%}  |  Graine : {seed}  |  Lignes CSV : {nrows:,}")
    print(f"  Run date  : {run_date}")
    print(f"  DB        : {db_path}")
    if dry_run:
        print("  MODE      : DRY-RUN (aucune écriture)")
    _hr("═")

    # ── Chargement CSV ────────────────────────────────────────────────────────
    print(f"\n[1/4] Chargement de {RAW_SALES} ({nrows:,} lignes)…")
    if not os.path.exists(RAW_SALES):
        print(f"  Erreur : fichier introuvable → {RAW_SALES}")
        sys.exit(1)
    df_clean = pd.read_csv(RAW_SALES, nrows=nrows)
    print(f"      {len(df_clean):,} lignes chargées  ({len(df_clean.columns)} colonnes)")

    # ── Injection ─────────────────────────────────────────────────────────────
    print(f"\n[2/4] Injection : {', '.join(types)}  @  {rate:.0%}…")
    t0 = time.perf_counter()
    injector = AnomalyInjector(seed=seed, injection_rate=rate)
    df_dirty, report = injector.inject(df_clean.copy(), anomaly_types=types)
    elapsed = time.perf_counter() - t0

    print(f"      Durée injection  : {elapsed:.2f}s")
    print(f"      Lignes avant     : {len(df_clean):,}")
    print(f"      Lignes après     : {len(df_dirty):,}")
    injected_list = report.get("anomalies_injected", [])
    from collections import Counter
    counts = Counter(r["type"] for r in injected_list)
    for atype, cnt in sorted(counts.items()):
        n_rows = sum(len(r["row_indices"]) for r in injected_list if r["type"] == atype)
        print(f"        [{atype:25s}] {n_rows:>6} lignes affectées  ({cnt} événement(s))")

    # ── Calcul des MetricPoints ───────────────────────────────────────────────
    duration = 30.0 + len(types) * 4.0 + (len(df_dirty) / len(df_clean) - 1) * 20
    pts = df_to_metric_points(df_clean, df_dirty, pipeline, run_date, round(duration, 2), report)

    print(f"\n[3/4] Calcul des MetricPoints : {len(pts)} points")
    if verbose:
        for p in pts:
            print(f"        {p.name:<40} = {p.value}")

    # ── Écriture en base ──────────────────────────────────────────────────────
    if dry_run:
        print(f"\n[4/4] DRY-RUN — aucune écriture (utilisez --dry-run=false pour persister)")
    else:
        print(f"\n[4/4] Écriture dans {db_path}…")
        store   = SQLiteMetricsStore(db_path=db_path)
        n_before = store.count()
        n_written = store.write(pts)
        print(f"      {n_written} points écrits  (total DB : {store.count():,}  avant : {n_before:,})")

        # Détection immédiate sur les nouvelles métriques
        print(f"\n      Détection d'anomalies sur les nouvelles métriques…")
        try:
            detector = AnomalyDetector(store)
            alerts   = detector.run()
            mgr      = AlertManager(db_path=db_path)
            n_saved  = mgr.save(alerts)
            by_sev   = {}
            for a in alerts:
                by_sev[a.severity] = by_sev.get(a.severity, 0) + 1
            print(f"      {len(alerts)} alertes détectées  {by_sev}")
            print(f"      {n_saved} alertes sauvegardées dans alerts DB")
        except Exception as exc:
            print(f"      (détection ignorée : {exc})")

    _hr()
    injected_list  = report.get("anomalies_injected", [])
    total_injected = sum(len(r.get("row_indices", [])) for r in injected_list)
    print(f"  ✅  Injection terminée — {total_injected:,} lignes affectées dans {len(types)} type(s)")
    _hr()


# ══════════════════════════════════════════════════════════════════════════════
# CLI
# ══════════════════════════════════════════════════════════════════════════════

def _interactive() -> dict:
    """Mode interactif si aucun argument n'est passé."""
    print("\n  Mode interactif — inject_to_db.py")
    _hr()

    print(f"\n  Pipelines disponibles : {', '.join(PIPELINE_DESCRIPTIONS)}")
    pipeline = input("  Pipeline [ingest_sales] : ").strip() or "ingest_sales"

    print(f"\n  Types disponibles :")
    for i, t in enumerate(ALL_TYPES, 1):
        print(f"    {i:2d}. {t:<30} {ANOMALY_CATALOG[t]['description'][:55]}")
    raw = input("  Types (séparés par virgule, ex: nulls,duplicates) [nulls] : ").strip()
    types = [t.strip() for t in (raw or "nulls").split(",") if t.strip() in ALL_TYPES]
    if not types:
        types = ["nulls"]

    rate_str = input("  Taux d'injection [0.05] : ").strip() or "0.05"
    rate     = float(rate_str)
    seed     = int(input("  Graine [42] : ").strip() or "42")

    return {"pipeline": pipeline, "types": types, "rate": rate, "seed": seed}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Injecte des anomalies dans les données CSV et écrit les métriques en base SQLite."
    )
    parser.add_argument("--pipeline", default=None,
                        choices=list(PIPELINE_DESCRIPTIONS), help="Pipeline cible")
    parser.add_argument("--types",    default=None,
                        help="Types séparés par virgule (ex: nulls,duplicates)")
    parser.add_argument("--rate",     type=float, default=0.05,
                        help="Taux d'injection 0.01–0.30 (défaut: 0.05)")
    parser.add_argument("--seed",     type=int,   default=42,
                        help="Graine aléatoire (défaut: 42)")
    parser.add_argument("--db",       default=DEFAULT_DB,
                        help=f"Chemin SQLite (défaut: {DEFAULT_DB})")
    parser.add_argument("--nrows",    type=int, default=50_000,
                        help="Nombre de lignes CSV à charger (défaut: 50000)")
    parser.add_argument("--dry-run",  action="store_true",
                        help="Simule sans écrire en base")
    parser.add_argument("--verbose",  action="store_true",
                        help="Affiche chaque MetricPoint calculé")
    parser.add_argument("--list-types", action="store_true",
                        help="Affiche le catalogue des types d'anomalies disponibles")
    args = parser.parse_args()

    if args.list_types:
        _hr("═")
        print("  Catalogue des anomalies disponibles")
        _hr("═")
        for t, info in ANOMALY_CATALOG.items():
            print(f"\n  {t}")
            print(f"    {info['description']}")
            print(f"    → {info['use_case']}")
        _hr()
        return

    # Mode interactif si pipeline ou types manquants
    if args.pipeline is None or args.types is None:
        params = _interactive()
        pipeline = params["pipeline"]
        types    = params["types"]
        rate     = params.get("rate", args.rate)
        seed     = params.get("seed", args.seed)
    else:
        pipeline = args.pipeline
        types    = [t.strip() for t in args.types.split(",") if t.strip() in ALL_TYPES]
        rate     = args.rate
        seed     = args.seed

    if not types:
        print(f"Erreur : types inconnus. Valides : {', '.join(ALL_TYPES)}")
        sys.exit(1)

    run(
        pipeline=pipeline,
        types=types,
        rate=rate,
        seed=seed,
        db_path=args.db,
        dry_run=args.dry_run,
        nrows=args.nrows,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    main()
