"""
CLI de collecte des métriques — collecte toutes les sources et insère dans SQLite.

Usage :
    # Tout collecter
    python spark/metrics/run_collector.py

    # Source spécifique
    python spark/metrics/run_collector.py --source spark
    python spark/metrics/run_collector.py --source airflow
    python spark/metrics/run_collector.py --source dbt

    # Avec base personnalisée
    python spark/metrics/run_collector.py --db /tmp/metrics.db

    # Afficher le résumé après collecte
    python spark/metrics/run_collector.py --summary
"""

from __future__ import annotations

import argparse
import json
import sys
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from spark.metrics.storage   import get_store, DEFAULT_DB
from spark.metrics.collector import AirflowCollector, SparkCollector, DbtCollector


COLLECTORS = {
    "airflow": AirflowCollector,
    "spark":   SparkCollector,
    "dbt":     DbtCollector,
}


def run_collection(sources: list[str], db_path: str, verbose: bool = True) -> dict:
    store   = get_store(db_path=db_path)
    results = {}

    for src in sources:
        cls = COLLECTORS.get(src)
        if cls is None:
            if verbose:
                print(f"  [SKIP] Source inconnue : {src}")
            continue

        collector = cls()
        points    = collector.collect()
        n         = store.write(points)
        results[src] = n

        if verbose:
            print(f"  [{src:<8}] {n:>4} points collectés → {db_path}")

    return results


def print_summary(db_path: str) -> None:
    store = get_store(db_path=db_path)
    rows  = store.summary()
    total = store.count()

    print(f"\n{'─'*65}")
    print(f"  {'SOURCE':<10} {'MÉTRIQUE':<35} {'N':>5}  DERNIÈRE VALEUR")
    print(f"{'─'*65}")
    for r in rows:
        print(
            f"  {r['source']:<10} {r['name']:<35} {int(r['count']):>5}"
            f"  {r['last_value']:.2f}  (ts: {r['last_ts'][:19]})"
        )
    print(f"{'─'*65}")
    print(f"  Total : {total} points  |  {len(rows)} métriques distinctes")


def main() -> None:
    parser = argparse.ArgumentParser(description="Collecte les métriques pipeline")
    parser.add_argument(
        "--source", choices=list(COLLECTORS) + ["all"], default="all",
        help="Source à collecter (défaut: all)",
    )
    parser.add_argument("--db",      default=DEFAULT_DB, help="Chemin vers metrics.db")
    parser.add_argument("--summary", action="store_true", help="Afficher le résumé après collecte")
    parser.add_argument("--json",    action="store_true", help="Sortie JSON (désactive les logs)")
    args = parser.parse_args()

    sources = list(COLLECTORS) if args.source == "all" else [args.source]
    verbose = not args.json

    if verbose:
        print(f"[run_collector] Base : {args.db}")
        print(f"[run_collector] Sources : {', '.join(sources)}")

    results = run_collection(sources, db_path=args.db, verbose=verbose)

    total = sum(results.values())
    if args.json:
        print(json.dumps({"collected": results, "total": total}))
    else:
        print(f"[run_collector] Total : {total} points insérés")
        if args.summary:
            print_summary(args.db)


if __name__ == "__main__":
    main()
