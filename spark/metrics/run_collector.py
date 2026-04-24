"""
CLI de collecte des métriques — collecte toutes les sources et insère dans SQLite.

Usage :
    python spark/metrics/run_collector.py                     # tout collecter
    python spark/metrics/run_collector.py --source spark
    python spark/metrics/run_collector.py --detect            # collecter + détecter anomalies
    python spark/metrics/run_collector.py --detect --summary  # avec résumé complet
    python spark/metrics/run_collector.py --db /tmp/metrics.db
    python spark/metrics/run_collector.py --json              # sortie JSON machine-readable
"""

from __future__ import annotations

import argparse
import json
import sys
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from spark.metrics.storage          import get_store, DEFAULT_DB
from spark.metrics.collector        import AirflowCollector, SparkCollector, DbtCollector
from spark.metrics.anomaly_detector import AnomalyDetector
from spark.metrics.alert_manager    import AlertManager


COLLECTORS = {
    "airflow": AirflowCollector,
    "spark":   SparkCollector,
    "dbt":     DbtCollector,
}

SEVERITY_ICON = {"critical": "🔴", "warning": "🟡", "info": "🔵"}


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


def run_detection(db_path: str, verbose: bool = True) -> dict:
    store   = get_store(db_path=db_path)
    mgr     = AlertManager(db_path=db_path)
    detector = AnomalyDetector(store)
    alerts  = detector.run()
    n_saved = mgr.save(alerts)

    if verbose and alerts:
        print(f"\n  ── Anomalies détectées ({len(alerts)}) ──────────────────────────")
        for a in sorted(alerts, key=lambda x: ("critical", "warning", "info").index(x.severity)):
            icon = SEVERITY_ICON.get(a.severity, "⚪")
            print(f"  {icon} [{a.severity.upper():<8}] {a.source}/{a.metric_name}")
            print(f"           {a.algorithm}: {a.details}")
    elif verbose:
        print("  ✅ Aucune anomalie détectée")

    return {"detected": len(alerts), "saved": n_saved}


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

    mgr = AlertManager(db_path=db_path)
    s   = mgr.summary()
    if s["unacknowledged"] > 0:
        print(f"\n  Alertes non traitées : {s['unacknowledged']} / {s['total']}")
        for row in s["by_severity"]:
            icon = SEVERITY_ICON.get(row["severity"], "⚪")
            print(f"    {icon} {row['severity']:<10} {row['count']}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Collecte les métriques pipeline")
    parser.add_argument(
        "--source", choices=list(COLLECTORS) + ["all"], default="all",
        help="Source à collecter (défaut: all)",
    )
    parser.add_argument("--db",      default=DEFAULT_DB, help="Chemin vers metrics.db")
    parser.add_argument("--detect",  action="store_true", help="Détecter les anomalies après collecte")
    parser.add_argument("--summary", action="store_true", help="Afficher le résumé après collecte")
    parser.add_argument("--json",    action="store_true", help="Sortie JSON (désactive les logs)")
    args = parser.parse_args()

    sources = list(COLLECTORS) if args.source == "all" else [args.source]
    verbose = not args.json

    if verbose:
        print(f"[run_collector] Base    : {args.db}")
        print(f"[run_collector] Sources : {', '.join(sources)}")

    collected = run_collection(sources, db_path=args.db, verbose=verbose)
    total     = sum(collected.values())

    detection = {}
    if args.detect:
        if verbose:
            print(f"[run_collector] Détection d'anomalies...")
        detection = run_detection(db_path=args.db, verbose=verbose)

    if args.json:
        print(json.dumps({"collected": collected, "total": total, "detection": detection}))
    else:
        print(f"[run_collector] Total   : {total} points insérés")
        if args.summary:
            print_summary(args.db)


if __name__ == "__main__":
    main()
