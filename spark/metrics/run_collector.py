"""
CLI de collecte des métriques — collecte toutes les sources et insère dans SQLite.

Usage :
    python spark/metrics/run_collector.py                     # tout collecter
    python spark/metrics/run_collector.py --source spark
    python spark/metrics/run_collector.py --detect            # collecter + détecter anomalies (règles)
    python spark/metrics/run_collector.py --detect-ml         # détection ML multi-couches
    python spark/metrics/run_collector.py --detect-ml --explain  # ML + explication Mistral AI
    python spark/metrics/run_collector.py --detect --detect-ml --summary  # les deux
    python spark/metrics/run_collector.py --db /tmp/metrics.db
    python spark/metrics/run_collector.py --json              # sortie JSON machine-readable

LLM (Semaine 10) :
    export MISTRAL_API_KEY="..."   # active les appels Mistral AI (RGPD-conforme)
    # Sans clé : fallback Ollama local, puis templates
"""

from __future__ import annotations

import argparse
import json
import sys
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from spark.metrics.storage                   import get_store, DEFAULT_DB
from spark.metrics.collector                 import AirflowCollector, SparkCollector, DbtCollector
from spark.metrics.anomaly_detector          import AnomalyDetector, AnomalyAlert
from spark.metrics.alert_manager             import AlertManager
from spark.metrics.pipeline_adapter          import build_pipeline_snapshots
from spark.metrics.anomaly_detection         import AnomalyDetector as MLAnomalyDetector
from spark.metrics.anomaly_detection.models  import Severity as MLSeverity
from spark.metrics.llm_explainer             import LLMExplainer

# ML severity → alert severity mapping
_ML_SEV_MAP = {
    MLSeverity.CRITICAL: "critical",
    MLSeverity.HIGH:     "warning",
    MLSeverity.MEDIUM:   "warning",
    MLSeverity.LOW:      "info",
}


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


def run_detection_ml(db_path: str, verbose: bool = True, explain: bool = False) -> dict:
    """Run multi-layer ML anomaly detection on pipeline metrics snapshots."""
    store    = get_store(db_path=db_path)
    mgr      = AlertManager(db_path=db_path)
    snapshots = build_pipeline_snapshots(store)

    if not snapshots and verbose:
        print("  ⚠️  Aucun pipeline trouvé dans le store")
        return {"detected": 0, "saved": 0, "pipelines": 0}

    all_alerts: list[AnomalyAlert] = []
    for pipeline_name, (current, history) in snapshots.items():
        try:
            detector = MLAnomalyDetector.for_pipeline(pipeline_name=pipeline_name)
            result   = detector.detect(current, history)
        except Exception as exc:
            if verbose:
                print(f"  ⚠️  [{pipeline_name}] ML detection error: {exc}")
            continue

        anomalies_with_ctx = result.anomalies
        if explain and anomalies_with_ctx:
            explainer = LLMExplainer()
            anomalies_with_ctx = explainer.enrich_anomalies(anomalies_with_ctx)

        for anomaly in anomalies_with_ctx:
            sev_str = _ML_SEV_MAP.get(anomaly.severity, "info")
            llm_note = ""
            if explain and anomaly.context.get("llm_explanation"):
                backend  = anomaly.context.get("llm_backend", "?")
                llm_note = f" | LLM [{backend}]: {anomaly.context['llm_explanation'][:200]}"
            alert   = AnomalyAlert(
                metric_name=anomaly.metric_name,
                source=pipeline_name,
                algorithm=f"ml:{anomaly.level}",
                severity=sev_str,
                value=anomaly.observed_value or 0.0,
                expected=(
                    str(anomaly.expected_value)
                    if anomaly.expected_value is not None
                    else anomaly.description[:80]
                ),
                details=(
                    anomaly.description
                    + f" [score={anomaly.severity_score:.1f}]"
                    + llm_note
                ),
                tags={
                    "llm_explanation": anomaly.context.get("llm_explanation", ""),
                    "llm_backend":     anomaly.context.get("llm_backend", ""),
                },
            )
            all_alerts.append(alert)

        if explain and verbose and result.has_anomalies:
            for anomaly in result.anomalies:
                expl = anomaly.context.get("llm_explanation", "")
                bk   = anomaly.context.get("llm_backend", "?")
                if expl:
                    print(f"    💬 [{bk}] {expl[:180]}")
        if verbose and result.has_anomalies:
            worst = result.worst_severity or ""
            icon  = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡", "LOW": "🔵"}.get(worst, "⚪")
            print(f"  {icon} [{pipeline_name}] {len(result.anomalies)} anomalie(s) ML "
                  f"— pire sévérité: {worst}")

    n_saved = mgr.save(all_alerts)
    if verbose:
        if all_alerts:
            print(f"\n  ── Anomalies ML détectées ({len(all_alerts)}) ───────────────────")
        else:
            print(f"  ✅ Aucune anomalie ML ({len(snapshots)} pipelines analysés)")

    return {"detected": len(all_alerts), "saved": n_saved, "pipelines": len(snapshots)}


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
    parser.add_argument("--db",        default=DEFAULT_DB, help="Chemin vers metrics.db")
    parser.add_argument("--detect",    action="store_true", help="Détection règles (zscore/IQR/threshold)")
    parser.add_argument("--detect-ml", action="store_true", dest="detect_ml",
                        help="Détection ML multi-couches (Volume/Distribution/Schema/Performance/Temporal/ML)")
    parser.add_argument("--explain",   action="store_true",
                        help="Génère une explication LLM (Mistral AI) pour chaque anomalie ML (Semaine 10)")
    parser.add_argument("--summary",   action="store_true", help="Afficher le résumé après collecte")
    parser.add_argument("--json",      action="store_true", help="Sortie JSON (désactive les logs)")
    args = parser.parse_args()

    sources = list(COLLECTORS) if args.source == "all" else [args.source]
    verbose = not args.json

    if verbose:
        print(f"[run_collector] Base    : {args.db}")
        print(f"[run_collector] Sources : {', '.join(sources)}")

    collected = run_collection(sources, db_path=args.db, verbose=verbose)
    total     = sum(collected.values())

    detection    = {}
    detection_ml = {}

    if args.detect:
        if verbose:
            print(f"[run_collector] Détection anomalies (règles)...")
        detection = run_detection(db_path=args.db, verbose=verbose)

    if args.detect_ml:
        if verbose:
            llm_tag = " + LLM Mistral" if args.explain else ""
            print(f"[run_collector] Détection anomalies ML{llm_tag}...")
        detection_ml = run_detection_ml(db_path=args.db, verbose=verbose, explain=args.explain)

    if args.json:
        print(json.dumps({
            "collected":    collected,
            "total":        total,
            "detection":    detection,
            "detection_ml": detection_ml,
        }))
    else:
        print(f"[run_collector] Total   : {total} points insérés")
        if args.summary:
            print_summary(args.db)


if __name__ == "__main__":
    main()
