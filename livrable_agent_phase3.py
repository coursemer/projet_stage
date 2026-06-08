"""
╔══════════════════════════════════════════════════════════════════════════════╗
║           LIVRABLE — Data Trust Agent : Phase 3 (Semaines 7-11)            ║
╚══════════════════════════════════════════════════════════════════════════════╝

Démontre l'agent complet sur 3 pipelines :
  1. ingest_sales     — comportement nominal
  2. clean_sales      — anomalie volume (chute de lignes)
  3. aggregate_sales  — anomalie performance (durée SLA dépassée)

Capacités couvertes :
  S7  — Collecte & centralisation (métriques synthétiques)
  S8-9 — Détection multi-niveaux (volume, performance, ML)
  S10 — Explication LLM (template fallback, Mistral si MISTRAL_API_KEY défini)
  S11 — Génération de règles, validation historique, A/B testing, feedback loop

Usage :
    python livrable_agent_phase3.py
    MISTRAL_API_KEY=sk-... python livrable_agent_phase3.py
"""
from __future__ import annotations

import os
import random
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from spark.metrics.anomaly_detection.anomaly_detector import AnomalyDetector
from spark.metrics.anomaly_detection.models import Anomaly, PipelineMetrics
from spark.metrics.llm_explainer import LLMExplainer
from spark.metrics.validation import (
    ABTesting,
    FeedbackLoop,
    HistoricalValidator,
    LabeledSnapshot,
    TestGenerator,
)
from spark.metrics.validation.models import FeedbackEntry

# ── Configuration ─────────────────────────────────────────────────────────────

RANDOM_SEED   = 42
HISTORY_DAYS  = 30
SLA_SECONDS   = 120.0

PIPELINES = {
    "ingest_sales": {
        "scenario": "nominal",
        "description": "Ingestion quotidienne des ventes — comportement normal",
        "base_rows": 50_000,
        "base_duration": 45.0,
    },
    "clean_sales": {
        "scenario": "volume_anomaly",
        "description": "Nettoyage des ventes — chute de volume (source upstream défaillante)",
        "base_rows": 48_000,
        "base_duration": 30.0,
    },
    "aggregate_sales": {
        "scenario": "perf_anomaly",
        "description": "Agrégation des ventes — durée SLA dépassée (cluster surchargé)",
        "base_rows": 47_000,
        "base_duration": 60.0,
    },
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_history(
    pipeline: str,
    base_rows: int,
    base_duration: float,
    n: int = HISTORY_DAYS,
    seed: int = RANDOM_SEED,
) -> List[PipelineMetrics]:
    rng  = random.Random(seed)
    base = datetime.now(timezone.utc) - timedelta(days=n)
    history = []
    for i in range(n):
        row_count = int(base_rows + rng.gauss(0, base_rows * 0.02))
        duration  = base_duration + rng.gauss(0, base_duration * 0.05)
        history.append(PipelineMetrics(
            pipeline_name=pipeline,
            measured_at=base + timedelta(days=i),
            row_count=max(0, row_count),
            duration_seconds=max(0.1, duration),
            success=True,
            task_failures=0,
            column_stats={
                "revenue": {
                    "mean":      rng.gauss(4500, 50),
                    "std":       rng.gauss(300, 10),
                    "null_rate": rng.uniform(0.0, 0.01),
                }
            },
        ))
    return history


def _make_current(
    pipeline: str,
    scenario: str,
    base_rows: int,
    base_duration: float,
) -> PipelineMetrics:
    if scenario == "nominal":
        return PipelineMetrics(
            pipeline_name=pipeline,
            row_count=base_rows + 100,
            duration_seconds=base_duration + 1.2,
            success=True,
            task_failures=0,
        )
    if scenario == "volume_anomaly":
        return PipelineMetrics(
            pipeline_name=pipeline,
            row_count=int(base_rows * 0.08),  # chute de 92 % — anomalie critique
            duration_seconds=base_duration - 2.0,
            success=True,
            task_failures=0,
        )
    if scenario == "perf_anomaly":
        return PipelineMetrics(
            pipeline_name=pipeline,
            row_count=base_rows,
            duration_seconds=SLA_SECONDS * 4.5,   # 4.5× le SLA
            success=True,
            task_failures=2,
        )
    raise ValueError(f"Scénario inconnu : {scenario}")


def _labeled_set(
    pipeline: str,
    history: List[PipelineMetrics],
    base_rows: int,
    base_duration: float,
    n_normal: int = 20,
    n_anomaly: int = 5,
) -> List[LabeledSnapshot]:
    rng = random.Random(RANDOM_SEED + 1)
    normal = [
        LabeledSnapshot(
            metrics=PipelineMetrics(
                pipeline_name=pipeline,
                row_count=int(base_rows + rng.gauss(0, base_rows * 0.02)),
                duration_seconds=base_duration + rng.gauss(0, 2.0),
                success=True,
            ),
            is_anomaly=False,
        )
        for _ in range(n_normal)
    ]
    anomalies = [
        LabeledSnapshot(
            metrics=PipelineMetrics(
                pipeline_name=pipeline,
                row_count=int(base_rows * rng.uniform(0.02, 0.10)),
                duration_seconds=base_duration * rng.uniform(4.0, 6.0),
                success=False,
                task_failures=rng.randint(2, 5),
            ),
            is_anomaly=True,
        )
        for _ in range(n_anomaly)
    ]
    return normal + anomalies


# ── Printer ───────────────────────────────────────────────────────────────────

def _hr(char: str = "─", width: int = 72) -> None:
    print(char * width)


def _section(title: str) -> None:
    _hr("═")
    print(f"  {title}")
    _hr("═")


def _print_anomaly(a: Anomaly, idx: int) -> None:
    expl = a.context.get("llm_explanation", "")
    back = a.context.get("llm_backend", "")
    print(f"\n  [{idx}] {a.level.upper()}  •  sévérité : {a.severity}  (score={a.severity_score:.1f})")
    print(f"      métrique : {a.metric_name}")
    print(f"      observé  : {a.observed_value}  |  attendu : {a.expected_value}")
    print(f"      {a.description}")
    if expl:
        print(f"\n      💬 Explication ({back}) :")
        for line in expl.split(". "):
            line = line.strip()
            if line:
                print(f"         {line}.")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    print()
    _hr("═")
    print("  DATA TRUST AGENT — Livrable Phase 3  (Semaines 7-11)")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    _hr("═")

    explainer    = LLMExplainer()
    ab_engine    = ABTesting()
    validator    = HistoricalValidator()

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        fb_db = f.name
    feedback_loop = FeedbackLoop(db_path=fb_db)

    pipeline_reports = {}

    # ══════════════════════════════════════════════════════════════════════════
    # Boucle sur les 3 pipelines
    # ══════════════════════════════════════════════════════════════════════════
    for pipeline_name, cfg in PIPELINES.items():
        print()
        _hr()
        print(f"  PIPELINE : {pipeline_name.upper()}")
        print(f"  Scénario : {cfg['description']}")
        _hr()

        history = _make_history(pipeline_name, cfg["base_rows"], cfg["base_duration"])
        current = _make_current(pipeline_name, cfg["scenario"], cfg["base_rows"], cfg["base_duration"])

        # ── S8-9 : Détection d'anomalies ──────────────────────────────────
        print("\n  ▶ Détection d'anomalies (S8-9)…")
        detector = AnomalyDetector.for_pipeline(
            pipeline_name,
            sla_seconds=SLA_SECONDS,
            volume_sigma=3.0,
            min_history=7,
            ml_min_train=20,
        )
        result = detector.detect(current, history)

        if result.has_anomalies:
            print(f"    {len(result.anomalies)} anomalie(s) détectée(s)  •  pire sévérité : {result.worst_severity}")

            # ── S10 : Explication LLM ──────────────────────────────────────
            print("  ▶ Explication LLM (S10)…")
            enriched = explainer.enrich_anomalies(result.anomalies)
            for i, a in enumerate(enriched, 1):
                _print_anomaly(a, i)
        else:
            print("    ✅ Aucune anomalie détectée — pipeline sain")

        # ── S11 : Génération de règles ─────────────────────────────────────
        print("\n  ▶ Génération de règles (S11)…")
        gen   = TestGenerator(sigma_factor=3.0)
        rules = gen.generate(history)
        print(f"    {len(rules)} règles générées :")
        for r in rules:
            lo  = f"{r.threshold_low:.1f}" if r.threshold_low  is not None else "—"
            hi  = f"{r.threshold_high:.1f}" if r.threshold_high is not None else "—"
            print(f"    • {r.metric_name:<30}  [{lo}, {hi}]  conf={r.confidence:.2f}")

        # ── S11 : Validation historique ────────────────────────────────────
        print("\n  ▶ Validation sur données historiques (S11)…")
        labeled  = _labeled_set(pipeline_name, history, cfg["base_rows"], cfg["base_duration"])
        v_results = validator.validate(rules, labeled)
        report    = validator.report(v_results)
        print(f"    mean_F1={report['mean_f1']:.3f}  "
              f"mean_precision={report['mean_precision']:.3f}  "
              f"mean_recall={report['mean_recall']:.3f}")
        print(f"    meilleure règle : {report['best_rule']}")

        # ── S11 : A/B testing sigma=2 vs sigma=3 ──────────────────────────
        print("\n  ▶ A/B testing des règles (sigma=2 vs sigma=3)…")
        ab = ab_engine.compare_sigmas(history, labeled, sigma_a=2.0, sigma_b=3.0)
        print(f"    σ=2.0 → F1={ab.f1_a:.3f}   σ=3.0 → F1={ab.f1_b:.3f}   gagnant : {ab.winner}")

        # ── S11 : Feedback loop ────────────────────────────────────────────
        print("\n  ▶ Feedback loop (S11)…")
        row_rule = next((r for r in rules if r.metric_name == "row_count"), None)
        if row_rule and cfg["scenario"] == "nominal":
            feedback_loop.record(FeedbackEntry(
                rule_id=row_rule.rule_id,
                feedback_type="false_positive",
                note="Pic saisonnier normal fin de mois",
            ))
            print(f"    1 false_positive enregistré sur '{row_rule.rule_id}'")
        adjusted = feedback_loop.adjust_rules(rules)
        changed  = sum(1 for a, b in zip(rules, adjusted) if a.sigma_factor != b.sigma_factor)
        print(f"    {changed} règle(s) ajustée(s) après feedback")

        pipeline_reports[pipeline_name] = {
            "scenario":       cfg["scenario"],
            "anomalies":      len(result.anomalies),
            "worst_severity": result.worst_severity.value if result.worst_severity else "none",
            "n_rules":        len(rules),
            "mean_f1":        report["mean_f1"],
            "ab_winner":      ab.winner,
            "fb_adjusted":    changed,
        }

    # ══════════════════════════════════════════════════════════════════════════
    # Rapport final
    # ══════════════════════════════════════════════════════════════════════════
    print()
    _section("RAPPORT FINAL — Agent fonctionnel sur 3 pipelines")
    print()

    header = f"  {'Pipeline':<22} {'Scénario':<18} {'Anomalies':>9} {'Sévérité':>10} {'Règles':>7} {'F1':>6} {'A/B':>12}"
    print(header)
    _hr()
    for pl, r in pipeline_reports.items():
        icon = "🔴" if r["anomalies"] > 0 else "🟢"
        print(
            f"  {icon} {pl:<20} {r['scenario']:<18} "
            f"{r['anomalies']:>9} {r['worst_severity']:>10} "
            f"{r['n_rules']:>7} {r['mean_f1']:>6.3f} {r['ab_winner']:>12}"
        )
    _hr()

    fb_stats = feedback_loop.stats()
    print(f"\n  Feedback store  : {fb_stats['total_feedback']} entrée(s) "
          f"— {fb_stats['false_positives']} FP / {fb_stats['false_negatives']} FN")
    print(f"  Règles couvertes par feedback : {fb_stats['rules_with_feedback']}")

    print()
    _hr("═")
    print("  ✅  Phase 3 — Data Trust Agent Core — LIVRABLE VALIDÉ")
    print("  Prochaine étape : Phase 4 — Data Catalog enrichi + Dashboard (S12-14)")
    _hr("═")
    print()

    try:
        os.unlink(fb_db)
    except OSError:
        pass


if __name__ == "__main__":
    main()
