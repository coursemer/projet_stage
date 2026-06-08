"""
Tests — Semaine 11 : Génération automatique & validation

T1 : test_generator_basic         — génération depuis historique simple
T2 : test_generator_min_samples   — règles avec trop peu de données = faible confiance
T3 : test_generator_save_load     — persistance JSON aller-retour
T4 : validator_perfect_rule       — règle parfaite → F1=1.0
T5 : validator_noisy_rule         — règle bruitée → F1 intermédiaire
T6 : validator_no_anomalies       — dataset 100% clean → FP seulement
T7 : ab_testing_sigma             — sigma=2 vs sigma=3 sur même dataset
T8 : feedback_record_query        — enregistrement + requête SQLite
T9 : feedback_adjust_fp           — FP feedback → seuils s'élargissent
T10: feedback_adjust_fn           — FN feedback → seuils se resserrent
T11: end_to_end_3_pipelines       — agent complet sur 3 pipelines
"""
from __future__ import annotations

import json
import os
import sys
import tempfile

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from spark.metrics.anomaly_detection.models import PipelineMetrics
from spark.metrics.validation import (
    ABTesting,
    FeedbackLoop,
    GeneratedRule,
    HistoricalValidator,
    LabeledSnapshot,
    TestGenerator,
)
from spark.metrics.validation.models import FeedbackEntry

results: dict[str, str] = {}


# ── Fixtures ──────────────────────────────────────────────────────────────────

def _clean_snapshot(pipeline: str, row_count: int = 1000, duration: float = 10.0) -> PipelineMetrics:
    return PipelineMetrics(
        pipeline_name=pipeline,
        row_count=row_count,
        duration_seconds=duration,
        success=True,
        task_failures=0,
    )


def _anomaly_snapshot(pipeline: str, row_count: int = 0, duration: float = 500.0) -> PipelineMetrics:
    return PipelineMetrics(
        pipeline_name=pipeline,
        row_count=row_count,
        duration_seconds=duration,
        success=False,
        task_failures=5,
    )


def _history(pipeline: str, n: int = 20) -> list:
    return [_clean_snapshot(pipeline, row_count=1000 + i * 5, duration=10.0 + i * 0.1) for i in range(n)]


# ══════════════════════════════════════════════════════════════════════════════
# T1 — TestGenerator : génération basique
# ══════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("T1 — test_generator_basic")
print("=" * 60)
try:
    history = _history("ingest_sales", 20)
    gen     = TestGenerator(sigma_factor=3.0, min_samples=5)
    rules   = gen.generate(history)

    assert len(rules) >= 3, f"Au moins 3 règles attendues (row_count, duration, success), obtenu {len(rules)}"

    names = {r.metric_name for r in rules}
    assert "row_count"        in names
    assert "duration_seconds" in names
    assert "success"          in names

    # Les seuils doivent être numériques
    for r in rules:
        if r.threshold_high is not None:
            assert r.threshold_high > 0, f"{r.metric_name}: threshold_high <= 0"

    print(f"  {len(rules)} règles générées  ✓")
    for r in rules:
        print(f"    [{r.metric_name}] low={r.threshold_low} high={r.threshold_high} conf={r.confidence}")
    results["T1-test_generator_basic"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T1-test_generator_basic"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T2 — TestGenerator : faible confiance avec peu de données
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T2 — test_generator_min_samples")
print("=" * 60)
try:
    history_short = _history("clean_sales", n=3)
    gen   = TestGenerator(sigma_factor=3.0, min_samples=10)
    rules = gen.generate(history_short)

    for r in rules:
        assert r.confidence <= 0.5, f"Confiance trop haute avec 3 échantillons : {r.confidence}"

    print(f"  {len(rules)} règles, toutes confidence ≤ 0.5  ✓")
    results["T2-test_generator_min_samples"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T2-test_generator_min_samples"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T3 — TestGenerator : save / load JSON
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T3 — test_generator_save_load")
print("=" * 60)
try:
    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "rules.json")
        history = _history("aggregate_sales", 15)
        gen     = TestGenerator()
        rules   = gen.generate(history)
        gen.save_rules(rules, path)

        loaded = TestGenerator.load_rules(path)
        assert len(loaded) == len(rules), f"{len(rules)} règles sauvegardées, {len(loaded)} rechargées"

        orig_ids   = {r.rule_id for r in rules}
        loaded_ids = {r.rule_id for r in loaded}
        assert orig_ids == loaded_ids, "rule_id non concordants"

    print(f"  {len(rules)} règles sauvegardées et rechargées  ✓")
    results["T3-test_generator_save_load"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T3-test_generator_save_load"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T4 — HistoricalValidator : règle parfaite → F1=1.0
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T4 — validator_perfect_rule")
print("=" * 60)
try:
    # Règle : row_count doit être > 500
    rule = GeneratedRule(
        rule_id="perf_rule",
        pipeline_name="ingest_sales",
        metric_name="row_count",
        threshold_low=500.0,
        threshold_high=None,
    )
    labeled = (
        [LabeledSnapshot(metrics=_clean_snapshot("ingest_sales", row_count=1000), is_anomaly=False)
         for _ in range(10)]
        +
        [LabeledSnapshot(metrics=_anomaly_snapshot("ingest_sales", row_count=0), is_anomaly=True)
         for _ in range(5)]
    )

    validator = HistoricalValidator()
    [result]  = validator.validate([rule], labeled)

    assert result.tp == 5,  f"tp={result.tp} attendu 5"
    assert result.fp == 0,  f"fp={result.fp} attendu 0"
    assert result.fn == 0,  f"fn={result.fn} attendu 0"
    assert result.tn == 10, f"tn={result.tn} attendu 10"
    assert result.f1 == 1.0, f"F1={result.f1} attendu 1.0"

    print(f"  TP={result.tp} FP={result.fp} FN={result.fn} TN={result.tn}  ✓")
    print(f"  F1={result.f1}  ✓")
    results["T4-validator_perfect_rule"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T4-validator_perfect_rule"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T5 — HistoricalValidator : règle bruitée → F1 entre 0 et 1
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T5 — validator_noisy_rule")
print("=" * 60)
try:
    # Règle trop stricte : row_count doit être entre 950 et 1050 (va générer des FP)
    rule = GeneratedRule(
        rule_id="noisy_rule",
        pipeline_name="ingest_sales",
        metric_name="row_count",
        threshold_low=950.0,
        threshold_high=1050.0,
    )
    labeled = (
        [LabeledSnapshot(metrics=_clean_snapshot("ingest_sales", row_count=800), is_anomaly=False)
         for _ in range(5)]    # 5 FP attendus (800 < 950 mais ce n'est pas une vraie anomalie)
        +
        [LabeledSnapshot(metrics=_clean_snapshot("ingest_sales", row_count=1000), is_anomaly=False)
         for _ in range(5)]
        +
        [LabeledSnapshot(metrics=_anomaly_snapshot("ingest_sales", row_count=0), is_anomaly=True)
         for _ in range(5)]
    )

    [result] = HistoricalValidator().validate([rule], labeled)
    assert 0.0 < result.f1 < 1.0, f"F1={result.f1} attendu entre 0 et 1"
    assert result.fp >= 1, f"fp={result.fp} attendu ≥ 1"

    print(f"  TP={result.tp} FP={result.fp} FN={result.fn} TN={result.tn}  ✓")
    print(f"  F1={result.f1:.3f}  ✓")
    results["T5-validator_noisy_rule"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T5-validator_noisy_rule"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T6 — HistoricalValidator : dataset 100% clean → que des TN ou FP
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T6 — validator_no_anomalies")
print("=" * 60)
try:
    rule = GeneratedRule(
        rule_id="clean_rule",
        pipeline_name="ingest_sales",
        metric_name="row_count",
        threshold_low=0.0,
        threshold_high=2000.0,
    )
    labeled = [
        LabeledSnapshot(metrics=_clean_snapshot("ingest_sales", row_count=1000), is_anomaly=False)
        for _ in range(10)
    ]
    [result] = HistoricalValidator().validate([rule], labeled)
    assert result.tp == 0, f"tp={result.tp} attendu 0"
    assert result.fn == 0, f"fn={result.fn} attendu 0"
    assert result.tn == 10

    print(f"  TP={result.tp} FP={result.fp} FN={result.fn} TN={result.tn}  ✓")
    results["T6-validator_no_anomalies"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T6-validator_no_anomalies"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T7 — ABTesting : sigma=2 vs sigma=3
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T7 — ab_testing_sigma")
print("=" * 60)
try:
    history = _history("ingest_sales", 20)
    labeled = (
        [LabeledSnapshot(metrics=_clean_snapshot("ingest_sales", row_count=1000), is_anomaly=False)
         for _ in range(20)]
        +
        [LabeledSnapshot(metrics=_anomaly_snapshot("ingest_sales", row_count=0, duration=500.0), is_anomaly=True)
         for _ in range(5)]
    )

    ab     = ABTesting()
    result = ab.compare_sigmas(history, labeled, sigma_a=2.0, sigma_b=3.0)

    assert result.winner in ("sigma=2.0", "sigma=3.0", "tie"), f"Winner inattendu: {result.winner}"
    assert 0.0 <= result.f1_a <= 1.0
    assert 0.0 <= result.f1_b <= 1.0

    print(f"  sigma=2.0 : F1={result.f1_a}  prec={result.precision_a}  rec={result.recall_a}")
    print(f"  sigma=3.0 : F1={result.f1_b}  prec={result.precision_b}  rec={result.recall_b}")
    print(f"  Gagnant : {result.winner}  (Δ={result.improvement_pct}%)  ✓")
    results["T7-ab_testing_sigma"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T7-ab_testing_sigma"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T8 — FeedbackLoop : enregistrement + requête
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T8 — feedback_record_query")
print("=" * 60)
try:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    loop = FeedbackLoop(db_path=db_path)
    loop.record(FeedbackEntry(rule_id="rule_A", feedback_type="false_positive", note="ok normal"))
    loop.record(FeedbackEntry(rule_id="rule_A", feedback_type="false_positive"))
    loop.record(FeedbackEntry(rule_id="rule_B", feedback_type="false_negative", metric_value=0.0))

    stats = loop.stats()
    assert stats["total_feedback"] == 3
    assert stats["false_positives"] == 2
    assert stats["false_negatives"] == 1
    assert stats["rules_with_feedback"] == 2

    rows = loop.query(rule_id="rule_A")
    assert len(rows) == 2
    rows_fp = loop.query(feedback_type="false_positive")
    assert len(rows_fp) == 2

    print(f"  3 feedbacks enregistrés  ✓")
    print(f"  query(rule_A)={len(rows)}  query(fp)={len(rows_fp)}  ✓")
    print(f"  stats={stats}  ✓")
    results["T8-feedback_record_query"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T8-feedback_record_query"] = "FAILED"
finally:
    try: os.unlink(db_path)
    except Exception: pass


# ══════════════════════════════════════════════════════════════════════════════
# T9 — FeedbackLoop : FP feedback → seuils s'élargissent
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T9 — feedback_adjust_fp")
print("=" * 60)
try:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    loop = FeedbackLoop(db_path=db_path, sigma_step=0.25)
    rule = GeneratedRule(
        rule_id="rule_dur",
        pipeline_name="ingest_sales",
        metric_name="duration_seconds",
        sigma_factor=3.0,
        threshold_low=0.0,
        threshold_high=25.0,   # cohérent : mean + sigma*std = 10 + 3*5 = 25
        mean=10.0,
        std=5.0,
    )

    # 4 false positives → sigma doit augmenter de 4 * 0.25 = 1.0
    for _ in range(4):
        loop.record(FeedbackEntry(rule_id="rule_dur", feedback_type="false_positive"))

    [adjusted] = loop.adjust_rules([rule])
    assert adjusted.sigma_factor > rule.sigma_factor, (
        f"sigma_factor aurait dû augmenter : {rule.sigma_factor} → {adjusted.sigma_factor}"
    )
    assert adjusted.threshold_high > rule.threshold_high, (
        f"threshold_high aurait dû augmenter : {rule.threshold_high} → {adjusted.threshold_high}"
    )

    print(f"  sigma {rule.sigma_factor} → {adjusted.sigma_factor}  ✓")
    print(f"  threshold_high {rule.threshold_high} → {adjusted.threshold_high}  ✓")
    results["T9-feedback_adjust_fp"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T9-feedback_adjust_fp"] = "FAILED"
finally:
    try: os.unlink(db_path)
    except Exception: pass


# ══════════════════════════════════════════════════════════════════════════════
# T10 — FeedbackLoop : FN feedback → seuils se resserrent
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T10 — feedback_adjust_fn")
print("=" * 60)
try:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    loop = FeedbackLoop(db_path=db_path, sigma_step=0.25)
    rule = GeneratedRule(
        rule_id="rule_rows",
        pipeline_name="clean_sales",
        metric_name="row_count",
        sigma_factor=3.0,
        threshold_low=0.0,
        threshold_high=2000.0,
        mean=1000.0,
        std=50.0,
    )

    # 4 false negatives → sigma doit diminuer
    for _ in range(4):
        loop.record(FeedbackEntry(rule_id="rule_rows", feedback_type="false_negative"))

    [adjusted] = loop.adjust_rules([rule])
    assert adjusted.sigma_factor < rule.sigma_factor, (
        f"sigma_factor aurait dû diminuer : {rule.sigma_factor} → {adjusted.sigma_factor}"
    )
    assert adjusted.threshold_high < rule.threshold_high, (
        f"threshold_high aurait dû diminuer : {rule.threshold_high} → {adjusted.threshold_high}"
    )

    print(f"  sigma {rule.sigma_factor} → {adjusted.sigma_factor}  ✓")
    print(f"  threshold_high {rule.threshold_high} → {adjusted.threshold_high}  ✓")
    results["T10-feedback_adjust_fn"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T10-feedback_adjust_fn"] = "FAILED"
finally:
    try: os.unlink(db_path)
    except Exception: pass


# ══════════════════════════════════════════════════════════════════════════════
# T11 — End-to-end : agent sur 3 pipelines
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T11 — end_to_end_3_pipelines")
print("=" * 60)
try:
    PIPELINES = ["ingest_sales", "clean_sales", "aggregate_sales"]

    all_rules       = []
    all_ab_results  = []
    all_feedback_ok = True

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        fb_db = f.name

    feedback_loop = FeedbackLoop(db_path=fb_db)
    ab_engine     = ABTesting()
    validator     = HistoricalValidator()

    for pipeline in PIPELINES:
        # 1. Historique simulé
        history = _history(pipeline, n=30)

        # 2. Générer les règles
        rules = TestGenerator(sigma_factor=3.0).generate(history)
        assert len(rules) >= 2, f"{pipeline}: moins de 2 règles générées"

        # 3. Dataset labelisé : 20 normaux + 5 anomalies
        labeled = (
            [LabeledSnapshot(metrics=_clean_snapshot(pipeline), is_anomaly=False) for _ in range(20)]
            + [LabeledSnapshot(metrics=_anomaly_snapshot(pipeline), is_anomaly=True)  for _ in range(5)]
        )

        # 4. Validation
        v_results = validator.validate(rules, labeled)
        report    = validator.report(v_results)
        assert report["rules"] > 0

        # 5. A/B test sigma=2 vs sigma=3
        ab = ab_engine.compare_sigmas(history, labeled, sigma_a=2.0, sigma_b=3.0)
        all_ab_results.append(ab)

        # 6. Feedback : 2 FP simulés sur la règle row_count
        row_rule = next((r for r in rules if r.metric_name == "row_count"), None)
        if row_rule:
            feedback_loop.record(FeedbackEntry(rule_id=row_rule.rule_id, feedback_type="false_positive"))
            feedback_loop.record(FeedbackEntry(rule_id=row_rule.rule_id, feedback_type="false_positive"))

        # 7. Ajustement des règles
        adjusted = feedback_loop.adjust_rules(rules)
        assert len(adjusted) == len(rules)

        all_rules.extend(rules)
        print(f"  [{pipeline}] rules={len(rules)} mean_f1={report['mean_f1']:.3f} ab_winner={ab.winner}")

    assert len(all_rules) >= 6, f"Au moins 6 règles au total, obtenu {len(all_rules)}"
    assert feedback_loop.stats()["total_feedback"] >= 2

    print(f"\n  Total règles   : {len(all_rules)}")
    print(f"  Feedback store : {feedback_loop.stats()}")
    print(f"  ✅ Agent fonctionnel sur {len(PIPELINES)} pipelines")
    results["T11-end_to_end_3_pipelines"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T11-end_to_end_3_pipelines"] = "FAILED"
finally:
    try: os.unlink(fb_db)
    except Exception: pass


# ══════════════════════════════════════════════════════════════════════════════
# RÉSUMÉ
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("RÉSUMÉ — Semaine 11")
print("=" * 60)
all_ok = True
for label, status in results.items():
    icon = "✅" if status == "OK" else "❌"
    print(f"  {icon}  {label:<40} {status}")
    if status != "OK":
        all_ok = False
print("=" * 60)
print("RÉSULTAT GLOBAL :", "✅ TOUS OK" if all_ok else "❌ ÉCHECS DÉTECTÉS")
