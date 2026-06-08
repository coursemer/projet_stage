"""
ABTesting — Semaine 11 : comparaison A/B de deux configurations de règles.

Compare deux jeux de règles (générés avec des sigma_factor différents
ou des pipelines différents) et identifie le meilleur selon le F1 moyen.

Usage :
    from spark.metrics.validation import ABTesting, TestGenerator, LabeledSnapshot

    gen_a = TestGenerator(sigma_factor=2.0)
    gen_b = TestGenerator(sigma_factor=3.0)

    rules_a = gen_a.generate(history)
    rules_b = gen_b.generate(history)

    ab = ABTesting()
    result = ab.compare(
        rules_a, "sigma=2.0",
        rules_b, "sigma=3.0",
        labeled_snapshots,
    )
    print(result.summary())
"""
from __future__ import annotations

from typing import List

from .historical_validator import HistoricalValidator, LabeledSnapshot
from .models import ABTestResult, GeneratedRule


class ABTesting:
    """Compare deux jeux de règles sur le même dataset labelisé."""

    def __init__(self) -> None:
        self._validator = HistoricalValidator()

    def compare(
        self,
        rules_a: List[GeneratedRule],
        name_a: str,
        rules_b: List[GeneratedRule],
        name_b: str,
        labeled: List[LabeledSnapshot],
    ) -> ABTestResult:
        """
        Évalue les deux jeux de règles et retourne un ABTestResult comparatif.
        """
        results_a = self._validator.validate(rules_a, labeled)
        results_b = self._validator.validate(rules_b, labeled)

        f1_a, prec_a, rec_a = _aggregate(results_a)
        f1_b, prec_b, rec_b = _aggregate(results_b)

        return ABTestResult(
            name_a=name_a,
            name_b=name_b,
            f1_a=round(f1_a, 3),
            f1_b=round(f1_b, 3),
            precision_a=round(prec_a, 3),
            precision_b=round(prec_b, 3),
            recall_a=round(rec_a, 3),
            recall_b=round(rec_b, 3),
            n_rules_a=len(rules_a),
            n_rules_b=len(rules_b),
        )

    def compare_sigmas(
        self,
        history: list,
        labeled: List[LabeledSnapshot],
        sigma_a: float = 2.0,
        sigma_b: float = 3.0,
    ) -> ABTestResult:
        """Raccourci : compare deux sigma_factor sur le même historique."""
        from .test_generator import TestGenerator

        rules_a = TestGenerator(sigma_factor=sigma_a).generate(history)
        rules_b = TestGenerator(sigma_factor=sigma_b).generate(history)
        return self.compare(
            rules_a, f"sigma={sigma_a}",
            rules_b, f"sigma={sigma_b}",
            labeled,
        )


# ── Helpers ───────────────────────────────────────────────────────────────────

def _aggregate(results):
    """Retourne (mean_f1, mean_precision, mean_recall) depuis une liste de ValidationResult."""
    if not results:
        return 0.0, 0.0, 0.0
    n = len(results)
    return (
        sum(r.f1        for r in results) / n,
        sum(r.precision for r in results) / n,
        sum(r.recall    for r in results) / n,
    )
