"""
Modèles de données — Semaine 11 : Génération & validation automatique.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, Literal, Optional


@dataclass
class GeneratedRule:
    """
    Règle de qualité générée automatiquement depuis l'historique des métriques.

    Condition évaluée : low <= valeur <= high
    Si low ou high est None, la borne correspondante n'est pas vérifiée.
    """
    rule_id: str
    pipeline_name: str
    metric_name: str
    sigma_factor: float = 3.0
    threshold_low: Optional[float] = None
    threshold_high: Optional[float] = None
    mean: Optional[float] = None
    std: Optional[float] = None
    n_samples: int = 0
    confidence: float = 0.0          # 0–1 basé sur n_samples
    generated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    # Poids de feedback cumulé (ajusté par FeedbackLoop)
    fp_count: int = 0
    fn_count: int = 0
    # Provenance : "statistical" (TestGenerator, mean ± k·σ) ou "llm" (Mistral,
    # déduit des logs Airflow/dbt + de l'historique des métriques)
    source: str = "statistical"
    reasoning: str = ""              # justification en langage naturel (rules LLM)

    def evaluate(self, value: Optional[float]) -> bool:
        """True = règle respectée (pas d'anomalie). False = anomalie détectée."""
        if value is None:
            return True
        if self.threshold_low is not None and value < self.threshold_low:
            return False
        if self.threshold_high is not None and value > self.threshold_high:
            return False
        return True

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "pipeline_name": self.pipeline_name,
            "metric_name": self.metric_name,
            "sigma_factor": self.sigma_factor,
            "threshold_low": self.threshold_low,
            "threshold_high": self.threshold_high,
            "mean": self.mean,
            "std": self.std,
            "n_samples": self.n_samples,
            "confidence": round(self.confidence, 3),
            "fp_count": self.fp_count,
            "fn_count": self.fn_count,
            "generated_at": self.generated_at.isoformat(),
            "source": self.source,
            "reasoning": self.reasoning,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "GeneratedRule":
        d = dict(d)
        if "generated_at" in d and isinstance(d["generated_at"], str):
            d["generated_at"] = datetime.fromisoformat(d["generated_at"])
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


@dataclass
class ValidationResult:
    """Résultat de validation d'un jeu de règles sur données historiques labelisées."""
    rule_id: str
    pipeline_name: str
    metric_name: str
    tp: int = 0    # vrais positifs  (anomalie détectée correctement)
    fp: int = 0    # faux positifs   (normal signalé à tort)
    fn: int = 0    # faux négatifs   (anomalie manquée)
    tn: int = 0    # vrais négatifs  (normal passé correctement)

    @property
    def precision(self) -> float:
        return self.tp / (self.tp + self.fp) if (self.tp + self.fp) > 0 else 0.0

    @property
    def recall(self) -> float:
        return self.tp / (self.tp + self.fn) if (self.tp + self.fn) > 0 else 0.0

    @property
    def f1(self) -> float:
        p, r = self.precision, self.recall
        return 2 * p * r / (p + r) if (p + r) > 0 else 0.0

    @property
    def accuracy(self) -> float:
        total = self.tp + self.fp + self.fn + self.tn
        return (self.tp + self.tn) / total if total > 0 else 0.0

    def summary(self) -> Dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "pipeline": self.pipeline_name,
            "metric": self.metric_name,
            "tp": self.tp, "fp": self.fp, "fn": self.fn, "tn": self.tn,
            "precision": round(self.precision, 3),
            "recall": round(self.recall, 3),
            "f1": round(self.f1, 3),
            "accuracy": round(self.accuracy, 3),
        }


@dataclass
class ABTestResult:
    """Résultat d'une comparaison A/B entre deux jeux de règles."""
    name_a: str
    name_b: str
    f1_a: float
    f1_b: float
    precision_a: float
    precision_b: float
    recall_a: float
    recall_b: float
    n_rules_a: int
    n_rules_b: int
    run_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def winner(self) -> str:
        if self.f1_a > self.f1_b:
            return self.name_a
        elif self.f1_b > self.f1_a:
            return self.name_b
        return "tie"

    @property
    def improvement_pct(self) -> float:
        if self.f1_a == 0:
            return 0.0
        return round((self.f1_b - self.f1_a) / self.f1_a * 100, 1)

    def summary(self) -> Dict[str, Any]:
        return {
            "name_a": self.name_a,
            "name_b": self.name_b,
            "f1_a": round(self.f1_a, 3),
            "f1_b": round(self.f1_b, 3),
            "precision_a": round(self.precision_a, 3),
            "precision_b": round(self.precision_b, 3),
            "recall_a": round(self.recall_a, 3),
            "recall_b": round(self.recall_b, 3),
            "winner": self.winner,
            "improvement_pct": self.improvement_pct,
        }


@dataclass
class FeedbackEntry:
    """Feedback opérateur sur une anomalie signalée par une règle."""
    rule_id: str
    feedback_type: Literal["false_positive", "false_negative"]
    anomaly_id: Optional[str] = None
    metric_value: Optional[float] = None
    note: str = ""
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
