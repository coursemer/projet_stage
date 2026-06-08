"""
FeedbackLoop — Semaine 11 : feedback opérateur + ajustement automatique des seuils.

Les opérateurs peuvent signaler :
  - "false_positive" : la règle a détecté une anomalie qui n'en est pas une
                       → seuils trop serrés → on élargit (sigma_factor + Δ)
  - "false_negative"  : la règle a manqué une vraie anomalie
                        → seuils trop lâches → on resserre (sigma_factor - Δ)

Le FeedbackLoop persiste le feedback dans SQLite et propose des règles ajustées.

Usage :
    from spark.metrics.validation import FeedbackLoop, FeedbackEntry

    loop = FeedbackLoop(db_path="feedback.db")
    loop.record(FeedbackEntry(rule_id="...", feedback_type="false_positive"))

    adjusted = loop.adjust_rules(rules)
    stats    = loop.stats()
"""
from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .models import FeedbackEntry, GeneratedRule


_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS feedback (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id       TEXT    NOT NULL,
    feedback_type TEXT    NOT NULL,  -- 'false_positive' | 'false_negative'
    anomaly_id    TEXT,
    metric_value  REAL,
    note          TEXT    DEFAULT '',
    created_at    TEXT    NOT NULL
)
"""

_SIGMA_STEP = 0.25   # delta σ par feedback
_SIGMA_MIN  = 1.0
_SIGMA_MAX  = 5.0


class FeedbackLoop:
    """
    Stocke le feedback opérateur (SQLite) et ajuste les seuils des règles.

    Parameters
    ----------
    db_path    : chemin vers la base SQLite de feedback
    sigma_step : amplitude du pas d'ajustement σ par feedback (défaut : 0.25)
    """

    def __init__(self, db_path: str = "feedback.db", sigma_step: float = _SIGMA_STEP) -> None:
        self.db_path    = db_path
        self.sigma_step = sigma_step
        self._init_db()

    # ── Public API ────────────────────────────────────────────────────────────

    def record(self, entry: FeedbackEntry) -> int:
        """Enregistre un feedback. Retourne l'id inséré."""
        with self._connect() as conn:
            cur = conn.execute(
                "INSERT INTO feedback (rule_id, feedback_type, anomaly_id, metric_value, note, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    entry.rule_id,
                    entry.feedback_type,
                    entry.anomaly_id,
                    entry.metric_value,
                    entry.note,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            conn.commit()
            return cur.lastrowid

    def record_many(self, entries: List[FeedbackEntry]) -> int:
        """Enregistre plusieurs feedbacks. Retourne le nombre inséré."""
        for e in entries:
            self.record(e)
        return len(entries)

    def adjust_rules(self, rules: List[GeneratedRule]) -> List[GeneratedRule]:
        """
        Retourne une copie des règles avec les seuils ajustés selon le feedback cumulé.

        Logique :
          - fp_net = n_false_positive - n_false_negative
          - si fp_net > 0 → trop de FP → on élargit (sigma_factor += fp_net * step)
          - si fp_net < 0 → trop de FN → on resserre (sigma_factor += fp_net * step)
        """
        counts = self._counts_per_rule()
        adjusted = []
        for rule in rules:
            fp_n = counts.get(rule.rule_id, {}).get("false_positive", 0)
            fn_n = counts.get(rule.rule_id, {}).get("false_negative", 0)
            fp_n_net = fp_n - fn_n

            new_sigma = rule.sigma_factor + fp_n_net * self.sigma_step
            new_sigma = max(_SIGMA_MIN, min(_SIGMA_MAX, new_sigma))

            if new_sigma == rule.sigma_factor:
                adjusted.append(rule)
                continue

            # Recalcule les seuils proportionnellement
            new_rule = _rescale_rule(rule, new_sigma)
            new_rule.fp_count = fp_n
            new_rule.fn_count = fn_n
            adjusted.append(new_rule)

        return adjusted

    def stats(self) -> Dict[str, Any]:
        """Statistiques globales du feedback store."""
        with self._connect() as conn:
            total = conn.execute("SELECT COUNT(*) FROM feedback").fetchone()[0]
            fp    = conn.execute(
                "SELECT COUNT(*) FROM feedback WHERE feedback_type='false_positive'"
            ).fetchone()[0]
            fn    = conn.execute(
                "SELECT COUNT(*) FROM feedback WHERE feedback_type='false_negative'"
            ).fetchone()[0]
            rules = conn.execute(
                "SELECT COUNT(DISTINCT rule_id) FROM feedback"
            ).fetchone()[0]
        return {
            "total_feedback": total,
            "false_positives": fp,
            "false_negatives": fn,
            "rules_with_feedback": rules,
        }

    def query(
        self,
        rule_id: Optional[str] = None,
        feedback_type: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """Interroge le feedback store avec filtres optionnels."""
        where, params = [], []
        if rule_id:
            where.append("rule_id = ?")
            params.append(rule_id)
        if feedback_type:
            where.append("feedback_type = ?")
            params.append(feedback_type)
        clause = ("WHERE " + " AND ".join(where)) if where else ""
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT id, rule_id, feedback_type, anomaly_id, metric_value, note, created_at "
                f"FROM feedback {clause} ORDER BY id DESC LIMIT ?",
                params + [limit],
            ).fetchall()
        return [
            {
                "id": r[0], "rule_id": r[1], "feedback_type": r[2],
                "anomaly_id": r[3], "metric_value": r[4],
                "note": r[5], "created_at": r[6],
            }
            for r in rows
        ]

    # ── Internal ──────────────────────────────────────────────────────────────

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(_CREATE_TABLE)
            conn.commit()

    def _connect(self) -> sqlite3.Connection:
        import os
        os.makedirs(os.path.dirname(self.db_path) or ".", exist_ok=True)
        return sqlite3.connect(self.db_path)

    def _counts_per_rule(self) -> Dict[str, Dict[str, int]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT rule_id, feedback_type, COUNT(*) FROM feedback "
                "GROUP BY rule_id, feedback_type"
            ).fetchall()
        result: Dict[str, Dict[str, int]] = {}
        for rule_id, fb_type, count in rows:
            result.setdefault(rule_id, {})[fb_type] = count
        return result


# ── Helpers ───────────────────────────────────────────────────────────────────

def _rescale_rule(rule: GeneratedRule, new_sigma: float) -> GeneratedRule:
    """Recrée une règle avec un nouveau sigma_factor, en recalculant les seuils."""
    import dataclasses
    new_rule = dataclasses.replace(rule, sigma_factor=new_sigma)
    if rule.mean is None or rule.std is None:
        return new_rule

    mean, std = rule.mean, rule.std
    if rule.threshold_low is not None:
        new_rule = dataclasses.replace(new_rule, threshold_low=round(mean - new_sigma * std, 4))
    if rule.threshold_high is not None:
        new_rule = dataclasses.replace(new_rule, threshold_high=round(mean + new_sigma * std, 4))
    return new_rule
