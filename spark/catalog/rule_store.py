"""
RuleStore — Persistance des règles générées avec statut d'approbation opérateur.

Statuts :
  pending  — règle générée, en attente de validation
  approved — règle validée par un opérateur
  rejected — règle rejetée (génère automatiquement un FP dans FeedbackLoop)

Usage :
    from spark.catalog.rule_store import RuleStore
    from spark.metrics.validation import TestGenerator

    store = RuleStore()
    rules = TestGenerator().generate(history)
    store.save_rules(rules)                          # → statut "pending"

    store.approve("ingest_sales__row_count__30s")
    store.reject("ingest_sales__duration_seconds__30s", note="Trop strict")

    approved = store.list_rules(status="approved")
    pending  = store.list_rules(status="pending")
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import sys, os
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from spark.metrics.validation.models import GeneratedRule

_DDL = """
CREATE TABLE IF NOT EXISTS generated_rules (
    rule_id        TEXT PRIMARY KEY,
    pipeline_name  TEXT NOT NULL,
    metric_name    TEXT NOT NULL,
    sigma_factor   REAL NOT NULL,
    threshold_low  REAL,
    threshold_high REAL,
    mean           REAL,
    std            REAL,
    n_samples      INTEGER DEFAULT 0,
    confidence     REAL DEFAULT 0.0,
    fp_count       INTEGER DEFAULT 0,
    fn_count       INTEGER DEFAULT 0,
    status         TEXT NOT NULL DEFAULT 'pending',
    status_note    TEXT DEFAULT '',
    generated_at   TEXT NOT NULL,
    reviewed_at    TEXT
);
"""


class RuleStore:
    """Stocke les GeneratedRule en SQLite avec leur statut d'approbation."""

    def __init__(self, db_path: str = "spark/data/rules.db") -> None:
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self.db_path = db_path
        self._init_db()

    # ── CRUD ──────────────────────────────────────────────────────────────────

    def save_rules(self, rules: List[GeneratedRule], overwrite: bool = False) -> int:
        """Sauvegarde une liste de règles. Retourne le nombre inséré."""
        n = 0
        for rule in rules:
            n += self._upsert(rule, overwrite=overwrite)
        return n

    def get(self, rule_id: str) -> Optional[GeneratedRule]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM generated_rules WHERE rule_id=?", (rule_id,)
            ).fetchone()
        return _row_to_rule(row) if row else None

    def list_rules(
        self,
        pipeline_name: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        where, params = [], []
        if pipeline_name:
            where.append("pipeline_name=?"); params.append(pipeline_name)
        if status:
            where.append("status=?"); params.append(status)
        clause = ("WHERE " + " AND ".join(where)) if where else ""
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM generated_rules {clause} ORDER BY pipeline_name, metric_name LIMIT ?",
                params + [limit],
            ).fetchall()
        return [_row_to_dict(r) for r in rows]

    def count(self, status: Optional[str] = None) -> int:
        if status:
            with self._connect() as conn:
                return conn.execute(
                    "SELECT COUNT(*) FROM generated_rules WHERE status=?", (status,)
                ).fetchone()[0]
        with self._connect() as conn:
            return conn.execute("SELECT COUNT(*) FROM generated_rules").fetchone()[0]

    # ── Approval workflow ─────────────────────────────────────────────────────

    def approve(self, rule_id: str, note: str = "") -> bool:
        return self._set_status(rule_id, "approved", note)

    def reject(self, rule_id: str, note: str = "") -> bool:
        ok = self._set_status(rule_id, "rejected", note)
        if ok:
            self._increment_fp(rule_id)
        return ok

    def reset(self, rule_id: str) -> bool:
        return self._set_status(rule_id, "pending", "")

    def summary(self) -> Dict[str, int]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT status, COUNT(*) FROM generated_rules GROUP BY status"
            ).fetchall()
        return dict(rows)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(_DDL)
            conn.commit()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _upsert(self, rule: GeneratedRule, overwrite: bool) -> int:
        conflict = "REPLACE" if overwrite else "IGNORE"
        with self._connect() as conn:
            cur = conn.execute(
                f"""INSERT OR {conflict} INTO generated_rules
                    (rule_id, pipeline_name, metric_name, sigma_factor,
                     threshold_low, threshold_high, mean, std, n_samples,
                     confidence, fp_count, fn_count, status, generated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,'pending',?)""",
                (
                    rule.rule_id, rule.pipeline_name, rule.metric_name,
                    rule.sigma_factor, rule.threshold_low, rule.threshold_high,
                    rule.mean, rule.std, rule.n_samples, rule.confidence,
                    rule.fp_count, rule.fn_count,
                    rule.generated_at.isoformat(),
                ),
            )
            conn.commit()
            return cur.rowcount

    def _set_status(self, rule_id: str, status: str, note: str) -> bool:
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE generated_rules SET status=?, status_note=?, reviewed_at=? WHERE rule_id=?",
                (status, note, datetime.now(timezone.utc).isoformat(), rule_id),
            )
            conn.commit()
        return cur.rowcount > 0

    def _increment_fp(self, rule_id: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "UPDATE generated_rules SET fp_count=fp_count+1 WHERE rule_id=?",
                (rule_id,),
            )
            conn.commit()


# ── Converters ────────────────────────────────────────────────────────────────

_COLS = [
    "rule_id", "pipeline_name", "metric_name", "sigma_factor",
    "threshold_low", "threshold_high", "mean", "std", "n_samples",
    "confidence", "fp_count", "fn_count", "status", "status_note",
    "generated_at", "reviewed_at",
]


def _row_to_dict(row) -> Dict[str, Any]:
    return dict(zip(_COLS, row))


def _row_to_rule(row) -> GeneratedRule:
    d = _row_to_dict(row)
    return GeneratedRule(
        rule_id=d["rule_id"],
        pipeline_name=d["pipeline_name"],
        metric_name=d["metric_name"],
        sigma_factor=d["sigma_factor"],
        threshold_low=d["threshold_low"],
        threshold_high=d["threshold_high"],
        mean=d["mean"],
        std=d["std"],
        n_samples=d["n_samples"] or 0,
        confidence=d["confidence"] or 0.0,
        fp_count=d["fp_count"] or 0,
        fn_count=d["fn_count"] or 0,
        generated_at=datetime.fromisoformat(d["generated_at"]),
    )
