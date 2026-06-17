"""
AlertRuleStore — Système d'alertes configurables (Semaine 14).

Stocke les AlertRule en SQLite. Chaque règle définit pour quels pipelines,
métriques et niveaux de sévérité une notification doit être envoyée, et vers
quels canaux (Teams, Email, console).

Usage :
    from spark.alerting import AlertRuleStore, AlertRule

    store = AlertRuleStore()
    store.add_rule(AlertRule(
        pipeline_name="clean_sales",
        metric_name="job.rejection_rate_pct",
        severity_min="HIGH",
        channels=["teams", "email"],
        recipients=["ops@company.com"],
        cooldown_min=60,
    ))

    matching = store.get_matching_rules("clean_sales", "job.rejection_rate_pct", "CRITICAL")
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from typing import List, Optional

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from spark.alerting.models import AlertRule

_DDL = """
CREATE TABLE IF NOT EXISTS alert_rules (
    rule_id       TEXT PRIMARY KEY,
    pipeline_name TEXT NOT NULL DEFAULT '*',
    metric_name   TEXT NOT NULL DEFAULT '*',
    severity_min  TEXT NOT NULL DEFAULT 'HIGH',
    channels      TEXT NOT NULL DEFAULT '["console"]',
    recipients    TEXT NOT NULL DEFAULT '[]',
    cooldown_min  INTEGER NOT NULL DEFAULT 30,
    enabled       INTEGER NOT NULL DEFAULT 1,
    created_at    TEXT NOT NULL
);
"""

_DEFAULT_RULES: List[dict] = [
    {
        "pipeline_name": "*",
        "metric_name":   "*",
        "severity_min":  "CRITICAL",
        "channels":      ["console"],
        "recipients":    [],
        "cooldown_min":  15,
    },
    {
        "pipeline_name": "*",
        "metric_name":   "job.rejection_rate_pct",
        "severity_min":  "HIGH",
        "channels":      ["console"],
        "recipients":    [],
        "cooldown_min":  30,
    },
    {
        "pipeline_name": "*",
        "metric_name":   "job.rows_output",
        "severity_min":  "HIGH",
        "channels":      ["console"],
        "recipients":    [],
        "cooldown_min":  30,
    },
]


class AlertRuleStore:
    """Persistance et interrogation des règles d'alerte configurables."""

    def __init__(self, db_path: str = "spark/data/alerting.db", seed_defaults: bool = True) -> None:
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self.db_path = db_path
        self._init_db()
        if seed_defaults and self.count() == 0:
            self._seed_defaults()

    # ── CRUD ──────────────────────────────────────────────────────────────────

    def add_rule(self, rule: AlertRule) -> str:
        """Insère ou remplace une règle. Retourne le rule_id."""
        with self._connect() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO alert_rules
                   (rule_id, pipeline_name, metric_name, severity_min,
                    channels, recipients, cooldown_min, enabled, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (
                    rule.rule_id,
                    rule.pipeline_name,
                    rule.metric_name,
                    rule.severity_min.upper(),
                    json.dumps(rule.channels),
                    json.dumps(rule.recipients),
                    rule.cooldown_min,
                    int(rule.enabled),
                    rule.created_at.isoformat(),
                ),
            )
            conn.commit()
        return rule.rule_id

    def remove_rule(self, rule_id: str) -> bool:
        with self._connect() as conn:
            cur = conn.execute("DELETE FROM alert_rules WHERE rule_id=?", (rule_id,))
            conn.commit()
        return cur.rowcount > 0

    def enable(self, rule_id: str) -> bool:
        return self._set_enabled(rule_id, True)

    def disable(self, rule_id: str) -> bool:
        return self._set_enabled(rule_id, False)

    def list_rules(self, enabled_only: bool = False) -> List[AlertRule]:
        where = "WHERE enabled=1" if enabled_only else ""
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM alert_rules {where} ORDER BY created_at"
            ).fetchall()
        return [_row_to_rule(r) for r in rows]

    def get(self, rule_id: str) -> Optional[AlertRule]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM alert_rules WHERE rule_id=?", (rule_id,)
            ).fetchone()
        return _row_to_rule(row) if row else None

    def count(self, enabled_only: bool = False) -> int:
        where = "WHERE enabled=1" if enabled_only else ""
        with self._connect() as conn:
            return conn.execute(
                f"SELECT COUNT(*) FROM alert_rules {where}"
            ).fetchone()[0]

    # ── Matching ──────────────────────────────────────────────────────────────

    def get_matching_rules(
        self, pipeline: str, metric: str, severity: str
    ) -> List[AlertRule]:
        """Retourne les règles actives qui s'appliquent à cette alerte."""
        return [r for r in self.list_rules(enabled_only=True)
                if r.matches(pipeline, metric, severity)]

    # ── Internal ──────────────────────────────────────────────────────────────

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(_DDL)
            conn.commit()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _set_enabled(self, rule_id: str, value: bool) -> bool:
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE alert_rules SET enabled=? WHERE rule_id=?",
                (int(value), rule_id),
            )
            conn.commit()
        return cur.rowcount > 0

    def _seed_defaults(self) -> None:
        for d in _DEFAULT_RULES:
            self.add_rule(AlertRule(**d))


# ── Helpers ───────────────────────────────────────────────────────────────────

def _row_to_rule(row: sqlite3.Row) -> AlertRule:
    d = dict(row)
    return AlertRule(
        rule_id=d["rule_id"],
        pipeline_name=d["pipeline_name"],
        metric_name=d["metric_name"],
        severity_min=d["severity_min"],
        channels=json.loads(d["channels"]),
        recipients=json.loads(d["recipients"]),
        cooldown_min=d["cooldown_min"],
        enabled=bool(d["enabled"]),
        created_at=datetime.fromisoformat(d["created_at"]),
    )
