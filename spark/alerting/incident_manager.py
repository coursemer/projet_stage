"""
IncidentManager — Gestion des incidents (Semaine 14).

Cycle de vie :  open → investigating → resolved

Fonctionnalités :
  - Création d'incident depuis une alerte ou manuellement
  - Mise à jour du statut avec journal des notes
  - Escalade automatique de la sévérité
  - Détection des incidents récurrents (> seuil sur une fenêtre glissante)
  - Liaison aux alertes AlertManager (alert_ids)

Usage :
    from spark.alerting import IncidentManager, IncidentRecord

    mgr = IncidentManager()

    inc = mgr.create(IncidentRecord(
        pipeline="clean_sales",
        title="Taux de rejet élevé",
        description="job.rejection_rate_pct = 45 % (seuil 20 %)",
        severity="HIGH",
        alert_ids=[12, 13],
    ))

    mgr.update_status(inc.id, "investigating", note="En cours d'investigation")
    mgr.resolve(inc.id, note="Source upstream corrigée")

    recurring = mgr.detect_recurring("clean_sales", window_hours=24, threshold=3)
"""
from __future__ import annotations

import json
import os
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from typing import List, Optional

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from spark.alerting.models import IncidentRecord, INCIDENT_STATUSES

_DDL = """
CREATE TABLE IF NOT EXISTS incidents (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    pipeline     TEXT NOT NULL,
    title        TEXT NOT NULL,
    description  TEXT DEFAULT '',
    severity     TEXT NOT NULL DEFAULT 'HIGH',
    status       TEXT NOT NULL DEFAULT 'open',
    alert_ids    TEXT DEFAULT '[]',
    notes        TEXT DEFAULT '[]',
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL,
    resolved_at  TEXT
);
CREATE INDEX IF NOT EXISTS idx_inc_pipeline ON incidents(pipeline, status);
CREATE INDEX IF NOT EXISTS idx_inc_status ON incidents(status);
"""


class IncidentManager:
    """Stockage et gestion du cycle de vie des incidents."""

    def __init__(self, db_path: str = "spark/data/incidents.db") -> None:
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self.db_path = db_path
        self._init_db()

    # ── Création ──────────────────────────────────────────────────────────────

    def create(self, incident: IncidentRecord) -> IncidentRecord:
        """Persiste un nouvel incident. Retourne l'incident avec son ID."""
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            cur = conn.execute(
                """INSERT INTO incidents
                   (pipeline, title, description, severity, status,
                    alert_ids, notes, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                (
                    incident.pipeline,
                    incident.title,
                    incident.description,
                    incident.severity.upper(),
                    incident.status,
                    json.dumps(incident.alert_ids),
                    json.dumps(incident.notes),
                    incident.created_at.isoformat(),
                    now,
                ),
            )
            conn.commit()
            incident.id = cur.lastrowid
        return incident

    @classmethod
    def from_alert(
        cls,
        alert: dict,
        pipeline: Optional[str] = None,
        db_path: str = "spark/data/incidents.db",
    ) -> "tuple[IncidentManager, IncidentRecord]":
        """Crée un IncidentManager et un incident depuis un dict d'alerte."""
        mgr = cls(db_path=db_path)
        sev = str(alert.get("severity", "HIGH")).upper()
        pip = pipeline or str(alert.get("source", "unknown"))
        metric = str(alert.get("metric_name", ""))
        val    = alert.get("value", "N/A")
        details = alert.get("details", "")
        inc = mgr.create(IncidentRecord(
            pipeline=pip,
            title=f"[{sev}] {metric} = {val}",
            description=details or f"Anomalie détectée sur {metric}",
            severity=sev,
            alert_ids=[alert["id"]] if "id" in alert else [],
        ))
        return mgr, inc

    # ── Mise à jour ───────────────────────────────────────────────────────────

    def update_status(
        self, incident_id: int, status: str, note: str = ""
    ) -> bool:
        """Change le statut d'un incident. Ajoute une note datée si fournie."""
        if status not in INCIDENT_STATUSES:
            raise ValueError(f"Statut invalide '{status}'. Valides : {INCIDENT_STATUSES}")
        inc = self.get(incident_id)
        if not inc:
            return False
        if note:
            inc.add_note(note)
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                "UPDATE incidents SET status=?, notes=?, updated_at=? WHERE id=?",
                (status, json.dumps(inc.notes), now, incident_id),
            )
            conn.commit()
        return True

    def resolve(self, incident_id: int, note: str = "") -> bool:
        """Ferme un incident (status=resolved)."""
        inc = self.get(incident_id)
        if not inc:
            return False
        if note:
            inc.add_note(note)
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                """UPDATE incidents
                   SET status='resolved', resolved_at=?, notes=?, updated_at=?
                   WHERE id=?""",
                (now, json.dumps(inc.notes), now, incident_id),
            )
            conn.commit()
        return True

    def escalate(self, incident_id: int) -> Optional[str]:
        """Monte la sévérité d'un cran. Retourne la nouvelle sévérité ou None."""
        inc = self.get(incident_id)
        if not inc:
            return None
        new_sev = inc.escalate()
        inc.add_note(f"Escalade → {new_sev}")
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                "UPDATE incidents SET severity=?, notes=?, updated_at=? WHERE id=?",
                (new_sev, json.dumps(inc.notes), now, incident_id),
            )
            conn.commit()
        return new_sev

    def add_note(self, incident_id: int, text: str) -> bool:
        """Ajoute une note datée à l'incident."""
        inc = self.get(incident_id)
        if not inc:
            return False
        inc.add_note(text)
        with self._connect() as conn:
            conn.execute(
                "UPDATE incidents SET notes=?, updated_at=? WHERE id=?",
                (json.dumps(inc.notes), datetime.now(timezone.utc).isoformat(), incident_id),
            )
            conn.commit()
        return True

    # ── Lecture ───────────────────────────────────────────────────────────────

    def get(self, incident_id: int) -> Optional[IncidentRecord]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM incidents WHERE id=?", (incident_id,)
            ).fetchone()
        return _row_to_incident(row) if row else None

    def list_open(self) -> List[IncidentRecord]:
        return self._query_status(("open", "investigating"))

    def list_by_pipeline(
        self, pipeline: str, status: Optional[str] = None
    ) -> List[IncidentRecord]:
        conditions = ["pipeline=?"]
        params: list = [pipeline]
        if status:
            conditions.append("status=?")
            params.append(status)
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM incidents WHERE {' AND '.join(conditions)} ORDER BY created_at DESC",
                params,
            ).fetchall()
        return [_row_to_incident(r) for r in rows]

    def list_all(self, limit: int = 200) -> List[IncidentRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM incidents ORDER BY created_at DESC LIMIT ?", (limit,)
            ).fetchall()
        return [_row_to_incident(r) for r in rows]

    def count(self, status: Optional[str] = None) -> int:
        where = "WHERE status=?" if status else ""
        params = (status,) if status else ()
        with self._connect() as conn:
            return conn.execute(
                f"SELECT COUNT(*) FROM incidents {where}", params
            ).fetchone()[0]

    def summary(self) -> dict:
        """Résumé global : count par status et par sévérité."""
        with self._connect() as conn:
            by_status = dict(conn.execute(
                "SELECT status, COUNT(*) FROM incidents GROUP BY status"
            ).fetchall())
            by_sev = dict(conn.execute(
                "SELECT severity, COUNT(*) FROM incidents WHERE status != 'resolved' GROUP BY severity"
            ).fetchall())
        return {"total": sum(by_status.values()), "by_status": by_status, "by_severity": by_sev}

    # ── Récurrence ────────────────────────────────────────────────────────────

    def detect_recurring(
        self,
        pipeline:     str,
        window_hours: int = 24,
        threshold:    int = 3,
    ) -> bool:
        """
        True si le pipeline a eu ≥ threshold incidents sur les dernières window_hours heures.
        Permet de déclencher un runbook automatique.
        """
        since = (datetime.now(timezone.utc) - timedelta(hours=window_hours)).isoformat()
        with self._connect() as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM incidents WHERE pipeline=? AND created_at >= ?",
                (pipeline, since),
            ).fetchone()[0]
        return count >= threshold

    def get_recurring_pipelines(
        self, window_hours: int = 24, threshold: int = 3
    ) -> List[str]:
        """Retourne la liste des pipelines en situation de récurrence."""
        since = (datetime.now(timezone.utc) - timedelta(hours=window_hours)).isoformat()
        with self._connect() as conn:
            rows = conn.execute(
                """SELECT pipeline, COUNT(*) as cnt FROM incidents
                   WHERE created_at >= ?
                   GROUP BY pipeline HAVING cnt >= ?""",
                (since, threshold),
            ).fetchall()
        return [r[0] for r in rows]

    # ── Internal ──────────────────────────────────────────────────────────────

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(_DDL)
            conn.commit()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _query_status(self, statuses: tuple) -> List[IncidentRecord]:
        placeholders = ",".join("?" * len(statuses))
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM incidents WHERE status IN ({placeholders}) ORDER BY created_at DESC",
                list(statuses),
            ).fetchall()
        return [_row_to_incident(r) for r in rows]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _row_to_incident(row: sqlite3.Row) -> IncidentRecord:
    d = dict(row)
    return IncidentRecord(
        id=d["id"],
        pipeline=d["pipeline"],
        title=d["title"],
        description=d.get("description", ""),
        severity=d["severity"],
        status=d["status"],
        alert_ids=json.loads(d.get("alert_ids", "[]")),
        notes=json.loads(d.get("notes", "[]")),
        created_at=datetime.fromisoformat(d["created_at"]),
        updated_at=datetime.fromisoformat(d["updated_at"]),
        resolved_at=datetime.fromisoformat(d["resolved_at"]) if d.get("resolved_at") else None,
    )
