"""
DataCatalog — Semaine 12 : publication automatique des descriptions, scores et incidents.

Usage :
    from spark.catalog import DataCatalog, CatalogEntry, Incident

    catalog = DataCatalog()

    # Publier une entrée
    catalog.publish(CatalogEntry(
        name="clean_sales",
        type="pipeline",
        description="Nettoyage et validation des ventes brutes (Pandera + Spark).",
        owner="data-team",
        tags=["spark", "sales", "cleaning"],
        quality_score=0.95,
    ))

    # Mettre à jour le score qualité depuis le Data Trust Agent
    catalog.update_quality_score("clean_sales", score=0.72, n_anomalies=3)

    # Enregistrer un incident
    catalog.record_incident(Incident(
        entry_name="clean_sales",
        severity="HIGH",
        description="Chute de volume : 3 840 lignes vs 47 887 attendues.",
        anomaly_type="volume",
    ))

    # Requêtes
    entry     = catalog.get("clean_sales")
    pipelines = catalog.list_entries(type="pipeline")
    incidents = catalog.incidents("clean_sales", resolved=False)
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from .models import CatalogEntry, Incident

_DDL = """
CREATE TABLE IF NOT EXISTS catalog_entries (
    name         TEXT PRIMARY KEY,
    type         TEXT NOT NULL DEFAULT 'pipeline',
    description  TEXT DEFAULT '',
    owner        TEXT DEFAULT '',
    tags         TEXT DEFAULT '[]',
    quality_score REAL,
    source_path  TEXT DEFAULT '',
    extra        TEXT DEFAULT '{}',
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS incidents (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    entry_name   TEXT NOT NULL,
    severity     TEXT NOT NULL,
    description  TEXT NOT NULL,
    anomaly_type TEXT DEFAULT '',
    resolved     INTEGER NOT NULL DEFAULT 0,
    created_at   TEXT NOT NULL,
    resolved_at  TEXT
);
"""


class DataCatalog:
    """
    Catalogue de données persisté en SQLite.

    Parameters
    ----------
    db_path : chemin vers la base SQLite (défaut : spark/data/catalog.db)
    """

    def __init__(self, db_path: str = "spark/data/catalog.db") -> None:
        import os
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self.db_path = db_path
        self._init_db()

    # ── CRUD entrées ──────────────────────────────────────────────────────────

    def publish(self, entry: CatalogEntry) -> None:
        """Crée ou met à jour une entrée (upsert)."""
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO catalog_entries
                   (name, type, description, owner, tags, quality_score,
                    source_path, extra, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(name) DO UPDATE SET
                     type=excluded.type,
                     description=excluded.description,
                     owner=excluded.owner,
                     tags=excluded.tags,
                     quality_score=excluded.quality_score,
                     source_path=excluded.source_path,
                     extra=excluded.extra,
                     updated_at=excluded.updated_at""",
                (
                    entry.name, entry.type, entry.description, entry.owner,
                    json.dumps(entry.tags), entry.quality_score,
                    entry.source_path, json.dumps(entry.extra),
                    entry.created_at.isoformat(), now,
                ),
            )
            conn.commit()

    def get(self, name: str) -> Optional[CatalogEntry]:
        """Récupère une entrée par son nom."""
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM catalog_entries WHERE name=?", (name,)
            ).fetchone()
        return _row_to_entry(row) if row else None

    def list_entries(
        self,
        type: Optional[str] = None,
        tag: Optional[str] = None,
        min_quality: Optional[float] = None,
    ) -> List[CatalogEntry]:
        """Liste les entrées avec filtres optionnels."""
        where, params = [], []
        if type:
            where.append("type=?"); params.append(type)
        if min_quality is not None:
            where.append("quality_score >= ?"); params.append(min_quality)
        clause = ("WHERE " + " AND ".join(where)) if where else ""
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM catalog_entries {clause} ORDER BY name", params
            ).fetchall()
        entries = [_row_to_entry(r) for r in rows]
        if tag:
            entries = [e for e in entries if tag in e.tags]
        return entries

    def update_quality_score(
        self, name: str, score: float, n_anomalies: int = 0
    ) -> None:
        """Met à jour le score qualité d'une entrée existante."""
        with self._connect() as conn:
            conn.execute(
                "UPDATE catalog_entries SET quality_score=?, updated_at=? WHERE name=?",
                (round(score, 4), datetime.now(timezone.utc).isoformat(), name),
            )
            conn.commit()

    def count(self) -> int:
        with self._connect() as conn:
            return conn.execute("SELECT COUNT(*) FROM catalog_entries").fetchone()[0]

    # ── Incidents ─────────────────────────────────────────────────────────────

    def record_incident(self, incident: Incident) -> int:
        """Enregistre un incident. Retourne son id."""
        with self._connect() as conn:
            cur = conn.execute(
                """INSERT INTO incidents
                   (entry_name, severity, description, anomaly_type, resolved, created_at)
                   VALUES (?,?,?,?,?,?)""",
                (
                    incident.entry_name, incident.severity,
                    incident.description, incident.anomaly_type,
                    int(incident.resolved),
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            conn.commit()
            return cur.lastrowid

    def incidents(
        self,
        entry_name: Optional[str] = None,
        resolved: Optional[bool] = None,
        severity: Optional[str] = None,
        limit: int = 100,
    ) -> List[Incident]:
        where, params = [], []
        if entry_name:
            where.append("entry_name=?"); params.append(entry_name)
        if resolved is not None:
            where.append("resolved=?"); params.append(int(resolved))
        if severity:
            where.append("severity=?"); params.append(severity)
        clause = ("WHERE " + " AND ".join(where)) if where else ""
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT id, entry_name, severity, description, anomaly_type, "
                f"resolved, created_at, resolved_at "
                f"FROM incidents {clause} ORDER BY id DESC LIMIT ?",
                params + [limit],
            ).fetchall()
        return [_row_to_incident(r) for r in rows]

    def resolve_incident(self, incident_id: int) -> bool:
        with self._connect() as conn:
            cur = conn.execute(
                "UPDATE incidents SET resolved=1, resolved_at=? WHERE id=?",
                (datetime.now(timezone.utc).isoformat(), incident_id),
            )
            conn.commit()
        return cur.rowcount > 0

    def incident_summary(self) -> Dict[str, Any]:
        with self._connect() as conn:
            total    = conn.execute("SELECT COUNT(*) FROM incidents").fetchone()[0]
            open_    = conn.execute("SELECT COUNT(*) FROM incidents WHERE resolved=0").fetchone()[0]
            by_sev   = conn.execute(
                "SELECT severity, COUNT(*) FROM incidents GROUP BY severity"
            ).fetchall()
        return {
            "total": total,
            "open": open_,
            "resolved": total - open_,
            "by_severity": dict(by_sev),
        }

    # ── Auto-publish depuis AnomalyDetector ──────────────────────────────────

    def publish_from_detection(
        self,
        pipeline_name: str,
        n_anomalies: int,
        worst_severity: Optional[str],
        anomalies: list,
    ) -> None:
        """
        Met à jour le catalog depuis un résultat AnomalyDetector.
        Crée l'entrée si elle n'existe pas encore.
        """
        # Score qualité : 1.0 si aucune anomalie, décroît selon sévérité
        severity_penalty = {"CRITICAL": 0.50, "HIGH": 0.25, "MEDIUM": 0.10, "LOW": 0.05}
        score = 1.0
        if worst_severity:
            score -= severity_penalty.get(worst_severity, 0.0)
        score = max(0.0, round(score - n_anomalies * 0.02, 3))

        existing = self.get(pipeline_name)
        if not existing:
            self.publish(CatalogEntry(
                name=pipeline_name,
                type="pipeline",
                description=f"Pipeline {pipeline_name} — géré par le Data Trust Agent.",
                tags=["auto-published"],
                quality_score=score,
            ))
        else:
            self.update_quality_score(pipeline_name, score, n_anomalies)

        # Enregistrer un incident par anomalie HIGH/CRITICAL
        for a in anomalies:
            sev = getattr(a, "severity", None)
            if sev in ("HIGH", "CRITICAL"):
                self.record_incident(Incident(
                    entry_name=pipeline_name,
                    severity=str(sev),
                    description=getattr(a, "description", ""),
                    anomaly_type=str(getattr(a, "level", "")),
                ))

    # ── Internal ──────────────────────────────────────────────────────────────

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(_DDL)
            conn.commit()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)


# ── Row converters ────────────────────────────────────────────────────────────

def _row_to_entry(row) -> CatalogEntry:
    name, type_, desc, owner, tags_j, score, src, extra_j, created, updated = row
    return CatalogEntry(
        name=name,
        type=type_,
        description=desc or "",
        owner=owner or "",
        tags=json.loads(tags_j or "[]"),
        quality_score=score,
        source_path=src or "",
        extra=json.loads(extra_j or "{}"),
        created_at=datetime.fromisoformat(created),
        updated_at=datetime.fromisoformat(updated),
    )


def _row_to_incident(row) -> Incident:
    id_, entry, sev, desc, atype, resolved, created, resolved_at = row
    return Incident(
        id=id_,
        entry_name=entry,
        severity=sev,
        description=desc,
        anomaly_type=atype or "",
        resolved=bool(resolved),
        created_at=datetime.fromisoformat(created),
        resolved_at=datetime.fromisoformat(resolved_at) if resolved_at else None,
    )
