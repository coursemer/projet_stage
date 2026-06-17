"""
RunbookEngine — Runbooks automatiques pour incidents récurrents (Semaine 14).

Fonctionnalités :
  - Catalogue de runbooks pré-chargés (5 types d'incidents courants)
  - Recherche du runbook le plus adapté à un incident
  - Exécution dry-run (liste des étapes) ou réelle (commandes shell)
  - Enregistrement d'un runbook pour un pattern récurrent
  - SQLite pour la persistance des runbooks et des exécutions

Runbooks pré-chargés :
  rejection_rate_high  — Taux de rejet anormal
  volume_drop          — Chute de volume de données
  duration_spike       — Durée d'exécution anormalement longue
  null_rate_high       — Taux de valeurs nulles élevé
  duplicate_spike      — Pics de doublons

Usage :
    from spark.alerting import RunbookEngine, IncidentRecord

    engine = RunbookEngine()
    rb = engine.find_runbook("job.rejection_rate_pct")
    print(rb.title, rb.steps)

    inc = IncidentRecord(pipeline="clean_sales", title="Rejet 45 %", severity="HIGH")
    rb = engine.suggest_for_incident(inc)
    result = engine.execute(rb, dry_run=True)   # retourne les étapes sans les exécuter
"""
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from typing import List, Optional

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from spark.alerting.models import IncidentRecord, RunbookEntry

_DDL = """
CREATE TABLE IF NOT EXISTS runbooks (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    pattern    TEXT NOT NULL UNIQUE,
    title      TEXT NOT NULL,
    steps      TEXT NOT NULL DEFAULT '[]',
    tags       TEXT NOT NULL DEFAULT '[]',
    auto_exec  INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS runbook_executions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    runbook_id  INTEGER NOT NULL,
    incident_id INTEGER,
    pipeline    TEXT,
    dry_run     INTEGER NOT NULL DEFAULT 1,
    steps_done  TEXT NOT NULL DEFAULT '[]',
    status      TEXT NOT NULL DEFAULT 'pending',
    executed_at TEXT NOT NULL
);
"""

_DEFAULT_RUNBOOKS: List[dict] = [
    {
        "pattern": "rejection_rate_high",
        "title":   "Taux de rejet anormal",
        "tags":    ["data-quality", "etl"],
        "auto_exec": False,
        "steps": [
            "1. Vérifier les logs du job dans spark/logs/ ou Airflow UI",
            "2. Inspecter les données sources dans spark/data/raw/ — chercher les colonnes avec trop de nulls",
            "3. Lancer : python inject_to_db.py --dry-run --types nulls --pipeline <pipeline>",
            "4. Comparer le schéma actuel avec le schéma attendu (dbt source freshness)",
            "5. Si source upstream dégradée : déclencher une alerte vers l'équipe data source",
            "6. Si le rejet persiste > 30 min : escalader l'incident vers CRITICAL",
            "7. Corriger la source, relancer le pipeline, valider en monitoring",
        ],
    },
    {
        "pattern": "volume_drop",
        "title":   "Chute de volume de données",
        "tags":    ["volume", "etl", "monitoring"],
        "auto_exec": False,
        "steps": [
            "1. Vérifier si le fichier source est vide ou incomplet (ls -lh spark/data/raw/)",
            "2. Contrôler le taux de succès du pipeline en amont (dag.status dans les métriques)",
            "3. Vérifier la fenêtre temporelle — chute de volume normale le week-end ?",
            "4. Comparer avec l'historique : python -c \"from spark.metrics.storage import get_store; ...\"",
            "5. Si le volume est à 0 : arrêter les jobs en aval, éviter la propagation",
            "6. Notifier l'équipe source avec le volume attendu vs observé",
            "7. Documenter dans l'incident et résoudre après normalisation du volume",
        ],
    },
    {
        "pattern": "duration_spike",
        "title":   "Durée d'exécution anormalement longue",
        "tags":    ["performance", "etl", "sla"],
        "auto_exec": False,
        "steps": [
            "1. Identifier le job en cours d'exécution (Airflow UI ou ps aux | grep python)",
            "2. Vérifier l'utilisation CPU/mémoire pendant l'exécution",
            "3. Inspecter le volume de données en entrée — plus grand que d'habitude ?",
            "4. Regarder les logs pour des attentes sur verrous ou connexions DB",
            "5. Si durée > 3× la normale : killer le job et relancer en dehors des heures de pointe",
            "6. Optimiser la requête ou augmenter les ressources si récurrent",
            "7. Ajuster le seuil SLA dans DEFAULT_THRESHOLDS si le nouveau comportement est attendu",
        ],
    },
    {
        "pattern": "null_rate_high",
        "title":   "Taux de valeurs nulles élevé",
        "tags":    ["data-quality", "schema", "nulls"],
        "auto_exec": False,
        "steps": [
            "1. Identifier les colonnes avec le plus fort taux de nulls via data_profiling",
            "2. Vérifier si ces colonnes sont déclarées NOT NULL dans le schéma dbt",
            "3. Remonter à la source (CRM / ERP / API) pour identifier la cause",
            "4. Appliquer un test dbt : dbt test --select source:<source>",
            "5. Si les nulls sont tolérables : mettre à jour le schéma et la documentation",
            "6. Si les nulls sont critiques : rejeter les lignes en amont et alerter la source",
            "7. Relancer le pipeline après correction de la source",
        ],
    },
    {
        "pattern": "duplicate_spike",
        "title":   "Pic de doublons",
        "tags":    ["data-quality", "duplicates", "deduplication"],
        "auto_exec": False,
        "steps": [
            "1. Identifier les colonnes-clés concernées (order_id, customer_id…)",
            "2. Compter les doublons exacts vs doublons partiels",
            "3. Vérifier si le pipeline a été rejoué (double ingestion accidentelle)",
            "4. Dédupliquer via : df.drop_duplicates(subset=['order_id'], keep='last')",
            "5. Ajouter une contrainte UNIQUE dans dbt si absente",
            "6. Vérifier l'idempotence du pipeline (peut-il être relancé sans créer de doublons ?)",
            "7. Documenter la cause et ajouter un test automatique pour détecter les futurs pics",
        ],
    },
]

# Mots-clés de matching : métrique/titre → pattern
_KEYWORD_MAP = {
    "rejection_rate":  "rejection_rate_high",
    "rows_rejected":   "rejection_rate_high",
    "rejet":           "rejection_rate_high",
    "rows_output":     "volume_drop",
    "row_count":       "volume_drop",
    "volume":          "volume_drop",
    "chute":           "volume_drop",
    "duration":        "duration_spike",
    "durée":           "duration_spike",
    "performance":     "duration_spike",
    "slow":            "duration_spike",
    "null_rate":       "null_rate_high",
    "null":            "null_rate_high",
    "nulls":           "null_rate_high",
    "duplicate":       "duplicate_spike",
    "doublon":         "duplicate_spike",
    "dup_rate":        "duplicate_spike",
}


class RunbookEngine:
    """Moteur de runbooks : recherche, suggestion et exécution."""

    def __init__(self, db_path: str = "spark/data/runbooks.db") -> None:
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self.db_path = db_path
        self._init_db()

    # ── Recherche ─────────────────────────────────────────────────────────────

    def find_runbook(self, metric_or_title: str) -> Optional[RunbookEntry]:
        """Retourne le runbook le plus adapté à la métrique ou au titre fourni."""
        key = metric_or_title.lower().replace("-", "_")
        for keyword, pattern in _KEYWORD_MAP.items():
            if keyword in key:
                return self._get_by_pattern(pattern)
        return None

    def suggest_for_incident(self, incident: IncidentRecord) -> Optional[RunbookEntry]:
        """Suggère un runbook pour un incident (cherche dans titre + pipeline)."""
        combined = f"{incident.title} {incident.description}".lower()
        for keyword, pattern in _KEYWORD_MAP.items():
            if keyword in combined:
                return self._get_by_pattern(pattern)
        return None

    def get_all(self) -> List[RunbookEntry]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM runbooks ORDER BY id").fetchall()
        return [_row_to_runbook(r) for r in rows]

    def get_by_id(self, runbook_id: int) -> Optional[RunbookEntry]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM runbooks WHERE id=?", (runbook_id,)
            ).fetchone()
        return _row_to_runbook(row) if row else None

    # ── Enregistrement ────────────────────────────────────────────────────────

    def register(self, entry: RunbookEntry) -> int:
        """Ajoute ou remplace un runbook. Retourne l'ID."""
        with self._connect() as conn:
            cur = conn.execute(
                """INSERT OR REPLACE INTO runbooks
                   (pattern, title, steps, tags, auto_exec, created_at)
                   VALUES (?,?,?,?,?,?)""",
                (
                    entry.pattern,
                    entry.title,
                    json.dumps(entry.steps),
                    json.dumps(entry.tags),
                    int(entry.auto_exec),
                    entry.created_at.isoformat(),
                ),
            )
            conn.commit()
            entry.id = cur.lastrowid
        return entry.id

    def register_recurring(
        self, pipeline: str, metric: str, steps: Optional[List[str]] = None
    ) -> RunbookEntry:
        """
        Crée un runbook spécifique pour un couple (pipeline, métrique) récurrent.
        """
        pattern = f"{pipeline}__{metric}"
        title   = f"Runbook automatique — {pipeline} / {metric}"
        default_steps = [
            f"1. Vérifier les métriques récentes de {pipeline} dans le dashboard",
            f"2. Inspecter les alertes sur {metric} dans les dernières 24h",
            f"3. Rechercher un runbook standard pour {metric}",
            f"4. Escalader à l'équipe responsable de {pipeline} si non résolu en 1h",
        ]
        rb = RunbookEntry(
            pattern=pattern,
            title=title,
            steps=steps or default_steps,
            tags=["recurring", pipeline, metric],
            auto_exec=False,
        )
        self.register(rb)
        return rb

    # ── Exécution ─────────────────────────────────────────────────────────────

    def execute(
        self,
        runbook:     RunbookEntry,
        incident:    Optional[IncidentRecord] = None,
        dry_run:     bool = True,
    ) -> List[str]:
        """
        Exécute un runbook.

        dry_run=True  : retourne les étapes textuelles sans lancer de commande shell
        dry_run=False : exécute les steps dont la ligne commence par '$ '
                        (convention : "$ python script.py arg1")

        Retourne la liste des outputs par étape.
        """
        outputs = []
        for step in runbook.steps:
            if not dry_run and step.strip().startswith("$ "):
                cmd = step.strip()[2:]
                try:
                    result = subprocess.run(
                        cmd, shell=True, capture_output=True, text=True, timeout=60
                    )
                    out = result.stdout.strip() or result.stderr.strip() or "(no output)"
                    outputs.append(f"[EXEC] {cmd}\n  → {out}")
                except subprocess.TimeoutExpired:
                    outputs.append(f"[TIMEOUT] {cmd}")
                except Exception as exc:
                    outputs.append(f"[ERROR] {cmd} — {exc}")
            else:
                outputs.append(f"[STEP] {step}")

        self._log_execution(
            runbook_id=runbook.id,
            incident_id=incident.id if incident else None,
            pipeline=incident.pipeline if incident else None,
            dry_run=dry_run,
            steps_done=outputs,
        )
        return outputs

    # ── Log exécutions ────────────────────────────────────────────────────────

    def execution_history(self, runbook_id: Optional[int] = None, limit: int = 50) -> List[dict]:
        where  = "WHERE runbook_id=?" if runbook_id else ""
        params = (runbook_id, limit) if runbook_id else (limit,)
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM runbook_executions {where} ORDER BY executed_at DESC LIMIT ?",
                params,
            ).fetchall()
        return [dict(r) for r in rows]

    # ── Internal ──────────────────────────────────────────────────────────────

    def _get_by_pattern(self, pattern: str) -> Optional[RunbookEntry]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM runbooks WHERE pattern=?", (pattern,)
            ).fetchone()
        return _row_to_runbook(row) if row else None

    def _log_execution(
        self,
        runbook_id:  int,
        incident_id: Optional[int],
        pipeline:    Optional[str],
        dry_run:     bool,
        steps_done:  List[str],
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO runbook_executions
                   (runbook_id, incident_id, pipeline, dry_run, steps_done, status, executed_at)
                   VALUES (?,?,?,?,?,'done',?)""",
                (
                    runbook_id,
                    incident_id,
                    pipeline,
                    int(dry_run),
                    json.dumps(steps_done),
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            conn.commit()

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(_DDL)
            conn.commit()
        self._seed_defaults()

    def _seed_defaults(self) -> None:
        with self._connect() as conn:
            n = conn.execute("SELECT COUNT(*) FROM runbooks").fetchone()[0]
        if n == 0:
            for d in _DEFAULT_RUNBOOKS:
                self.register(RunbookEntry(**d))

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn


# ── Helpers ───────────────────────────────────────────────────────────────────

def _row_to_runbook(row: sqlite3.Row) -> RunbookEntry:
    d = dict(row)
    return RunbookEntry(
        id=d["id"],
        pattern=d["pattern"],
        title=d["title"],
        steps=json.loads(d["steps"]),
        tags=json.loads(d["tags"]),
        auto_exec=bool(d.get("auto_exec", 0)),
        created_at=datetime.fromisoformat(d["created_at"]),
    )
