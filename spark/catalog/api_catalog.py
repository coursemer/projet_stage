"""
API Catalog — Semaine 13 : router FastAPI pour le Data Catalog, Lineage, Search et Rules.

Endpoints :
  GET  /api/v1/catalog                        liste des entrées (filtres: type, tag, min_quality)
  GET  /api/v1/catalog/{name}                 détail d'une entrée
  POST /api/v1/catalog                        publier / mettre à jour une entrée
  GET  /api/v1/catalog/{name}/incidents       incidents liés à cette entrée
  POST /api/v1/catalog/{name}/incidents/{id}/resolve
  GET  /api/v1/catalog/incidents/summary      résumé global des incidents
  GET  /api/v1/catalog/{name}/lineage         upstream + downstream (dbt + Spark)
  GET  /api/v1/catalog/search?q=...           recherche sémantique (TF-IDF)

  GET  /api/v1/rules                          liste des règles générées (filtres: pipeline, status)
  GET  /api/v1/rules/summary                  résumé pending/approved/rejected
  POST /api/v1/rules/generate/{pipeline}      génère + sauvegarde les règles d'un pipeline
  POST /api/v1/rules/{rule_id}/approve        approuve une règle
  POST /api/v1/rules/{rule_id}/reject         rejette une règle (enregistre FP)
  POST /api/v1/rules/{rule_id}/reset          remet en pending

Inclusion dans api.py :
    from spark.catalog.api_catalog import create_catalog_router
    app.include_router(create_catalog_router(...))
"""
from __future__ import annotations

import os
import sys
from typing import Any, Dict, List, Optional

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from spark.catalog.catalog import DataCatalog
from spark.catalog.lineage import LineageParser
from spark.catalog.models import CatalogEntry, Incident
from spark.catalog.rule_store import RuleStore
from spark.catalog.search import SemanticSearch

DBT_MANIFEST   = os.path.join(BASE_DIR, "dbt", "target", "manifest.json")
SPARK_JOBS_DIR = os.path.join(BASE_DIR, "spark", "jobs")


# ── Pydantic schemas ──────────────────────────────────────────────────────────

class EntryIn(BaseModel):
    name: str
    type: str = "pipeline"
    description: str = ""
    owner: str = ""
    tags: List[str] = []
    quality_score: Optional[float] = None
    source_path: str = ""


class IncidentIn(BaseModel):
    severity: str
    description: str
    anomaly_type: str = ""


class RejectIn(BaseModel):
    note: str = ""


# ── Router factory ────────────────────────────────────────────────────────────

def create_catalog_router(
    catalog_db: str = "spark/data/catalog.db",
    rules_db:   str = "spark/data/rules.db",
) -> APIRouter:

    router  = APIRouter(prefix="/api/v1", tags=["catalog"])
    catalog = DataCatalog(db_path=catalog_db)
    rules   = RuleStore(db_path=rules_db)
    search  = SemanticSearch(use_embeddings=False)
    _lineage_cache: Dict[str, Any] = {}

    def _get_lineage():
        if "graph" not in _lineage_cache:
            parser = LineageParser()
            g = parser.from_spark_jobs(SPARK_JOBS_DIR)
            if os.path.exists(DBT_MANIFEST):
                g = parser.from_dbt(DBT_MANIFEST).merge(g)
            _lineage_cache["graph"] = g
        return _lineage_cache["graph"]

    def _refresh_search():
        entries = catalog.list_entries()
        if entries:
            search.index(entries)

    # ── Catalog entries ───────────────────────────────────────────────────────

    @router.get("/catalog")
    def list_catalog(
        type:        Optional[str]   = Query(None),
        tag:         Optional[str]   = Query(None),
        min_quality: Optional[float] = Query(None),
    ):
        entries = catalog.list_entries(type=type, tag=tag, min_quality=min_quality)
        return {
            "count":   len(entries),
            "entries": [_entry_to_dict(e) for e in entries],
        }

    @router.get("/catalog/incidents/summary")
    def incidents_summary():
        return catalog.incident_summary()

    @router.get("/catalog/search")
    def search_catalog(q: str = Query(..., description="Texte de recherche"), top_k: int = Query(5)):
        _refresh_search()
        if search.n_indexed == 0:
            return {"results": []}
        results = search.query(q, top_k=top_k)
        return {
            "query":   q,
            "backend": search.backend,
            "results": [
                {"name": r.entry.name, "type": r.entry.type,
                 "score": r.score, "description": r.entry.description}
                for r in results
            ],
        }

    @router.get("/catalog/{name}")
    def get_entry(name: str):
        entry = catalog.get(name)
        if not entry:
            raise HTTPException(404, detail=f"Entrée '{name}' introuvable")
        return _entry_to_dict(entry)

    @router.post("/catalog", status_code=201)
    def publish_entry(data: EntryIn):
        catalog.publish(CatalogEntry(
            name=data.name, type=data.type, description=data.description,
            owner=data.owner, tags=data.tags, quality_score=data.quality_score,
            source_path=data.source_path,
        ))
        _lineage_cache.clear()
        return {"published": data.name}

    @router.get("/catalog/{name}/incidents")
    def get_incidents(name: str, resolved: Optional[bool] = Query(None)):
        entry = catalog.get(name)
        if not entry:
            raise HTTPException(404, detail=f"Entrée '{name}' introuvable")
        incs = catalog.incidents(name, resolved=resolved)
        return {"count": len(incs), "incidents": [_inc_to_dict(i) for i in incs]}

    @router.post("/catalog/{name}/incidents", status_code=201)
    def add_incident(name: str, data: IncidentIn):
        entry = catalog.get(name)
        if not entry:
            raise HTTPException(404, detail=f"Entrée '{name}' introuvable")
        inc_id = catalog.record_incident(Incident(
            entry_name=name, severity=data.severity,
            description=data.description, anomaly_type=data.anomaly_type,
        ))
        return {"id": inc_id}

    @router.post("/catalog/{name}/incidents/{inc_id}/resolve")
    def resolve_incident(name: str, inc_id: int):
        ok = catalog.resolve_incident(inc_id)
        if not ok:
            raise HTTPException(404, detail=f"Incident {inc_id} introuvable")
        return {"resolved": inc_id}

    @router.get("/catalog/{name}/lineage")
    def get_lineage(name: str):
        graph = _get_lineage()
        return {
            "name":        name,
            "upstream":    graph.upstream_of(name),
            "downstream":  graph.downstream_of(name),
            "ancestors":   graph.ancestors(name),
            "descendants": graph.descendants(name),
        }

    # ── Rules ─────────────────────────────────────────────────────────────────

    @router.get("/rules")
    def list_rules(
        pipeline: Optional[str] = Query(None),
        status:   Optional[str] = Query(None),
    ):
        rule_list = rules.list_rules(pipeline_name=pipeline, status=status)
        return {"count": len(rule_list), "rules": rule_list}

    @router.get("/rules/summary")
    def rules_summary():
        return rules.summary()

    @router.post("/rules/generate/{pipeline}", status_code=201)
    def generate_rules(pipeline: str):
        """
        Génère les règles pour un pipeline depuis les métriques historiques simulées.
        En production, remplacer _mock_history() par build_pipeline_snapshots(store).
        """
        from spark.metrics.validation import TestGenerator
        history = _mock_history(pipeline)
        gen     = TestGenerator(sigma_factor=3.0)
        gen_rules = gen.generate(history)
        n = rules.save_rules(gen_rules, overwrite=False)
        return {
            "pipeline": pipeline,
            "generated": len(gen_rules),
            "saved": n,
            "rule_ids": [r.rule_id for r in gen_rules],
        }

    @router.post("/rules/{rule_id}/approve")
    def approve_rule(rule_id: str):
        if not rules.approve(rule_id):
            raise HTTPException(404, detail=f"Règle '{rule_id}' introuvable")
        return {"approved": rule_id}

    @router.post("/rules/{rule_id}/reject")
    def reject_rule(rule_id: str, data: RejectIn = RejectIn()):
        if not rules.reject(rule_id, note=data.note):
            raise HTTPException(404, detail=f"Règle '{rule_id}' introuvable")
        return {"rejected": rule_id, "note": data.note}

    @router.post("/rules/{rule_id}/reset")
    def reset_rule(rule_id: str):
        if not rules.reset(rule_id):
            raise HTTPException(404, detail=f"Règle '{rule_id}' introuvable")
        return {"reset": rule_id}

    return router


# ── Helpers ───────────────────────────────────────────────────────────────────

def _entry_to_dict(e: CatalogEntry) -> Dict[str, Any]:
    return {
        "name": e.name, "type": e.type, "description": e.description,
        "owner": e.owner, "tags": e.tags, "quality_score": e.quality_score,
        "source_path": e.source_path,
        "created_at": e.created_at.isoformat(),
        "updated_at": e.updated_at.isoformat(),
    }


def _inc_to_dict(i: Incident) -> Dict[str, Any]:
    return {
        "id": i.id, "entry_name": i.entry_name, "severity": i.severity,
        "description": i.description, "anomaly_type": i.anomaly_type,
        "resolved": i.resolved,
        "created_at": i.created_at.isoformat(),
        "resolved_at": i.resolved_at.isoformat() if i.resolved_at else None,
    }


def _mock_history(pipeline: str):
    """Historique simulé minimal pour la génération de règles via l'API."""
    import random
    from spark.metrics.anomaly_detection.models import PipelineMetrics
    rng = random.Random(42)
    return [
        PipelineMetrics(
            pipeline_name=pipeline,
            row_count=int(50_000 + rng.gauss(0, 1000)),
            duration_seconds=30.0 + rng.gauss(0, 2.0),
            success=True,
            task_failures=0,
        )
        for _ in range(30)
    ]
