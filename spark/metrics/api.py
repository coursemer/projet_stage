"""
API de métriques centralisée — FastAPI REST.

Endpoints :
  GET  /api/v1/health
  GET  /api/v1/metrics                 ?source=&name=&from_ts=&to_ts=&limit=
  GET  /api/v1/metrics/sources         liste des sources disponibles
  GET  /api/v1/metrics/summary         dernière valeur + stats par (source, name)
  GET  /api/v1/metrics/history/{name}  série temporelle d'une métrique
  POST /api/v1/metrics/push            insérer un ou plusieurs points

Démarrage :
    python spark/metrics/api.py
    python spark/metrics/api.py --port 8090 --db spark/data/metrics.db

Exemple curl :
    curl http://localhost:8090/api/v1/health
    curl http://localhost:8090/api/v1/metrics/summary
    curl http://localhost:8090/api/v1/metrics?source=spark&limit=10
    curl -X POST http://localhost:8090/api/v1/metrics/push \\
         -H "Content-Type: application/json" \\
         -d '[{"source":"spark","name":"ingest_sales.rows_output","value":48000}]'
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from fastapi import FastAPI, HTTPException, Query
    from fastapi.middleware.cors import CORSMiddleware
    from pydantic import BaseModel, Field
    _FASTAPI_OK = True
except ImportError:
    _FASTAPI_OK = False

from .storage import MetricPoint, SQLiteMetricsStore, DEFAULT_DB


# ── Pydantic schema — défini au niveau module pour que Pydantic v2 puisse le résoudre ──

if _FASTAPI_OK:
    class MetricIn(BaseModel):  # type: ignore[misc]
        source: str
        name:   str
        value:  float
        ts:     Optional[str] = Field(
            default_factory=lambda: datetime.now(timezone.utc).isoformat()
        )
        tags:  Dict[str, Any] = {}
        extra: Dict[str, Any] = {}
else:
    MetricIn = None  # type: ignore[assignment,misc]


# ── App factory ───────────────────────────────────────────────────────────────

def create_app(db_path: str = DEFAULT_DB) -> "FastAPI":  # type: ignore[return]
    if not _FASTAPI_OK:
        raise RuntimeError(
            "FastAPI non installé. Exécutez : pip install fastapi"
        )

    app = FastAPI(
        title="Data Trust — Metrics API",
        description="API de métriques centralisée pour Airflow, Spark et dbt.",
        version="1.0.0",
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"], allow_methods=["*"], allow_headers=["*"],
    )

    store = SQLiteMetricsStore(db_path=db_path)

    # ── Routes ────────────────────────────────────────────────────────────────

    @app.get("/api/v1/health")
    def health():
        return {
            "status":        "ok",
            "ts":            datetime.now(timezone.utc).isoformat(),
            "total_metrics": store.count(),
            "db_path":       db_path,
        }

    @app.get("/api/v1/metrics")
    def get_metrics(
        source:  Optional[str] = Query(None, description="Filtrer par source (airflow/spark/dbt)"),
        name:    Optional[str] = Query(None, description="Filtrer par nom de métrique"),
        from_ts: Optional[str] = Query(None, description="Borne inférieure ISO-8601"),
        to_ts:   Optional[str] = Query(None, description="Borne supérieure ISO-8601"),
        limit:   int           = Query(500,  description="Nombre max de résultats"),
    ):
        rows = store.query(source=source, name=name, from_ts=from_ts, to_ts=to_ts, limit=limit)
        return {"count": len(rows), "metrics": rows}

    @app.get("/api/v1/metrics/sources")
    def get_sources():
        return {"sources": store.sources()}

    @app.get("/api/v1/metrics/summary")
    def get_summary():
        rows = store.summary()
        return {"count": len(rows), "summary": rows}

    @app.get("/api/v1/metrics/history/{name:path}")
    def get_history(
        name:    str,
        source:  Optional[str] = Query(None),
        from_ts: Optional[str] = Query(None),
        to_ts:   Optional[str] = Query(None),
        limit:   int           = Query(1000),
    ):
        rows = store.query(source=source, name=name, from_ts=from_ts, to_ts=to_ts, limit=limit)
        if not rows:
            raise HTTPException(status_code=404, detail=f"Aucune métrique trouvée pour '{name}'")
        rows.sort(key=lambda r: r["ts"])
        return {"name": name, "count": len(rows), "series": rows}

    @app.post("/api/v1/metrics/push", status_code=201)
    def push_metrics(points: List[MetricIn]):  # type: ignore[valid-type]
        if not points:
            raise HTTPException(status_code=400, detail="Liste de points vide")
        metric_points = [
            MetricPoint(
                source=p.source, name=p.name, value=p.value,
                ts=p.ts or datetime.now(timezone.utc).isoformat(),
                tags=p.tags, extra=p.extra,
            )
            for p in points
        ]
        n = store.write(metric_points)
        return {"inserted": n, "ts": datetime.now(timezone.utc).isoformat()}

    return app


# ── CLI ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Démarre l'API de métriques")
    parser.add_argument("--host",   default="0.0.0.0",  help="Hôte (défaut: 0.0.0.0)")
    parser.add_argument("--port",   type=int, default=8090, help="Port (défaut: 8090)")
    parser.add_argument("--db",     default=DEFAULT_DB, help="Chemin vers metrics.db")
    parser.add_argument("--reload", action="store_true", help="Rechargement auto (dev)")
    args = parser.parse_args()

    if not _FASTAPI_OK:
        print("Erreur : FastAPI non installé. Exécutez : pip install fastapi")
        return

    try:
        import uvicorn
    except ImportError:
        print("Erreur : uvicorn non installé. Exécutez : pip install uvicorn")
        return

    print(f"[metrics/api] Démarrage sur http://{args.host}:{args.port}")
    print(f"[metrics/api] Base SQLite    : {args.db}")
    print(f"[metrics/api] Documentation  : http://localhost:{args.port}/docs")

    app = create_app(db_path=args.db)
    uvicorn.run(app, host=args.host, port=args.port, reload=args.reload)


if __name__ == "__main__":
    main()
