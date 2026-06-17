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
    from fastapi import FastAPI, HTTPException, Query, Request
    from fastapi.middleware.cors import CORSMiddleware
    from pydantic import BaseModel, Field
    _FASTAPI_OK = True
except ImportError:
    _FASTAPI_OK = False

import sys
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

try:
    from .storage           import MetricPoint, SQLiteMetricsStore, DEFAULT_DB
    from .anomaly_detector  import AnomalyDetector, AnomalyAlert
    from .alert_manager     import AlertManager
    from .pipeline_adapter  import build_pipeline_snapshots
    from .anomaly_detection import AnomalyDetector as MLAnomalyDetector
    from .anomaly_detection.models import Severity as MLSeverity
    from .llm_explainer     import LLMExplainer
except ImportError:
    from spark.metrics.storage                  import MetricPoint, SQLiteMetricsStore, DEFAULT_DB
    from spark.metrics.anomaly_detector         import AnomalyDetector, AnomalyAlert
    from spark.metrics.alert_manager            import AlertManager
    from spark.metrics.pipeline_adapter         import build_pipeline_snapshots
    from spark.metrics.anomaly_detection        import AnomalyDetector as MLAnomalyDetector
    from spark.metrics.anomaly_detection.models import Severity as MLSeverity
    from spark.metrics.llm_explainer            import LLMExplainer

_ML_SEV_MAP = {
    MLSeverity.CRITICAL: "critical",
    MLSeverity.HIGH:     "warning",
    MLSeverity.MEDIUM:   "warning",
    MLSeverity.LOW:      "info",
}


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

    store     = SQLiteMetricsStore(db_path=db_path)
    alert_mgr = AlertManager(db_path=db_path)

    # ── Prometheus /metrics endpoint ──────────────────────────────────────────
    try:
        from spark.metrics.prometheus_exporter import PrometheusExporter, add_metrics_route
        _prom_exporter = PrometheusExporter(store, alert_mgr)
        add_metrics_route(app, _prom_exporter)
    except Exception:
        pass  # prometheus_client non installé → endpoint silencieusement absent

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

    # ── Alertes ───────────────────────────────────────────────────────────────

    @app.get("/api/v1/alerts")
    def get_alerts(
        source:       Optional[str]  = Query(None),
        severity:     Optional[str]  = Query(None, description="critical | warning | info"),
        metric_name:  Optional[str]  = Query(None),
        algorithm:    Optional[str]  = Query(None),
        acknowledged: Optional[bool] = Query(None),
        from_ts:      Optional[str]  = Query(None),
        to_ts:        Optional[str]  = Query(None),
        limit:        int            = Query(200),
    ):
        rows = alert_mgr.query(
            source=source, severity=severity, metric_name=metric_name,
            algorithm=algorithm, acknowledged=acknowledged,
            from_ts=from_ts, to_ts=to_ts, limit=limit,
        )
        return {"count": len(rows), "alerts": rows}

    @app.get("/api/v1/alerts/summary")
    def get_alerts_summary():
        return alert_mgr.summary()

    @app.post("/api/v1/alerts/detect", status_code=201)
    def detect_anomalies():
        """Lance la détection d'anomalies sur les métriques stockées et sauvegarde les alertes."""
        detector = AnomalyDetector(store)
        alerts   = detector.run()
        n_saved  = alert_mgr.save(alerts)
        by_sev: dict = {}
        for a in alerts:
            by_sev[a.severity] = by_sev.get(a.severity, 0) + 1
        return {
            "detected": len(alerts),
            "saved":    n_saved,
            "by_severity": by_sev,
            "ts": datetime.now(timezone.utc).isoformat(),
        }

    @app.patch("/api/v1/alerts/{alert_id}/acknowledge")
    def acknowledge_alert(alert_id: int):
        ok = alert_mgr.acknowledge(alert_id)
        if not ok:
            raise HTTPException(status_code=404, detail=f"Alerte {alert_id} introuvable")
        return {"acknowledged": True, "alert_id": alert_id}

    @app.post("/api/v1/alerts/detect-ml", status_code=201)
    def detect_anomalies_ml():
        """Détection ML multi-couches (Volume/Distribution/Schema/Performance/Temporal/ML)."""
        snapshots  = build_pipeline_snapshots(store)
        all_alerts: list = []
        results_by_pipeline: dict = {}

        for pipeline_name, (current, history) in snapshots.items():
            try:
                detector = MLAnomalyDetector.for_pipeline(pipeline_name=pipeline_name)
                result   = detector.detect(current, history)
            except Exception as exc:
                results_by_pipeline[pipeline_name] = {"error": str(exc)}
                continue

            pipeline_alerts = []
            for anomaly in result.anomalies:
                sev_str = _ML_SEV_MAP.get(anomaly.severity, "info")
                alert   = AnomalyAlert(
                    metric_name=anomaly.metric_name,
                    source=pipeline_name,
                    algorithm=f"ml:{anomaly.level}",
                    severity=sev_str,
                    value=anomaly.observed_value or 0.0,
                    expected=(
                        str(anomaly.expected_value)
                        if anomaly.expected_value is not None
                        else anomaly.description[:80]
                    ),
                    details=anomaly.description + f" [score={anomaly.severity_score:.1f}]",
                )
                pipeline_alerts.append(alert)
                all_alerts.append(alert)

            results_by_pipeline[pipeline_name] = result.summary()

        n_saved = alert_mgr.save(all_alerts)
        by_sev: dict = {}
        for a in all_alerts:
            by_sev[a.severity] = by_sev.get(a.severity, 0) + 1

        return {
            "pipelines_analyzed": len(snapshots),
            "detected":           len(all_alerts),
            "saved":              n_saved,
            "by_severity":        by_sev,
            "by_pipeline":        results_by_pipeline,
            "ts":                 datetime.now(timezone.utc).isoformat(),
        }

    @app.post("/api/v1/alerts/explain", status_code=200)
    def explain_alerts(
        source:   Optional[str] = Query(None, description="Filtrer par pipeline"),
        severity: Optional[str] = Query(None, description="critical | warning | info"),
        limit:    int           = Query(20,   description="Nombre max d'alertes à expliquer"),
    ):
        """
        Génère des explications LLM (Mistral AI) pour les alertes ML récentes.
        Ref: analyse_llm.docx — Mistral Large 2, RGPD-conforme, hébergé en France.
        Fallback: Ollama local → template si aucun backend disponible.
        """
        ml_alerts = [
            a for a in alert_mgr.query(source=source, severity=severity, limit=limit * 3)
            if str(a.get("algorithm", "")).startswith("ml:")
        ][:limit]

        if not ml_alerts:
            return {"explained": 0, "results": []}

        explainer = LLMExplainer()
        results   = []
        for alert in ml_alerts:
            text, backend = explainer.explain_alert_dict(alert)
            results.append({
                "alert_id":    alert.get("id"),
                "source":      alert.get("source"),
                "metric_name": alert.get("metric_name"),
                "severity":    alert.get("severity"),
                "explanation": text,
                "backend":     backend,
            })

        return {
            "explained": len(results),
            "results":   results,
            "ts":        datetime.now(timezone.utc).isoformat(),
        }

    @app.post("/api/v1/alerts/acknowledge-all")
    def acknowledge_all_alerts(
        source:      Optional[str] = Query(None),
        severity:    Optional[str] = Query(None),
        metric_name: Optional[str] = Query(None),
    ):
        n = alert_mgr.acknowledge_all(source=source, severity=severity, metric_name=metric_name)
        return {"acknowledged": n}

    @app.post("/api/v1/alerts/webhook")
    async def alertmanager_webhook(request: Request):
        """
        Receiver webhook pour Prometheus AlertManager.
        AlertManager POST ici quand une alerte fire ou se résout.
        Persiste les alertes reçues dans la table alerts SQLite.
        """
        try:
            payload = await request.json()
        except Exception:
            return {"received": 0}

        alerts_received = payload.get("alerts", [])
        saved = 0
        for a in alerts_received:
            labels      = a.get("labels", {})
            annotations = a.get("annotations", {})
            status      = a.get("status", "firing")
            if status == "resolved":
                continue
            sev_map = {"critical": "critical", "warning": "warning", "info": "info"}
            sev     = sev_map.get(labels.get("severity", "info"), "info")
            from spark.metrics.anomaly_detector import AnomalyAlert
            alert_obj = AnomalyAlert(
                metric_name=labels.get("metric", labels.get("alertname", "unknown")),
                source=labels.get("pipeline", labels.get("source", "alertmanager")),
                algorithm="prometheus",
                severity=sev,
                value=0.0,
                details=annotations.get("description", annotations.get("summary", "")),
            )
            alert_mgr.save([alert_obj])
            saved += 1

        return {
            "received":  len(alerts_received),
            "persisted": saved,
            "ts":        datetime.now(timezone.utc).isoformat(),
        }

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

    print(f"[metrics/api] Démarrage sur http://{args.host}:{args.port}")
    print(f"[metrics/api] Base SQLite    : {args.db}")
    print(f"[metrics/api] Documentation  : http://localhost:{args.port}/docs")

    app = create_app(db_path=args.db)
    _serve_app(app, host=args.host, port=args.port)


def _serve_app(app, host: str, port: int) -> None:
    """Lance l'app ASGI — essaie hypercorn, puis uvicorn (avec patch si besoin)."""
    # ── Hypercorn (premier choix : pas de conflits httptools/websockets) ──────
    try:
        import asyncio
        from hypercorn.config import Config as HConfig
        from hypercorn.asyncio import serve as hserve

        cfg = HConfig()
        cfg.bind = [f"{host}:{port}"]
        asyncio.run(hserve(app, cfg))
        return
    except ImportError:
        pass

    # ── Uvicorn (avec patch a2wsgi + websockets si nécessaire) ───────────────
    try:
        try:
            import a2wsgi as _a2
            if not hasattr(_a2, "WSGIMiddleware"):
                from a2wsgi.wsgi import WSGIMiddleware as _mw
                _a2.WSGIMiddleware = _mw
        except Exception:
            pass
        import uvicorn
        uvicorn.run(app, host=host, port=port)
        return
    except Exception as exc:
        print(f"Erreur uvicorn : {exc}")

    print("Erreur : aucun serveur ASGI disponible.")
    print("Installez hypercorn :  pip install hypercorn")


if __name__ == "__main__":
    main()