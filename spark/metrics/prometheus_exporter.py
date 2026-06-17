"""
PrometheusExporter — Exposition des métriques SQLite au format Prometheus.

Expose un endpoint GET /metrics (texte Prometheus exposition format) à brancher
sur l'API FastAPI existante. Prometheus scrape cet endpoint et évalue les règles
d'alerte ; AlertManager reçoit les alertes et les route vers les receivers.

Architecture :
    SQLite (metrics.db)
        └── PrometheusExporter.collect()
                └── Gauges prometheus_client
                        └── GET /metrics  ←  Prometheus scrape
                                                └── AlertManager  →  Teams / Email

Métriques exposées (labels: pipeline, run_date) :
    data_trust_job_rows_output          — lignes produites
    data_trust_job_rows_rejected        — lignes rejetées
    data_trust_job_rejection_rate_pct   — taux de rejet %
    data_trust_job_duration_seconds     — durée d'exécution
    data_trust_job_null_rate_pct        — taux global de nulls %
    data_trust_job_duplicate_rate_pct   — taux de doublons %
    data_trust_job_anomalies_injected   — anomalies injectées (tests)
    data_trust_alerts_total             — total alertes (label: severity)
    data_trust_alerts_unacknowledged    — alertes non acquittées

Usage autonome :
    python spark/metrics/prometheus_exporter.py --port 8091

Intégration FastAPI (dans api.py) :
    from spark.metrics.prometheus_exporter import PrometheusExporter, add_metrics_route
    exporter = PrometheusExporter(store, alert_mgr)
    add_metrics_route(app, exporter)
"""
from __future__ import annotations

import argparse
import os
import sys
import threading
import time
from typing import Optional

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

try:
    import prometheus_client as prom
    from prometheus_client import (
        CollectorRegistry, Gauge, Counter, generate_latest, CONTENT_TYPE_LATEST,
    )
    _PROM_OK = True
except ImportError:
    _PROM_OK = False

try:
    from spark.metrics.storage      import SQLiteMetricsStore, DEFAULT_DB
    from spark.metrics.alert_manager import AlertManager
except ImportError:
    from .storage       import SQLiteMetricsStore, DEFAULT_DB
    from .alert_manager import AlertManager


# ── Noms de métriques Prometheus ──────────────────────────────────────────────

_METRIC_MAP = {
    "job.rows_output":          ("data_trust_job_rows_output",         "Nombre de lignes produites par job"),
    "job.rows_rejected":        ("data_trust_job_rows_rejected",        "Nombre de lignes rejetées"),
    "job.rejection_rate_pct":   ("data_trust_job_rejection_rate_pct",  "Taux de rejet (%)"),
    "job.duration_seconds":     ("data_trust_job_duration_seconds",    "Durée d'exécution du job (s)"),
    "job.null_rate_pct":        ("data_trust_job_null_rate_pct",       "Taux de valeurs nulles global (%)"),
    "job.duplicate_rate_pct":   ("data_trust_job_duplicate_rate_pct",  "Taux de doublons (%)"),
    "job.anomalies_injected":   ("data_trust_job_anomalies_injected",  "Nombre d'anomalies injectées"),
}


class PrometheusExporter:
    """
    Lit les dernières métriques depuis SQLite et les expose au format Prometheus.

    Paramètres
    ----------
    store     : SQLiteMetricsStore à lire
    alert_mgr : AlertManager pour les métriques d'alertes (optionnel)
    registry  : CollectorRegistry Prometheus (None = registre global)
    """

    def __init__(
        self,
        store:     SQLiteMetricsStore,
        alert_mgr: Optional[AlertManager] = None,
        registry:  Optional["CollectorRegistry"] = None,  # type: ignore[name-defined]
    ) -> None:
        if not _PROM_OK:
            raise ImportError(
                "prometheus_client non installé. Exécutez : pip install prometheus_client"
            )
        self.store     = store
        self.alert_mgr = alert_mgr
        self.registry  = registry or CollectorRegistry()
        self._gauges: dict = {}
        self._alert_total     = Gauge(
            "data_trust_alerts_total",
            "Nombre total d'alertes",
            ["severity"],
            registry=self.registry,
        )
        self._alert_unack = Gauge(
            "data_trust_alerts_unacknowledged",
            "Alertes non acquittées",
            registry=self.registry,
        )
        self._scrape_duration = Gauge(
            "data_trust_scrape_duration_seconds",
            "Durée de la collecte Prometheus (s)",
            registry=self.registry,
        )
        self._build_gauges()

    # ── API publique ──────────────────────────────────────────────────────────

    def collect(self) -> None:
        """Met à jour toutes les Gauges depuis SQLite (appelé à chaque scrape)."""
        t0 = time.perf_counter()
        self._collect_metrics()
        self._collect_alerts()
        self._scrape_duration.set(round(time.perf_counter() - t0, 4))

    def to_text(self) -> str:
        """Retourne la réponse /metrics au format texte Prometheus."""
        self.collect()
        return generate_latest(self.registry).decode("utf-8")

    def content_type(self) -> str:
        return CONTENT_TYPE_LATEST

    # ── Intégration FastAPI ───────────────────────────────────────────────────

    def make_fastapi_route(self):
        """Retourne une fonction de route FastAPI pour GET /metrics."""
        try:
            from fastapi.responses import Response
        except ImportError:
            raise ImportError("FastAPI requis : pip install fastapi")

        exporter = self

        def metrics_endpoint():
            return Response(
                content=exporter.to_text(),
                media_type=exporter.content_type(),
            )
        return metrics_endpoint

    # ── Collecte ──────────────────────────────────────────────────────────────

    def _build_gauges(self) -> None:
        for metric_suffix, (prom_name, doc) in _METRIC_MAP.items():
            self._gauges[metric_suffix] = Gauge(
                prom_name, doc,
                ["pipeline", "run_date"],
                registry=self.registry,
            )

    def _collect_metrics(self) -> None:
        summary = self.store.summary()
        for row in summary:
            name   = row.get("name", "")
            value  = row.get("last_value")
            tags   = row.get("tags") or {}
            if value is None:
                continue
            pipeline = tags.get("job", tags.get("pipeline", "unknown"))
            run_date = tags.get("run_date", "unknown")

            # Métriques connues
            for suffix, gauge in self._gauges.items():
                if name == f"job.{suffix}" or name.endswith(suffix) or name == suffix:
                    gauge.labels(pipeline=pipeline, run_date=run_date).set(float(value))
                    break

            # Métriques dynamiques (ex: job.amount.mean, job.order_id.null_rate)
            if "." in name.removeprefix("job."):
                safe_name = (
                    "data_trust_" +
                    name.replace(".", "_").replace("-", "_")
                )
                if safe_name not in self._gauges:
                    parts = name.split(".")
                    doc = f"Métrique dynamique {name}"
                    self._gauges[safe_name] = Gauge(
                        safe_name, doc,
                        ["pipeline", "run_date"],
                        registry=self.registry,
                    )
                self._gauges[safe_name].labels(
                    pipeline=pipeline, run_date=run_date
                ).set(float(value))

    def _collect_alerts(self) -> None:
        if not self.alert_mgr:
            return
        summary = self.alert_mgr.summary()
        for entry in summary.get("by_severity", []):
            sev   = entry.get("severity", "unknown")
            count = entry.get("count", 0)
            self._alert_total.labels(severity=sev).set(count)
        self._alert_unack.set(summary.get("unacknowledged", 0))


# ── Helper pour api.py ────────────────────────────────────────────────────────

def add_metrics_route(app, exporter: PrometheusExporter) -> None:
    """Ajoute GET /metrics à une app FastAPI existante."""
    app.add_api_route(
        "/metrics",
        exporter.make_fastapi_route(),
        methods=["GET"],
        summary="Prometheus metrics",
        tags=["prometheus"],
        include_in_schema=True,
    )


# ── Serveur autonome ──────────────────────────────────────────────────────────

def _serve_standalone(port: int, db_path: str, interval: int) -> None:
    """Lance un mini serveur HTTP qui expose /metrics en autonome."""
    from http.server import BaseHTTPRequestHandler, HTTPServer

    store     = SQLiteMetricsStore(db_path=db_path)
    alert_mgr = AlertManager(db_path=db_path)
    exporter  = PrometheusExporter(store, alert_mgr)

    print(f"[prometheus-exporter] Démarrage sur http://0.0.0.0:{port}/metrics")
    print(f"[prometheus-exporter] Source SQLite : {db_path}")
    print(f"[prometheus-exporter] Intervalle de rafraîchissement : {interval}s")

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/metrics":
                body = exporter.to_text().encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", exporter.content_type())
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, fmt, *args):  # silence default access log
            pass

    httpd = HTTPServer(("0.0.0.0", port), Handler)
    httpd.serve_forever()


def main():
    parser = argparse.ArgumentParser(description="Prometheus exporter pour Data Trust Agent")
    parser.add_argument("--port",     type=int, default=8091, help="Port HTTP (défaut: 8091)")
    parser.add_argument("--db",       default=DEFAULT_DB,     help="Chemin metrics.db")
    parser.add_argument("--interval", type=int, default=15,   help="Intervalle scrape conseillé (info)")
    args = parser.parse_args()

    if not _PROM_OK:
        print("Erreur : pip install prometheus_client")
        sys.exit(1)

    _serve_standalone(args.port, args.db, args.interval)


if __name__ == "__main__":
    main()
