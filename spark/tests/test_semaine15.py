"""
Tests Semaine 15 — Intégration end-to-end : stack complet Data Trust Agent.

T1  : metrics_store_write_query       — écriture + requête SQLite
T2  : alert_manager_save_query        — sauvegarde alertes + filtres
T3  : prometheus_exporter_empty       — export Prometheus sur DB vide
T4  : prometheus_exporter_with_data   — métriques correctes après écriture
T5  : webhook_receiver_fires          — POST /alerts/webhook persist alertes
T6  : webhook_receiver_skips_resolved — status=resolved ignoré
T7  : catalog_publish_from_detection  — anomalies → catalog + incidents
T8  : catalog_quality_score_decay     — score décroît avec sévérité
T9  : alerting_rule_match_chain       — règles → notification → cooldown
T10 : incident_escalation_chain       — alerte → incident → escalade → note
T11 : runbook_suggest_and_execute     — incident → runbook → dry_run
T12 : detect_recurring_threshold      — 3 incidents en fenêtre → récurrent
T13 : full_pipeline_e2e               — métriques → détection → alerte → incident → runbook
T14 : full_catalog_e2e                — métriques → détection → catalog → score → incident
T15 : api_health_endpoint             — GET /api/v1/health renvoie 200 + count
T16 : api_push_and_query              — POST /push + GET /metrics
T17 : api_webhook_endpoint            — POST /alerts/webhook via TestClient
T18 : api_detect_endpoint             — POST /alerts/detect crée des alertes
T19 : prometheus_labels               — labels pipeline + run_date présents
T20 : alert_rule_store_seed           — règles par défaut + get_matching_rules
"""
from __future__ import annotations

import os
import sys
import tempfile
from datetime import datetime, timezone, timedelta
from typing import List

import pytest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from fastapi.testclient import TestClient

from spark.metrics.storage import SQLiteMetricsStore, MetricPoint
from spark.metrics.alert_manager import AlertManager
from spark.metrics.anomaly_detector import AnomalyDetector, AnomalyAlert
from spark.metrics.prometheus_exporter import PrometheusExporter
from spark.metrics.api import create_app
from spark.catalog.catalog import DataCatalog
from spark.catalog.models import CatalogEntry, Incident
from spark.alerting.models import AlertRule, NotificationChannel, IncidentRecord
from spark.alerting.alert_config import AlertRuleStore
from spark.alerting.notifier import NotificationService
from spark.alerting.incident_manager import IncidentManager
from spark.alerting.runbook_engine import RunbookEngine


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def tmp_db(tmp_path):
    return str(tmp_path / "metrics.db")

@pytest.fixture
def store(tmp_db):
    return SQLiteMetricsStore(db_path=tmp_db)

@pytest.fixture
def alert_mgr(tmp_db):
    return AlertManager(db_path=tmp_db)

@pytest.fixture
def store_with_data(store):
    store.write([
        MetricPoint(source="spark", name="clean_sales.rows_output",       value=45_000),
        MetricPoint(source="spark", name="clean_sales.rejection_rate_pct", value=2.5),
        MetricPoint(source="spark", name="clean_sales.null_rate_pct",      value=1.2),
        MetricPoint(source="spark", name="ingest_sales.rows_output",       value=30_000),
        MetricPoint(source="spark", name="ingest_sales.rejection_rate_pct",value=25.0),
        MetricPoint(source="airflow", name="clean_sales.duration_sec",     value=180),
        MetricPoint(source="dbt",   name="stg_sales.row_count",            value=28_000),
    ])
    return store

@pytest.fixture
def api_client(tmp_db):
    app = create_app(db_path=tmp_db)
    return TestClient(app)

@pytest.fixture
def rule_store(tmp_path):
    return AlertRuleStore(db_path=str(tmp_path / "alerting.db"), seed_defaults=False)

@pytest.fixture
def notif_svc(tmp_path):
    return NotificationService(
        channels=[NotificationChannel("console")],
        log_db_path=str(tmp_path / "notif.db"),
        dry_run=True,
    )

@pytest.fixture
def inc_mgr(tmp_path):
    return IncidentManager(db_path=str(tmp_path / "incidents.db"))

@pytest.fixture
def runbook(tmp_path):
    return RunbookEngine(db_path=str(tmp_path / "runbooks.db"))

@pytest.fixture
def catalog(tmp_path):
    return DataCatalog(db_path=str(tmp_path / "catalog.db"))


# ── T1 : metrics store ────────────────────────────────────────────────────────

def test_metrics_store_write_query(store):
    pts = [
        MetricPoint(source="spark", name="job.rows_output", value=1000),
        MetricPoint(source="airflow", name="job.duration_sec", value=42),
    ]
    n = store.write(pts)
    assert n == 2
    rows = store.query(source="spark")
    assert len(rows) == 1
    assert rows[0]["name"] == "job.rows_output"


# ── T2 : alert manager ────────────────────────────────────────────────────────

def test_alert_manager_save_query(alert_mgr):
    alerts = [
        AnomalyAlert(metric_name="job.rows_output", source="clean_sales",
                     algorithm="zscore", severity="critical", value=0, details="volume drop"),
        AnomalyAlert(metric_name="job.rejection_rate_pct", source="ingest_sales",
                     algorithm="iqr", severity="warning", value=25.0, details="high rejection"),
    ]
    n = alert_mgr.save(alerts)
    assert n == 2
    rows = alert_mgr.query(severity="critical")
    assert len(rows) == 1
    assert rows[0]["source"] == "clean_sales"


# ── T3 : prometheus exporter empty ───────────────────────────────────────────

def test_prometheus_exporter_empty(store, alert_mgr):
    exp = PrometheusExporter(store, alert_mgr)
    text = exp.to_text()
    assert "# TYPE" in text
    assert "data_trust_alerts_total" in text


# ── T4 : prometheus exporter with data ───────────────────────────────────────

def test_prometheus_exporter_with_data(store_with_data, alert_mgr):
    exp = PrometheusExporter(store_with_data, alert_mgr)
    text = exp.to_text()
    assert "data_trust_job_rows_output" in text or "data_trust_" in text
    assert 'pipeline="clean_sales"' in text or "clean_sales" in text


# ── T5 : webhook receiver — firing ───────────────────────────────────────────

def test_webhook_receiver_fires(api_client):
    payload = {
        "alerts": [
            {
                "labels":      {"alertname": "RejectionRateCritical", "severity": "critical",
                                 "pipeline": "clean_sales", "metric": "rejection_rate_pct"},
                "annotations": {"summary": "Rejection rate > 20%", "description": "rate=25.3%"},
                "status":      "firing",
            }
        ]
    }
    resp = api_client.post("/api/v1/alerts/webhook", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data["received"] == 1
    assert data["persisted"] == 1


# ── T6 : webhook receiver — resolved skipped ─────────────────────────────────

def test_webhook_receiver_skips_resolved(api_client):
    payload = {
        "alerts": [
            {
                "labels":  {"alertname": "TestAlert", "severity": "warning"},
                "status":  "resolved",
            }
        ]
    }
    resp = api_client.post("/api/v1/alerts/webhook", json=payload)
    assert resp.status_code == 200
    assert resp.json()["persisted"] == 0


# ── T7 : catalog publish from detection ──────────────────────────────────────

def test_catalog_publish_from_detection(catalog):
    from spark.metrics.anomaly_detection.models import AnomalyLevel

    class _FakeAnomaly:
        def __init__(self, sev):
            self.severity    = sev
            self.level       = AnomalyLevel.VOLUME
            self.description = f"Anomalie {sev}"

    anomalies = [_FakeAnomaly("CRITICAL"), _FakeAnomaly("HIGH"), _FakeAnomaly("MEDIUM")]
    catalog.publish_from_detection(
        pipeline_name="clean_sales",
        n_anomalies=3,
        worst_severity="CRITICAL",
        anomalies=anomalies,
    )
    entry = catalog.get("clean_sales")
    assert entry is not None
    assert entry.quality_score < 0.5
    incidents = catalog.incidents("clean_sales")
    assert len(incidents) == 2  # seul CRITICAL et HIGH sont enregistrés


# ── T8 : catalog quality score decay ─────────────────────────────────────────

def test_catalog_quality_score_decay(catalog):
    catalog.publish(CatalogEntry(name="p1", quality_score=1.0))
    catalog.publish_from_detection("p1", n_anomalies=5, worst_severity="HIGH", anomalies=[])  # no anomaly objects needed for score-only
    entry = catalog.get("p1")
    assert entry.quality_score < 0.8


# ── T9 : alerting rule match chain ───────────────────────────────────────────

def test_alerting_rule_match_chain(rule_store, notif_svc):
    rule = AlertRule(
        pipeline_name="clean_sales",
        metric_name="rejection_rate_pct",
        severity_min="HIGH",
        channels=["console"],
        cooldown_min=60,
    )
    rule_store.add_rule(rule)

    alert = {
        "metric_name": "rejection_rate_pct",
        "source":      "clean_sales",
        "severity":    "critical",
        "value":       25.0,
        "details":     "taux de rejet élevé",
    }
    matching = rule_store.get_matching_rules("clean_sales", "rejection_rate_pct", "critical")
    assert len(matching) == 1

    results = notif_svc.notify(alert, matching[0])
    assert any(r["status"] == "sent" for r in results)

    # 2e envoi → cooldown
    results2 = notif_svc.notify(alert, matching[0])
    assert any(r["status"] == "cooldown" for r in results2)


# ── T10 : incident escalation chain ──────────────────────────────────────────

def test_incident_escalation_chain(inc_mgr):
    inc = IncidentRecord(
        pipeline="clean_sales",
        title="Volume drop critique",
        description="45k → 3k lignes",
        severity="HIGH",
    )
    saved = inc_mgr.create(inc)
    assert saved.id is not None

    inc_mgr.update_status(saved.id, "investigating", "Équipe data alertée")
    inc_mgr.add_note(saved.id, "Cause identifiée : job upstream en erreur")

    msg = inc_mgr.escalate(saved.id)
    updated = inc_mgr.get(saved.id)
    assert updated.severity == "CRITICAL"
    assert "CRITICAL" in msg

    inc_mgr.resolve(saved.id, "Job upstream relancé, volume restauré")
    resolved = inc_mgr.get(saved.id)
    assert resolved.status == "resolved"
    assert resolved.is_open is False


# ── T11 : runbook suggest and execute ────────────────────────────────────────

def test_runbook_suggest_and_execute(runbook):
    rb = runbook.find_runbook("job.rejection_rate_pct")
    assert rb is not None
    assert "rejection" in rb.pattern.lower()

    steps = runbook.execute(rb, incident=None, dry_run=True)
    assert len(steps) > 0
    assert all(s.startswith("[STEP]") or s.startswith("[EXEC]") for s in steps)


# ── T12 : detect recurring threshold ─────────────────────────────────────────

def test_detect_recurring_threshold(inc_mgr):
    for i in range(3):
        inc_mgr.create(IncidentRecord(
            pipeline="unstable_pipeline",
            title=f"Incident #{i+1}",
            description="répétition",
            severity="HIGH",
        ))
    is_recurring = inc_mgr.detect_recurring("unstable_pipeline", window_hours=24, threshold=3)
    assert is_recurring is True

    not_recurring = inc_mgr.detect_recurring("clean_pipeline", window_hours=24, threshold=3)
    assert not_recurring is False


# ── T13 : full pipeline end-to-end ───────────────────────────────────────────

def test_full_pipeline_e2e(tmp_path):
    """Métriques → détection → alerte → incident → runbook."""
    db = str(tmp_path / "e2e.db")
    store    = SQLiteMetricsStore(db_path=db)
    alt_mgr  = AlertManager(db_path=db)
    inc_mgr  = IncidentManager(db_path=str(tmp_path / "inc.db"))
    rb_eng   = RunbookEngine(db_path=str(tmp_path / "rb.db"))

    # 1. Écriture métriques
    store.write([
        MetricPoint(source="spark", name="clean_sales.rows_output",       value=100),
        MetricPoint(source="spark", name="clean_sales.rejection_rate_pct", value=30.0),
    ])

    # 2. Simulation alerte (bypass détecteur pour contrôle de test)
    alert = AnomalyAlert(
        metric_name="clean_sales.rejection_rate_pct",
        source="clean_sales",
        algorithm="threshold",
        severity="critical",
        value=30.0,
        details="Taux de rejet 30% > seuil 20%",
    )
    alt_mgr.save([alert])

    # 3. Créer incident depuis alerte
    saved_alerts = alt_mgr.query(severity="critical")
    assert len(saved_alerts) == 1
    inc = IncidentRecord(
        pipeline=saved_alerts[0]["source"],
        title="Rejet élevé — clean_sales",
        description=saved_alerts[0]["details"],
        severity="HIGH",
        alert_ids=[str(saved_alerts[0]["id"])],
    )
    saved_inc = inc_mgr.create(inc)

    # 4. Runbook suggéré + exécution dry_run
    rb = rb_eng.find_runbook("rejection_rate_pct")
    assert rb is not None
    steps = rb_eng.execute(rb, saved_inc, dry_run=True)
    assert len(steps) > 0

    # 5. Résolution
    inc_mgr.resolve(saved_inc.id, "Cause identifiée et corrigée")
    resolved = inc_mgr.get(saved_inc.id)
    assert resolved.status == "resolved"


# ── T14 : full catalog end-to-end ────────────────────────────────────────────

def test_full_catalog_e2e(tmp_path):
    """Métriques → détection → catalog → score → incident."""
    catalog  = DataCatalog(db_path=str(tmp_path / "catalog.db"))

    # Publication initiale avec bon score
    catalog.publish(CatalogEntry(
        name="clean_sales", type="pipeline",
        description="Nettoyage ventes brutes",
        owner="data-team", tags=["spark", "sales"],
        quality_score=0.95,
    ))

    # Dégradation simulée après détection de 2 anomalies CRITICAL
    class _A:
        def __init__(self, sev):
            self.severity    = sev
            self.level       = "volume"
            self.description = "anomalie " + sev
    catalog.publish_from_detection("clean_sales", 2, "CRITICAL", [_A("CRITICAL"), _A("HIGH")])

    entry = catalog.get("clean_sales")
    assert entry.quality_score < 0.55
    assert len(catalog.incidents("clean_sales")) == 2

    # Résolution d'un incident
    incidents = catalog.incidents("clean_sales")
    resolved = catalog.resolve_incident(incidents[0].id)
    assert resolved is True

    summary = catalog.incident_summary()
    assert summary["total"] == 2
    assert summary["resolved"] == 1


# ── T15 : API health ─────────────────────────────────────────────────────────

def test_api_health_endpoint(api_client):
    resp = api_client.get("/api/v1/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert "total_metrics" in data


# ── T16 : API push and query ─────────────────────────────────────────────────

def test_api_push_and_query(api_client):
    payload = [
        {"source": "spark",   "name": "clean_sales.rows_output",        "value": 48_000},
        {"source": "airflow", "name": "clean_sales.duration_sec",        "value": 120},
        {"source": "dbt",     "name": "stg_sales.row_count",             "value": 45_000},
    ]
    resp = api_client.post("/api/v1/metrics/push", json=payload)
    assert resp.status_code == 201
    assert resp.json()["inserted"] == 3

    resp2 = api_client.get("/api/v1/metrics?source=spark")
    assert resp2.status_code == 200
    assert resp2.json()["count"] == 1

    resp3 = api_client.get("/api/v1/metrics/summary")
    assert resp3.json()["count"] == 3


# ── T17 : API webhook endpoint ───────────────────────────────────────────────

def test_api_webhook_endpoint(api_client):
    payload = {
        "alerts": [
            {
                "labels":      {"alertname": "VolumeDrop", "severity": "critical",
                                 "pipeline": "ingest_sales"},
                "annotations": {"description": "volume < 1000 (attendu ~45000)"},
                "status":      "firing",
            },
            {
                "labels":  {"alertname": "VolumeDropResolved", "severity": "critical",
                             "pipeline": "ingest_sales"},
                "status":  "resolved",
            },
        ]
    }
    resp = api_client.post("/api/v1/alerts/webhook", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data["received"] == 2
    assert data["persisted"] == 1  # resolved ignoré


# ── T18 : API detect endpoint ────────────────────────────────────────────────

def test_api_detect_endpoint(api_client):
    # Injecter des métriques d'abord
    api_client.post("/api/v1/metrics/push", json=[
        {"source": "spark", "name": "clean_sales.rows_output", "value": 50_000},
        {"source": "spark", "name": "ingest_sales.rows_output", "value": 1},
    ])
    resp = api_client.post("/api/v1/alerts/detect")
    assert resp.status_code == 201
    data = resp.json()
    assert "detected" in data
    assert "saved" in data


# ── T19 : prometheus labels ───────────────────────────────────────────────────

def test_prometheus_labels(store_with_data, alert_mgr):
    exp  = PrometheusExporter(store_with_data, alert_mgr)
    text = exp.to_text()
    assert 'pipeline="' in text
    assert 'run_date="' in text


# ── T20 : alert rule store seed ──────────────────────────────────────────────

def test_alert_rule_store_seed(tmp_path):
    rs = AlertRuleStore(db_path=str(tmp_path / "alerting.db"), seed_defaults=True)
    assert rs.count() >= 3

    matching = rs.get_matching_rules("any_pipeline", "job.rejection_rate_pct", "HIGH")
    assert len(matching) >= 1
