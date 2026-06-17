"""
Tests Semaine 14 — Alerting & notifications.

T1  AlertRule.matches — pipeline/metric/severity matching
T2  AlertRule.matches — wildcard "*"
T3  AlertRuleStore — add, list, get, count
T4  AlertRuleStore — get_matching_rules filtre par sévérité
T5  AlertRuleStore — enable/disable
T6  AlertRuleStore — règles par défaut au démarrage
T7  NotificationService — canal console (dry_run)
T8  NotificationService — cooldown bloque la seconde notification
T9  NotificationService — notify_many sur plusieurs règles
T10 IncidentManager — create + get
T11 IncidentManager — update_status avec note
T12 IncidentManager — resolve
T13 IncidentManager — escalate monte la sévérité
T14 IncidentManager — detect_recurring
T15 RunbookEngine — find_runbook par métrique
T16 RunbookEngine — suggest_for_incident
T17 RunbookEngine — execute dry_run retourne les étapes
T18 RunbookEngine — register_recurring crée un runbook custom
T19 Intégration — alerte → incident → runbook
"""
from __future__ import annotations

import os
import sys
import tempfile

import pytest

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, BASE_DIR)

from spark.alerting.models import AlertRule, NotificationChannel, IncidentRecord, RunbookEntry, severity_gte
from spark.alerting.alert_config import AlertRuleStore
from spark.alerting.notifier import NotificationService
from spark.alerting.incident_manager import IncidentManager
from spark.alerting.runbook_engine import RunbookEngine


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def tmp_db(tmp_path):
    return str(tmp_path / "test.db")

@pytest.fixture
def rule_store(tmp_db):
    return AlertRuleStore(db_path=tmp_db, seed_defaults=False)

@pytest.fixture
def notif_svc(tmp_path):
    return NotificationService(
        channels=[NotificationChannel("console")],
        log_db_path=str(tmp_path / "notif_log.db"),
        dry_run=True,
    )

@pytest.fixture
def inc_mgr(tmp_db):
    return IncidentManager(db_path=tmp_db)

@pytest.fixture
def runbook_engine(tmp_path):
    return RunbookEngine(db_path=str(tmp_path / "runbooks.db"))

@pytest.fixture
def sample_alert():
    return {
        "id": 1,
        "source": "clean_sales",
        "metric_name": "job.rejection_rate_pct",
        "severity": "CRITICAL",
        "value": 45.2,
        "details": "Taux de rejet = 45 % (seuil 20 %)",
        "ts": "2026-06-17T10:00:00+00:00",
    }


# ── T1 : AlertRule.matches — critères exacts ──────────────────────────────────

def test_t1_alert_rule_matches_exact():
    rule = AlertRule(
        pipeline_name="clean_sales",
        metric_name="job.rejection_rate_pct",
        severity_min="HIGH",
    )
    assert rule.matches("clean_sales", "job.rejection_rate_pct", "HIGH")
    assert rule.matches("clean_sales", "job.rejection_rate_pct", "CRITICAL")
    assert not rule.matches("clean_sales", "job.rejection_rate_pct", "LOW")
    assert not rule.matches("ingest_sales", "job.rejection_rate_pct", "HIGH")


# ── T2 : AlertRule.matches — wildcard ────────────────────────────────────────

def test_t2_alert_rule_matches_wildcard():
    rule = AlertRule(pipeline_name="*", metric_name="*", severity_min="CRITICAL")
    assert rule.matches("any_pipeline", "any_metric", "CRITICAL")
    assert not rule.matches("any_pipeline", "any_metric", "HIGH")


# ── T3 : AlertRuleStore — CRUD ────────────────────────────────────────────────

def test_t3_alert_rule_store_crud(rule_store):
    rule = AlertRule(
        pipeline_name="ingest_sales",
        metric_name="job.rows_output",
        severity_min="HIGH",
        channels=["console", "teams"],
        recipients=["ops@co.fr"],
    )
    rule_id = rule_store.add_rule(rule)
    assert rule_store.count() == 1

    fetched = rule_store.get(rule_id)
    assert fetched is not None
    assert fetched.pipeline_name == "ingest_sales"
    assert "teams" in fetched.channels
    assert "ops@co.fr" in fetched.recipients

    rules = rule_store.list_rules()
    assert len(rules) == 1

    removed = rule_store.remove_rule(rule_id)
    assert removed
    assert rule_store.count() == 0


# ── T4 : AlertRuleStore — get_matching_rules ──────────────────────────────────

def test_t4_alert_rule_store_matching(rule_store):
    rule_store.add_rule(AlertRule(
        pipeline_name="clean_sales",
        metric_name="*",
        severity_min="HIGH",
        channels=["console"],
    ))
    rule_store.add_rule(AlertRule(
        pipeline_name="*",
        metric_name="*",
        severity_min="CRITICAL",
        channels=["teams"],
    ))
    # CRITICAL → 2 règles s'appliquent
    matches = rule_store.get_matching_rules("clean_sales", "job.rejection_rate_pct", "CRITICAL")
    assert len(matches) == 2
    # HIGH → seulement la règle clean_sales/HIGH
    matches = rule_store.get_matching_rules("clean_sales", "job.rejection_rate_pct", "HIGH")
    assert len(matches) == 1
    # MEDIUM → aucune règle
    matches = rule_store.get_matching_rules("clean_sales", "job.rejection_rate_pct", "MEDIUM")
    assert len(matches) == 0


# ── T5 : AlertRuleStore — enable/disable ──────────────────────────────────────

def test_t5_alert_rule_store_enable_disable(rule_store):
    rule = AlertRule(pipeline_name="*", severity_min="HIGH")
    rid  = rule_store.add_rule(rule)

    rule_store.disable(rid)
    assert rule_store.count(enabled_only=True) == 0
    matches = rule_store.get_matching_rules("x", "y", "CRITICAL")
    assert len(matches) == 0

    rule_store.enable(rid)
    assert rule_store.count(enabled_only=True) == 1


# ── T6 : AlertRuleStore — règles par défaut ───────────────────────────────────

def test_t6_alert_rule_store_defaults(tmp_path):
    store = AlertRuleStore(db_path=str(tmp_path / "alerting.db"), seed_defaults=True)
    assert store.count() == 3  # 3 règles par défaut


# ── T7 : NotificationService — console dry_run ────────────────────────────────

def test_t7_notif_console_dry_run(notif_svc, sample_alert):
    rule = AlertRule(
        pipeline_name="clean_sales",
        severity_min="HIGH",
        channels=["console"],
        cooldown_min=0,
    )
    results = notif_svc.notify(sample_alert, rule)
    assert len(results) == 1
    assert results[0]["status"] == "sent"
    assert results[0]["channel"] == "console"


# ── T8 : NotificationService — cooldown ──────────────────────────────────────

def test_t8_notif_cooldown(notif_svc, sample_alert):
    rule = AlertRule(
        pipeline_name="clean_sales",
        severity_min="HIGH",
        channels=["console"],
        cooldown_min=60,  # 60 minutes de silence
    )
    r1 = notif_svc.notify(sample_alert, rule)
    assert r1[0]["status"] == "sent"

    r2 = notif_svc.notify(sample_alert, rule)
    assert r2[0]["status"] == "cooldown"


# ── T9 : NotificationService — notify_many ───────────────────────────────────

def test_t9_notif_notify_many(notif_svc, sample_alert):
    rules = [
        AlertRule(pipeline_name="clean_sales", severity_min="HIGH",
                  channels=["console"], cooldown_min=0),
        AlertRule(pipeline_name="*",           severity_min="CRITICAL",
                  channels=["console"], cooldown_min=0),
    ]
    results = notif_svc.notify_many(sample_alert, rules)
    assert len(results) == 2
    sent = [r for r in results if r["status"] == "sent"]
    assert len(sent) == 2


# ── T10 : IncidentManager — create + get ──────────────────────────────────────

def test_t10_incident_create_get(inc_mgr):
    inc = inc_mgr.create(IncidentRecord(
        pipeline="clean_sales",
        title="Taux de rejet 45 %",
        description="job.rejection_rate_pct > seuil",
        severity="HIGH",
    ))
    assert inc.id > 0

    fetched = inc_mgr.get(inc.id)
    assert fetched is not None
    assert fetched.pipeline == "clean_sales"
    assert fetched.status == "open"
    assert fetched.severity == "HIGH"


# ── T11 : IncidentManager — update_status avec note ──────────────────────────

def test_t11_incident_update_status(inc_mgr):
    inc = inc_mgr.create(IncidentRecord(
        pipeline="clean_sales",
        title="Rejet élevé",
        severity="HIGH",
    ))
    ok = inc_mgr.update_status(inc.id, "investigating", note="En cours d'investigation")
    assert ok

    fetched = inc_mgr.get(inc.id)
    assert fetched.status == "investigating"
    assert len(fetched.notes) == 1
    assert "investigation" in fetched.notes[0]


# ── T12 : IncidentManager — resolve ──────────────────────────────────────────

def test_t12_incident_resolve(inc_mgr):
    inc = inc_mgr.create(IncidentRecord(
        pipeline="clean_sales",
        title="Test résolution",
        severity="MEDIUM",
    ))
    ok = inc_mgr.resolve(inc.id, note="Corrigé : source upstream restaurée")
    assert ok

    fetched = inc_mgr.get(inc.id)
    assert fetched.status == "resolved"
    assert fetched.resolved_at is not None
    assert "restaurée" in fetched.notes[0]

    assert inc_mgr.count("resolved") == 1
    assert inc_mgr.count("open") == 0


# ── T13 : IncidentManager — escalate ─────────────────────────────────────────

def test_t13_incident_escalate(inc_mgr):
    inc = inc_mgr.create(IncidentRecord(
        pipeline="aggregate_sales",
        title="Durée × 4",
        severity="HIGH",
    ))
    new_sev = inc_mgr.escalate(inc.id)
    assert new_sev == "CRITICAL"

    fetched = inc_mgr.get(inc.id)
    assert fetched.severity == "CRITICAL"
    assert "CRITICAL" in fetched.notes[0]

    # Déjà CRITICAL → reste CRITICAL
    new_sev2 = inc_mgr.escalate(inc.id)
    assert new_sev2 == "CRITICAL"


# ── T14 : IncidentManager — detect_recurring ─────────────────────────────────

def test_t14_incident_detect_recurring(inc_mgr):
    pipeline = "clean_sales"
    for i in range(3):
        inc_mgr.create(IncidentRecord(
            pipeline=pipeline,
            title=f"Incident #{i}",
            severity="HIGH",
        ))
    assert inc_mgr.detect_recurring(pipeline, window_hours=1, threshold=3)
    assert not inc_mgr.detect_recurring(pipeline, window_hours=1, threshold=4)
    assert not inc_mgr.detect_recurring("other_pipeline", window_hours=1, threshold=1)


# ── T15 : RunbookEngine — find_runbook ───────────────────────────────────────

def test_t15_runbook_find_by_metric(runbook_engine):
    rb = runbook_engine.find_runbook("job.rejection_rate_pct")
    assert rb is not None
    assert "rejet" in rb.title.lower() or "rejection" in rb.pattern
    assert len(rb.steps) > 0


# ── T16 : RunbookEngine — suggest_for_incident ───────────────────────────────

def test_t16_runbook_suggest_for_incident(runbook_engine):
    inc = IncidentRecord(
        pipeline="clean_sales",
        title="[CRITICAL] job.null_rate = 42 %",
        description="Taux de nulls anormalement élevé sur la colonne amount",
        severity="CRITICAL",
    )
    rb = runbook_engine.suggest_for_incident(inc)
    assert rb is not None
    assert "null" in rb.pattern or "null" in rb.title.lower()


# ── T17 : RunbookEngine — execute dry_run ────────────────────────────────────

def test_t17_runbook_execute_dry_run(runbook_engine):
    rb = runbook_engine.find_runbook("volume_drop")
    assert rb is not None
    outputs = runbook_engine.execute(rb, dry_run=True)
    assert len(outputs) == len(rb.steps)
    assert all("[STEP]" in o for o in outputs)


# ── T18 : RunbookEngine — register_recurring ─────────────────────────────────

def test_t18_runbook_register_recurring(runbook_engine):
    rb = runbook_engine.register_recurring(
        pipeline="ingest_crm",
        metric="job.rejection_rate_pct",
        steps=["1. Vérifier CRM API", "2. Relancer l'ingestion"],
    )
    assert rb.id > 0
    assert "ingest_crm" in rb.pattern
    assert len(rb.steps) == 2
    assert "recurring" in rb.tags


# ── T19 : Intégration — alerte → incident → runbook ──────────────────────────

def test_t19_integration_alert_incident_runbook(tmp_path, sample_alert):
    rule_store = AlertRuleStore(db_path=str(tmp_path / "alerting.db"), seed_defaults=False)
    rule = AlertRule(
        pipeline_name="clean_sales",
        metric_name="job.rejection_rate_pct",
        severity_min="HIGH",
        channels=["console"],
        cooldown_min=0,
    )
    rule_store.add_rule(rule)

    # 1 — Notification
    svc = NotificationService(
        channels=[NotificationChannel("console")],
        log_db_path=str(tmp_path / "notif_log.db"),
        dry_run=True,
    )
    matching = rule_store.get_matching_rules(
        sample_alert["source"],
        sample_alert["metric_name"],
        sample_alert["severity"],
    )
    assert len(matching) == 1
    results = svc.notify_many(sample_alert, matching)
    assert results[0]["status"] == "sent"

    # 2 — Incident
    mgr = IncidentManager(db_path=str(tmp_path / "incidents.db"))
    inc = mgr.create(IncidentRecord(
        pipeline=sample_alert["source"],
        title=f"[{sample_alert['severity']}] {sample_alert['metric_name']}",
        description=sample_alert["details"],
        severity=sample_alert["severity"],
        alert_ids=[sample_alert["id"]],
    ))
    assert inc.id > 0
    mgr.update_status(inc.id, "investigating", note="Alerte traitée")

    # 3 — Runbook
    engine = RunbookEngine(db_path=str(tmp_path / "runbooks.db"))
    rb = engine.suggest_for_incident(mgr.get(inc.id))
    assert rb is not None
    steps = engine.execute(rb, incident=mgr.get(inc.id), dry_run=True)
    assert len(steps) > 0

    # 4 — Résolution
    mgr.resolve(inc.id, note="Runbook appliqué, source corrigée")
    final = mgr.get(inc.id)
    assert final.status == "resolved"
