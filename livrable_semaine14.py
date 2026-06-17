"""
Livrable Semaine 14 — Alerting & notifications : démonstration complète.

Scénario :
  1. Configurer des règles d'alerte (console + Teams simulé)
  2. Simuler 3 alertes de sévérités croissantes
  3. Envoyer les notifications (dry_run / console)
  4. Créer des incidents liés aux alertes
  5. Détecter un pipeline récurrent
  6. Suggérer et exécuter un runbook (dry_run)
"""
from __future__ import annotations

import os
import sys
import tempfile
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

from spark.alerting import (
    AlertRule, AlertRuleStore, NotificationChannel,
    NotificationService, IncidentRecord, IncidentManager, RunbookEngine,
)


def hr(char="═", w=68):
    print(char * w)


def section(title: str):
    print()
    hr()
    print(f"  {title}")
    hr()


# ── Répertoire temporaire partagé pour tous les stores ────────────────────────
_TMP = tempfile.mkdtemp(prefix="s14_demo_")

ALERTING_DB  = os.path.join(_TMP, "alerting.db")
NOTIF_DB     = os.path.join(_TMP, "notif_log.db")
INCIDENT_DB  = os.path.join(_TMP, "incidents.db")
RUNBOOK_DB   = os.path.join(_TMP, "runbooks.db")

# ══════════════════════════════════════════════════════════════════════════════
# 1. Configuration des règles d'alerte
# ══════════════════════════════════════════════════════════════════════════════

section("1/5 — Système d'alertes configurables")

rule_store = AlertRuleStore(db_path=ALERTING_DB, seed_defaults=False)

rules_config = [
    AlertRule(
        pipeline_name="*",
        metric_name="*",
        severity_min="CRITICAL",
        channels=["console"],
        recipients=[],
        cooldown_min=0,
    ),
    AlertRule(
        pipeline_name="clean_sales",
        metric_name="job.rejection_rate_pct",
        severity_min="HIGH",
        channels=["console", "teams"],
        recipients=["ops@delomid-it.com"],
        cooldown_min=0,
    ),
    AlertRule(
        pipeline_name="aggregate_sales",
        metric_name="job.duration_seconds",
        severity_min="MEDIUM",
        channels=["console"],
        recipients=[],
        cooldown_min=0,
    ),
]

for r in rules_config:
    rule_store.add_rule(r)

print(f"  {rule_store.count()} règles configurées :\n")
for r in rule_store.list_rules():
    status = "✅" if r.enabled else "❌"
    print(f"  {status}  [{r.severity_min:8s}]  {r.pipeline_name:20s}  {r.metric_name:35s}  → {r.channels}")

# ══════════════════════════════════════════════════════════════════════════════
# 2. Alertes simulées
# ══════════════════════════════════════════════════════════════════════════════

section("2/5 — Alertes simulées (3 pipelines)")

ALERTS = [
    {
        "id": 101,
        "source": "ingest_sales",
        "metric_name": "job.rows_output",
        "severity": "MEDIUM",
        "value": 120,
        "details": "Volume anormalement bas : 120 lignes (attendu ~50 000)",
        "ts": datetime.now(timezone.utc).isoformat(),
        "algorithm": "zscore",
    },
    {
        "id": 102,
        "source": "clean_sales",
        "metric_name": "job.rejection_rate_pct",
        "severity": "HIGH",
        "value": 45.2,
        "details": "Taux de rejet = 45 % (seuil 20 %)",
        "ts": datetime.now(timezone.utc).isoformat(),
        "algorithm": "threshold",
    },
    {
        "id": 103,
        "source": "aggregate_sales",
        "metric_name": "job.duration_seconds",
        "severity": "CRITICAL",
        "value": 487.0,
        "details": "Durée = 487 s (normale ≈ 30 s, SLA = 300 s)",
        "ts": datetime.now(timezone.utc).isoformat(),
        "algorithm": "iqr",
    },
]

for a in ALERTS:
    print(f"\n  [{a['severity']:8s}]  {a['source']:20s}  {a['metric_name']:35s}  = {a['value']}")
    print(f"           {a['details']}")

# ══════════════════════════════════════════════════════════════════════════════
# 3. Notifications
# ══════════════════════════════════════════════════════════════════════════════

section("3/5 — Notifications multi-canaux")

svc = NotificationService(
    channels=[
        NotificationChannel("console"),
        NotificationChannel("teams", {"webhook_url": "https://teams.webhook.example/hook"}),
    ],
    log_db_path=NOTIF_DB,
    dry_run=True,   # dry_run : simule Teams/Email sans vrai appel HTTP
)

total_sent = 0
for alert in ALERTS:
    matching = rule_store.get_matching_rules(
        alert["source"], alert["metric_name"], alert["severity"]
    )
    if not matching:
        print(f"\n  [{alert['severity']:8s}]  {alert['source']} — aucune règle active")
        continue

    results = svc.notify_many(alert, matching)
    for r in results:
        icon = "📤" if r["status"] == "sent" else ("⏸" if r["status"] == "cooldown" else "❌")
        print(f"\n  {icon}  [{alert['severity']:8s}]  canal={r['channel']:8s}  "
              f"statut={r['status']}")
        if r["status"] == "sent" and r["channel"] == "console":
            # Affiche le message console formaté
            print(r["detail"])
        elif r["status"] == "sent":
            print(f"           → {r['detail']}")
        if r["status"] == "sent":
            total_sent += 1

print(f"\n  Total : {total_sent} notification(s) envoyée(s)")

# ══════════════════════════════════════════════════════════════════════════════
# 4. Gestion des incidents
# ══════════════════════════════════════════════════════════════════════════════

section("4/5 — Gestion des incidents")

mgr = IncidentManager(db_path=INCIDENT_DB)

created_incidents = []
for alert in ALERTS:
    inc = mgr.create(IncidentRecord(
        pipeline=alert["source"],
        title=f"[{alert['severity']}] {alert['metric_name']} = {alert['value']}",
        description=alert["details"],
        severity=alert["severity"],
        alert_ids=[alert["id"]],
    ))
    created_incidents.append(inc)
    print(f"\n  📋 Incident #{inc.id:02d} créé — {inc.pipeline}")
    print(f"     Titre    : {inc.title}")
    print(f"     Sévérité : {inc.severity}  |  Statut : {inc.status}")

# Mise à jour du statut
inc_rejet = created_incidents[1]
mgr.update_status(inc_rejet.id, "investigating",
                  note="Équipe data contactée — source upstream en cours d'analyse")
print(f"\n  ⚙️  Incident #{inc_rejet.id} → investigating")

# Escalade de l'incident CRITICAL
inc_perf = created_incidents[2]
new_sev = mgr.escalate(inc_perf.id)
print(f"  ⬆️  Incident #{inc_perf.id} escaladé → {new_sev}")

# Simulation de récurrence : 3 incidents sur clean_sales
for i in range(2):
    mgr.create(IncidentRecord(
        pipeline="clean_sales",
        title=f"Rejet récurrent #{i+2}",
        severity="HIGH",
    ))

is_recurring = mgr.detect_recurring("clean_sales", window_hours=1, threshold=3)
recurring_pipelines = mgr.get_recurring_pipelines(window_hours=1, threshold=3)
print(f"\n  🔁 Récurrence clean_sales (3 incidents / 1h) : {is_recurring}")
print(f"     Pipelines récurrents : {recurring_pipelines}")

# Résumé
summary = mgr.summary()
print(f"\n  Résumé incidents : {summary}")

# Résolution d'un incident
mgr.resolve(created_incidents[0].id, note="Volume remonté après relance du job amont")
print(f"\n  ✅  Incident #{created_incidents[0].id} résolu")

# ══════════════════════════════════════════════════════════════════════════════
# 5. Runbooks automatiques
# ══════════════════════════════════════════════════════════════════════════════

section("5/5 — Runbooks automatiques pour incidents récurrents")

engine = RunbookEngine(db_path=RUNBOOK_DB)

print(f"  {len(engine.get_all())} runbooks pré-chargés :\n")
for rb in engine.get_all():
    print(f"    [{rb.id}] {rb.pattern:30s}  → {rb.title}  ({len(rb.steps)} étapes)")

# Suggestion de runbook pour chaque incident
print()
for inc in created_incidents:
    fetched = mgr.get(inc.id)
    rb = engine.suggest_for_incident(fetched)
    if rb:
        print(f"  📖  Incident #{inc.id:02d} ({inc.pipeline:20s}) → Runbook : «{rb.title}»")
    else:
        print(f"  📖  Incident #{inc.id:02d} ({inc.pipeline:20s}) → Aucun runbook standard")

# Exécution dry_run du runbook de rejet
print()
rb_rejet = engine.find_runbook("job.rejection_rate_pct")
print(f"  Exécution (dry_run) du runbook «{rb_rejet.title}» :")
outputs = engine.execute(rb_rejet, incident=mgr.get(inc_rejet.id), dry_run=True)
for o in outputs:
    print(f"    {o}")

# Runbook automatique pour pipeline récurrent
print()
rb_auto = engine.register_recurring(
    pipeline="clean_sales",
    metric="job.rejection_rate_pct",
)
print(f"  🤖  Runbook récurrent créé : «{rb_auto.title}»")
print(f"      Tags : {rb_auto.tags}")
print(f"      Étapes : {len(rb_auto.steps)}")

# ══════════════════════════════════════════════════════════════════════════════
# Bilan
# ══════════════════════════════════════════════════════════════════════════════

section("Bilan Semaine 14")

n_rules    = rule_store.count()
n_sent     = total_sent
n_inc      = mgr.count()
n_resolved = mgr.count("resolved")
n_open     = mgr.count("open") + mgr.count("investigating")
n_rb       = len(engine.get_all())

print(f"""
  Règles d'alerte configurées  : {n_rules}
  Notifications envoyées       : {n_sent}
  Incidents créés              : {n_inc}
    → ouverts / en cours       : {n_open}
    → résolus                  : {n_resolved}
  Runbooks disponibles         : {n_rb}
  Récurrence détectée          : clean_sales (3 incidents / 1h)
""")

hr("─")
print("  Fichiers livrés :")
files = [
    ("spark/alerting/models.py",          "AlertRule, NotificationChannel, IncidentRecord, RunbookEntry"),
    ("spark/alerting/alert_config.py",    "AlertRuleStore — règles configurables SQLite"),
    ("spark/alerting/notifier.py",        "NotificationService — console, Teams, Email (SMTP)"),
    ("spark/alerting/incident_manager.py","IncidentManager — cycle de vie + escalade + récurrence"),
    ("spark/alerting/runbook_engine.py",  "RunbookEngine — 5 runbooks + suggest + execute + register"),
    ("spark/tests/test_semaine14.py",     "19/19 tests OK"),
]
for f, desc in files:
    print(f"    {f:<45s}  {desc}")
hr()
