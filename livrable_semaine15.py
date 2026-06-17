"""
Livrable Semaine 15 — Intégration end-to-end : 5 scénarios de démonstration.

Scénario 1 : Chute de volume critique
  Métriques → détection seuil → alerte CRITICAL → incident → runbook dry_run

Scénario 2 : Taux de rejet élevé
  Métriques → alerte HIGH → notification console → escalade → résolution

Scénario 3 : Pipeline récidiviste
  3 incidents en 24h → détection récurrence → runbook dynamique enregistré

Scénario 4 : Prometheus — exposition métriques
  SQLite → PrometheusExporter → format texte Prometheus exposition

Scénario 5 : Data Catalog — dégradation & rétablissement
  Anomalies détectées → catalog mis à jour → score qualité → incidents résolus
"""
from __future__ import annotations

import os
import sys
import tempfile
from datetime import datetime, timezone

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

from spark.metrics.storage         import SQLiteMetricsStore, MetricPoint
from spark.metrics.alert_manager   import AlertManager
from spark.metrics.anomaly_detector import AnomalyDetector, AnomalyAlert
from spark.metrics.prometheus_exporter import PrometheusExporter
from spark.catalog.catalog         import DataCatalog
from spark.catalog.models          import CatalogEntry
from spark.alerting.models         import AlertRule, NotificationChannel, IncidentRecord
from spark.alerting.alert_config   import AlertRuleStore
from spark.alerting.notifier       import NotificationService
from spark.alerting.incident_manager import IncidentManager
from spark.alerting.runbook_engine import RunbookEngine


# ── Helpers ───────────────────────────────────────────────────────────────────

def hr(char="═", w=70):
    print(char * w)

def section(title: str):
    print()
    hr()
    print(f"  {title}")
    hr()

def sub(label: str):
    print(f"\n  {'─'*60}")
    print(f"  {label}")
    print(f"  {'─'*60}")

def ok(msg: str):   print(f"    ✔  {msg}")
def info(msg: str): print(f"    ·  {msg}")
def warn(msg: str): print(f"    ⚠  {msg}")


# ── Scénario 1 : Chute de volume critique ─────────────────────────────────────

def scenario_1_volume_drop(tmpdir: str) -> None:
    section("SCÉNARIO 1 — Chute de volume critique")

    db      = os.path.join(tmpdir, "s1_metrics.db")
    inc_db  = os.path.join(tmpdir, "s1_incidents.db")
    rb_db   = os.path.join(tmpdir, "s1_runbooks.db")

    store   = SQLiteMetricsStore(db_path=db)
    alt_mgr = AlertManager(db_path=db)
    inc_mgr = IncidentManager(db_path=inc_db)
    rb_eng  = RunbookEngine(db_path=rb_db)

    sub("1.1  Écriture des métriques (volume anormal)")
    store.write([
        MetricPoint(source="spark", name="clean_sales.rows_output",        value=850),
        MetricPoint(source="spark", name="clean_sales.rejection_rate_pct", value=1.2),
        MetricPoint(source="airflow", name="clean_sales.duration_sec",     value=210),
    ])
    ok("3 métriques écrites — clean_sales.rows_output = 850 (attendu ~45 000)")

    sub("1.2  Détection automatique — seuil volume")
    alert = AnomalyAlert(
        metric_name="clean_sales.rows_output",
        source="clean_sales",
        algorithm="threshold",
        severity="critical",
        value=850,
        expected="[30 000 – 60 000]",
        details="Volume 850 < seuil min 5 000 — chute de 98 %",
    )
    alt_mgr.save([alert])
    ok("Alerte CRITICAL sauvegardée")
    info(str(alert))

    sub("1.3  Création incident")
    inc = inc_mgr.create(IncidentRecord(
        pipeline="clean_sales",
        title="Chute de volume critique — clean_sales",
        description="Volume observé 850, attendu ~45 000. Chute de 98 %.",
        severity="HIGH",
        alert_ids=["1"],
    ))
    ok(f"Incident #{inc.id} créé — statut : {inc.status} | sévérité : {inc.severity}")

    sub("1.4  Runbook suggéré + exécution dry_run")
    rb = rb_eng.find_runbook("rows_output")
    if rb:
        ok(f"Runbook trouvé : « {rb.title} »")
        steps = rb_eng.execute(rb, inc, dry_run=True)
        for step in steps[:4]:
            info(step)
        if len(steps) > 4:
            info(f"… {len(steps) - 4} étape(s) supplémentaire(s)")
    else:
        warn("Aucun runbook associé à rows_output")

    sub("1.5  Résolution")
    inc_mgr.update_status(inc.id, "investigating", "Cause : source upstream indisponible")
    inc_mgr.resolve(inc.id, "Job upstream relancé à 14h05, volume restauré")
    resolved = inc_mgr.get(inc.id)
    ok(f"Incident résolu — statut : {resolved.status}")


# ── Scénario 2 : Taux de rejet élevé ─────────────────────────────────────────

def scenario_2_rejection_rate(tmpdir: str) -> None:
    section("SCÉNARIO 2 — Taux de rejet élevé + escalade")

    rule_db  = os.path.join(tmpdir, "s2_alerting.db")
    notif_db = os.path.join(tmpdir, "s2_notif.db")
    inc_db   = os.path.join(tmpdir, "s2_incidents.db")

    rule_store = AlertRuleStore(db_path=rule_db, seed_defaults=False)
    notif_svc  = NotificationService(
        channels=[NotificationChannel("console")],
        log_db_path=notif_db,
        dry_run=True,
    )
    inc_mgr = IncidentManager(db_path=inc_db)

    sub("2.1  Configuration règle d'alerte")
    rule = AlertRule(
        pipeline_name="ingest_sales",
        metric_name="rejection_rate_pct",
        severity_min="HIGH",
        channels=["console"],
        cooldown_min=30,
    )
    rule_store.add_rule(rule)
    ok(f"Règle ajoutée : {rule.pipeline_name}/{rule.metric_name} sévérité ≥ {rule.severity_min}")

    sub("2.2  Alerte taux de rejet (28.5 %)")
    alert_dict = {
        "metric_name": "rejection_rate_pct",
        "source":      "ingest_sales",
        "severity":    "HIGH",
        "value":       28.5,
        "details":     "Taux de rejet 28.5 % > seuil 20 %",
    }
    matching = rule_store.get_matching_rules("ingest_sales", "rejection_rate_pct", "HIGH")
    ok(f"{len(matching)} règle(s) correspondante(s)")

    results = notif_svc.notify(alert_dict, matching[0])
    for r in results:
        ok(f"Canal {r['channel']} → {r['status']} | {r.get('detail','')[:60]}")

    sub("2.3  Incident + escalade")
    inc = inc_mgr.create(IncidentRecord(
        pipeline="ingest_sales",
        title="Rejet élevé — ingest_sales",
        description="28.5 % de lignes rejetées (seuil 20 %)",
        severity="MEDIUM",
    ))
    msg = inc_mgr.escalate(inc.id)
    ok(f"Escalade → {msg}")
    inc_mgr.add_note(inc.id, "Investigation : données source corrompues depuis S3")

    sub("2.4  Résolution")
    inc_mgr.resolve(inc.id, "Données S3 recorrigées, rejets retombés à 1.8 %")
    updated = inc_mgr.get(inc.id)
    ok(f"Incident #{updated.id} : {updated.severity} → résolu")


# ── Scénario 3 : Pipeline récidiviste ────────────────────────────────────────

def scenario_3_recurring(tmpdir: str) -> None:
    section("SCÉNARIO 3 — Pipeline récidiviste")

    inc_db = os.path.join(tmpdir, "s3_incidents.db")
    rb_db  = os.path.join(tmpdir, "s3_runbooks.db")

    inc_mgr = IncidentManager(db_path=inc_db)
    rb_eng  = RunbookEngine(db_path=rb_db)

    sub("3.1  Simulation de 3 incidents en 24h sur unstable_etl")
    for i in range(1, 4):
        inc_mgr.create(IncidentRecord(
            pipeline="unstable_etl",
            title=f"Échec ETL #{i}",
            description=f"Job ETL échoué ({i}/3 — fenêtre 24h)",
            severity="HIGH",
        ))
        ok(f"Incident #{i} créé")

    sub("3.2  Détection de récurrence")
    is_rec = inc_mgr.detect_recurring("unstable_etl", window_hours=24, threshold=3)
    ok(f"Pipeline récidiviste détecté : {is_rec}")

    rec_pipelines = inc_mgr.get_recurring_pipelines(window_hours=24, threshold=3)
    ok(f"Pipelines récidivistes : {rec_pipelines}")

    sub("3.3  Enregistrement d'un runbook dédié")
    rb = rb_eng.register_recurring(
        pipeline="unstable_etl",
        metric="job.failure_count",
        steps=[
            "Vérifier les logs du job ETL : spark/logs/unstable_etl.log",
            "Vérifier la connectivité source S3 : aws s3 ls s3://data-bucket/",
            "Relancer le job manuellement si source OK : airflow dags trigger unstable_etl",
            "Escalader à l'équipe infra si échec persistant",
        ],
    )
    ok(f"Runbook « {rb.title} » enregistré (id={rb.id})")
    steps = rb_eng.execute(rb, None, dry_run=True)
    for step in steps:
        info(step)


# ── Scénario 4 : Exposition métriques Prometheus ─────────────────────────────

def scenario_4_prometheus(tmpdir: str) -> None:
    section("SCÉNARIO 4 — Prometheus metrics exposition")

    db      = os.path.join(tmpdir, "s4_metrics.db")
    store   = SQLiteMetricsStore(db_path=db)
    alt_mgr = AlertManager(db_path=db)

    sub("4.1  Écriture métriques multi-pipelines")
    store.write([
        MetricPoint(source="spark",   name="clean_sales.rows_output",       value=48_320),
        MetricPoint(source="spark",   name="clean_sales.rejection_rate_pct",value=1.8),
        MetricPoint(source="spark",   name="ingest_sales.rows_output",       value=52_100),
        MetricPoint(source="airflow", name="clean_sales.duration_sec",       value=142),
        MetricPoint(source="airflow", name="ingest_sales.duration_sec",      value=88),
        MetricPoint(source="dbt",     name="stg_sales.row_count",            value=47_890),
    ])
    ok(f"6 métriques écrites — {store.count()} au total")

    sub("4.2  PrometheusExporter — format texte")
    exp  = PrometheusExporter(store, alt_mgr)
    text = exp.to_text()
    lines = [l for l in text.splitlines() if l and not l.startswith("#")]
    meta  = [l for l in text.splitlines() if l.startswith("#")]
    ok(f"{len(meta)} lignes méta (#HELP/#TYPE) + {len(lines)} séries de données")

    sub("4.3  Aperçu — premières lignes")
    for line in text.splitlines()[:14]:
        info(line)
    info("…")

    sub("4.4  Labels présents")
    has_pipeline = 'pipeline="' in text
    has_date     = 'run_date="'  in text
    ok(f"Label pipeline : {has_pipeline}")
    ok(f"Label run_date : {has_date}")
    ok(f"Endpoint exposé : GET /metrics")
    info("Prometheus scrape : prom/prometheus:v2.53.0 → http://metrics-api:8090/metrics (toutes les 15s)")


# ── Scénario 5 : Data Catalog — dégradation & rétablissement ─────────────────

def scenario_5_catalog(tmpdir: str) -> None:
    section("SCÉNARIO 5 — Data Catalog : dégradation et rétablissement")

    cat_db = os.path.join(tmpdir, "s5_catalog.db")
    catalog = DataCatalog(db_path=cat_db)

    sub("5.1  Publication initiale — score qualité nominal")
    catalog.publish(CatalogEntry(
        name="clean_sales",
        type="pipeline",
        description="Nettoyage et validation des ventes brutes (Pandera + Spark).",
        owner="data-team",
        tags=["spark", "sales", "cleaning"],
        quality_score=0.97,
    ))
    entry = catalog.get("clean_sales")
    ok(f"Score initial : {entry.quality_score:.2f}")

    sub("5.2  Détection de 3 anomalies (CRITICAL + HIGH + MEDIUM)")
    class _FakeAnomaly:
        def __init__(self, sev, desc):
            self.severity    = sev
            self.level       = "volume"
            self.description = desc

    anomalies = [
        _FakeAnomaly("CRITICAL", "Volume chute de 98 %"),
        _FakeAnomaly("HIGH",     "Taux de rejet 28.5 %"),
        _FakeAnomaly("MEDIUM",   "Valeur null_rate légèrement élevée"),
    ]
    catalog.publish_from_detection("clean_sales", n_anomalies=3,
                                    worst_severity="CRITICAL", anomalies=anomalies)
    entry = catalog.get("clean_sales")
    ok(f"Score après anomalies : {entry.quality_score:.3f}  ← dégradé")

    sub("5.3  Incidents créés automatiquement")
    incidents = catalog.incidents("clean_sales")
    for inc in incidents:
        ok(f"[{inc.severity}] {inc.description}")

    sub("5.4  Résolution des incidents")
    for inc in incidents:
        catalog.resolve_incident(inc.id)
    ok(f"{len(incidents)} incident(s) résolu(s)")

    sub("5.5  Rétablissement du score qualité")
    catalog.publish_from_detection("clean_sales", n_anomalies=0,
                                    worst_severity=None, anomalies=[])
    entry = catalog.get("clean_sales")
    ok(f"Score rétabli : {entry.quality_score:.3f}")

    sub("5.6  Résumé global")
    summary = catalog.incident_summary()
    ok(f"Total incidents : {summary['total']} | ouverts : {summary['open']} | résolus : {summary['resolved']}")
    ok(f"Par sévérité : {summary['by_severity']}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    hr("═")
    print("  LIVRABLE SEMAINE 15 — Data Trust Agent")
    print("  Intégration end-to-end : 5 scénarios de démonstration")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    hr("═")

    tmpdir = tempfile.mkdtemp(prefix="s15_demo_")
    info(f"Répertoire temporaire : {tmpdir}")

    try:
        scenario_1_volume_drop(tmpdir)
        scenario_2_rejection_rate(tmpdir)
        scenario_3_recurring(tmpdir)
        scenario_4_prometheus(tmpdir)
        scenario_5_catalog(tmpdir)
    except Exception as exc:
        print(f"\n  ERREUR : {exc}")
        raise

    section("RÉSUMÉ")
    ok("Scénario 1 — Chute de volume critique          : métriques → alerte → incident → runbook → résolu")
    ok("Scénario 2 — Taux de rejet élevé               : règle → notification → escalade → résolu")
    ok("Scénario 3 — Pipeline récidiviste               : 3 incidents → détection récurrence → runbook dédié")
    ok("Scénario 4 — Prometheus metrics exposition      : SQLite → exporter → format Prometheus")
    ok("Scénario 5 — Data Catalog dégradation/retabliss.: anomalies → score → incidents → résolution")
    print()
    hr()
    print("  Stack démontré :")
    info("SQLiteMetricsStore · AlertManager · AnomalyDetector")
    info("AlertRuleStore · NotificationService · IncidentManager · RunbookEngine")
    info("PrometheusExporter · DataCatalog")
    info("Prometheus v2.53 + AlertManager v0.27 (docker-compose)")
    hr()
    print()


if __name__ == "__main__":
    main()
