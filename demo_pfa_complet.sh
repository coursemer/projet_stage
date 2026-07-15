#!/usr/bin/env bash
# Data Trust Agent — Démonstration complète PFA
# Usage : bash demo_pfa_complet.sh
set -uo pipefail

PYTHON=".venv/bin/python"
STREAMLIT=".venv/bin/streamlit"
LOG_DIR="/tmp/data_trust_pfa"
mkdir -p "$LOG_DIR"

hr()   { printf '\n%s\n' "$(printf '═%.0s' {1..68})"; }
ok()   { echo "  ✔  $*"; }
info() { echo "  ·  $*"; }
step() { hr; echo "  $*"; hr; }

wait_http() {
    local url=$1 label=$2 max=${3:-120} n=0
    printf "  ·  attente %-30s" "$label"
    until curl -sf "$url" > /dev/null 2>&1; do
        sleep 3; ((n+=3))
        (( n > max )) && { echo " TIMEOUT"; return 1; }
        printf "."
    done
    echo " OK (${n}s)"
}

[[ -f "$PYTHON" ]] || { echo "ERREUR : .venv introuvable"; exit 1; }

# ─────────────────────────────────────────────────────────────────────────────
step "1 / STACK DOCKER — Metrics API · Prometheus · AlertManager · InfluxDB"
# ─────────────────────────────────────────────────────────────────────────────

docker compose up -d prometheus alertmanager influxdb metrics-api \
    > "$LOG_DIR/docker.log" 2>&1
ok "docker compose up -d"

wait_http "http://localhost:8090/api/v1/health" "metrics-api:8090"  180
wait_http "http://localhost:9090/-/ready"        "prometheus:9090"    60
wait_http "http://localhost:9093/-/ready"        "alertmanager:9093"  60

"$PYTHON" - <<'EOF'
import urllib.request, json
h = json.loads(urllib.request.urlopen("http://localhost:8090/api/v1/health",  timeout=5).read())
a = json.loads(urllib.request.urlopen("http://localhost:8090/api/v1/alerts/summary", timeout=5).read())
print(f"  ·  métriques en base : {h.get('total_metrics','?')}")
print(f"  ·  alertes en base   : {a.get('total','?')}  (unack={a.get('unacknowledged','?')})")
EOF

# ─────────────────────────────────────────────────────────────────────────────
step "2 / DASHBOARD STREAMLIT"
# ─────────────────────────────────────────────────────────────────────────────

lsof -ti:8501 | xargs kill -9 2>/dev/null || true
sleep 1
"$STREAMLIT" run dashboard_agent.py \
    --server.port 8501 --server.headless true --server.address 0.0.0.0 \
    > "$LOG_DIR/streamlit.log" 2>&1 &
STREAMLIT_PID=$!
wait_http "http://localhost:8501/" "streamlit:8501" 60
open http://localhost:8501 2>/dev/null || true
ok "Dashboard → http://localhost:8501  (PID=$STREAMLIT_PID)"

# ─────────────────────────────────────────────────────────────────────────────
step "3 / INJECTION D'ANOMALIES — simulation données corrompues"
# ─────────────────────────────────────────────────────────────────────────────

"$PYTHON" inject_to_db.py \
    --pipeline clean_sales --types nulls,out_of_range --rate 0.15 --seed 42 \
    2>&1 | grep -E "✅|alertes|points écrits|Injection"

"$PYTHON" inject_to_db.py \
    --pipeline ingest_sales --types duplicates --rate 0.10 --seed 99 \
    2>&1 | grep -E "✅|alertes|points écrits|Injection"

# ─────────────────────────────────────────────────────────────────────────────
step "4 / DÉTECTION MULTI-ALGORITHMES — threshold · z-score · IQR · trend"
# ─────────────────────────────────────────────────────────────────────────────

"$PYTHON" - <<'EOF'
import urllib.request, json, sys
sys.path.insert(0, ".")

# 4a — détection via API (règles métier)
req = urllib.request.Request("http://localhost:8090/api/v1/alerts/detect",
                              data=b"", method="POST",
                              headers={"Content-Type": "application/json"})
d = json.loads(urllib.request.urlopen(req, timeout=15).read())
print(f"  ✔  {d['detected']} anomalies détectées (règles métier)  →  {d['saved']} sauvegardées")
print(f"  ·  Par sévérité : {d['by_severity']}")

# 4b — détection ML multi-couches
try:
    from spark.metrics.storage          import SQLiteMetricsStore
    from spark.metrics.pipeline_adapter import build_pipeline_snapshots
    from spark.metrics.anomaly_detection import AnomalyDetector as MLDetector

    store     = SQLiteMetricsStore()
    snapshots = build_pipeline_snapshots(store)
    ml        = MLDetector()
    ml_anoms  = [a for snap in snapshots for a in ml.detect(snap)]
    print(f"  ✔  {len(ml_anoms)} anomalies ML (VolumeDetector / SchemaDetector / TemporalDetector…)")
    for a in ml_anoms[:5]:
        print(f"     [{a.severity.value:8}]  {a.pipeline_name:<20}  {a.metric_name}")
    if len(ml_anoms) > 5:
        print(f"     … et {len(ml_anoms)-5} autre(s)")
except Exception as e:
    print(f"  ·  ML detector : {e}")

# résumé alertes
a = json.loads(urllib.request.urlopen("http://localhost:8090/api/v1/alerts/summary", timeout=5).read())
print(f"\n  ✔  Total alertes : {a['total']}  (unack={a['unacknowledged']})")
for row in a['by_severity']:
    print(f"     {row['severity']:10} : {row['count']}")
EOF

# ─────────────────────────────────────────────────────────────────────────────
step "5 / EXPLICATION LLM — Mistral AI (RGPD · hébergé France)"
# ─────────────────────────────────────────────────────────────────────────────

"$PYTHON" - <<'EOF'
import sys
sys.path.insert(0, ".")
try:
    from spark.metrics.llm_explainer import LLMExplainer
    from spark.metrics.anomaly_detection.models import Anomaly, AnomalyLevel, Severity

    explainer = LLMExplainer()
    print(f"  ·  Backend actif : {explainer._active_backend()}\n")

    anomalies = [
        Anomaly(pipeline_name="clean_sales", metric_name="rejection_rate_pct",
                level=AnomalyLevel.VOLUME, severity=Severity.CRITICAL,
                value=45.2, expected_range=(0.0, 20.0),
                description="rejection_rate_pct=45.2 dépasse le seuil 20%"),
        Anomaly(pipeline_name="ingest_sales", metric_name="rows_output",
                level=AnomalyLevel.VOLUME, severity=Severity.CRITICAL,
                value=850.0, expected_range=(30000.0, 60000.0),
                description="Volume 850 — chute de 98%"),
    ]
    enriched = explainer.enrich_anomalies(anomalies)
    for a in enriched:
        expl = (a.context or {}).get("llm_explanation", "non disponible")
        print(f"  [{a.severity.value}]  {a.pipeline_name} / {a.metric_name}  =  {a.value}")
        print(f"  « {expl} »\n")
except Exception as e:
    print(f"  · LLMExplainer : {e}")
EOF

# ─────────────────────────────────────────────────────────────────────────────
step "6 / ALERTING & NOTIFICATIONS — règles · canaux · cooldown"
# ─────────────────────────────────────────────────────────────────────────────

"$PYTHON" - <<'EOF'
import sys, os, tempfile
sys.path.insert(0, ".")
from spark.alerting import (
    AlertRule, AlertRuleStore, NotificationChannel, NotificationService
)

tmp = tempfile.mkdtemp(prefix="demo_notif_")

# ── 6.1 Règles configurables ─────────────────────────────────────────────────
print("  ── 6.1  Règles d'alerte configurables (AlertRuleStore)")
store = AlertRuleStore(db_path=f"{tmp}/rules.db", seed_defaults=False)

rules = [
    AlertRule("*",               "*",                   "CRITICAL", ["console","alertmanager"], [],                        cooldown_min=0),
    AlertRule("clean_sales",     "rejection_rate_pct",  "HIGH",     ["console","teams","email"],["ops@delomid-it.com"],    cooldown_min=0),
    AlertRule("ingest_sales",    "rows_output",         "HIGH",     ["console","alertmanager"], [],                        cooldown_min=0),
    AlertRule("aggregate_sales", "duration_sec",        "MEDIUM",   ["console"],               [],                        cooldown_min=0),
]
for r in rules: store.add_rule(r)
print(f"\n  ✔  {store.count()} règles enregistrées :")
for r in store.list_rules():
    print(f"     {'✅' if r.enabled else '❌'}  [{r.severity_min:8}]  {r.pipeline_name:<20}  {r.metric_name:<25}  → {r.channels}")

# ── 6.2 Quatre canaux en parallèle ───────────────────────────────────────────
print("\n  ── 6.2  Quatre canaux : console · teams · email · alertmanager")
svc = NotificationService(
    channels=[
        NotificationChannel("console"),
        NotificationChannel("teams",        {"webhook_url": "https://teams.webhook.office.com/demo"}),
        NotificationChannel("email",        {"smtp_host": "smtp.delomid-it.com", "smtp_port": 587,
                                              "smtp_user": "alerts@delomid-it.com",
                                              "from_addr": "alerts@delomid-it.com", "use_tls": True}),
        NotificationChannel("alertmanager", {"url": "http://localhost:9093",
                                              "generator_url": "http://localhost:8090"}),
    ],
    log_db_path=f"{tmp}/notif.db",
    dry_run=True,   # console + alertmanager sont réels ; teams/email simulés
)

alerts_to_send = [
    {"metric_name": "rejection_rate_pct", "source": "clean_sales",
     "severity": "HIGH",     "value": 45.2,  "details": "Taux rejet 45.2% > seuil 20%"},
    {"metric_name": "rows_output",        "source": "ingest_sales",
     "severity": "HIGH",     "value": 850.0, "details": "Volume 850 < seuil min 5 000 (chute 98%)"},
    {"metric_name": "duration_sec",       "source": "aggregate_sales",
     "severity": "MEDIUM",   "value": 487.0, "details": "Durée 487s — SLA 300s dépassé"},
    {"metric_name": "rows_output",        "source": "clean_sales",
     "severity": "CRITICAL", "value": 0.0,   "details": "Aucune ligne produite — pipeline bloqué"},
]

total_sent = 0
for alert in alerts_to_send:
    matching = store.get_matching_rules(alert["source"], alert["metric_name"], alert["severity"])
    if not matching:
        continue
    print(f"\n  [{alert['severity']:8}]  {alert['source']}.{alert['metric_name']} = {alert['value']}")
    results = svc.notify_many(alert, matching)
    for r in results:
        icon = "📤" if r["status"] == "sent" else ("⏸" if r["status"] == "cooldown" else "❌")
        detail = str(r.get("detail", ""))[:80]
        print(f"    {icon}  canal={r['channel']:14}  statut={r['status']:8}  {detail}")
        if r["status"] == "sent": total_sent += 1
print(f"\n  ✔  {total_sent} notification(s) envoyée(s) au total")

# ── 6.3 Cooldown : une même alerte ne doit pas spammer ───────────────────────
print("\n  ── 6.3  Cooldown — protection anti-spam")
store2 = AlertRuleStore(db_path=f"{tmp}/rules2.db", seed_defaults=False)
store2.add_rule(AlertRule("clean_sales", "rejection_rate_pct", "HIGH",
                           ["console"], [], cooldown_min=30))
svc2 = NotificationService(channels=[NotificationChannel("console")],
                            log_db_path=f"{tmp}/notif2.db", dry_run=True)

alert_cd = {"metric_name": "rejection_rate_pct", "source": "clean_sales",
             "severity": "HIGH", "value": 45.2, "details": "test cooldown"}
rule_cd  = store2.get_matching_rules("clean_sales", "rejection_rate_pct", "HIGH")[0]

r1 = svc2.notify(alert_cd, rule_cd)
r2 = svc2.notify(alert_cd, rule_cd)   # doit être bloquée par cooldown (30 min)
print(f"  1er envoi   → statut={r1[0]['status']}")
print(f"  2ème envoi  → statut={r2[0]['status']}  (cooldown 30 min actif ✔)")

# ── 6.4 Canal alertmanager — envoi réel vers AlertManager :9093 ──────────────
print("\n  ── 6.4  Canal alertmanager → POST réel vers http://localhost:9093")
svc_real = NotificationService(
    channels=[NotificationChannel("alertmanager", {"url": "http://localhost:9093",
                                                    "generator_url": "http://localhost:8090"})],
    log_db_path=f"{tmp}/notif_am.db",
    dry_run=False,   # envoi HTTP réel
)
store3 = AlertRuleStore(db_path=f"{tmp}/rules3.db", seed_defaults=False)
store3.add_rule(AlertRule("clean_sales", "rejection_rate_pct", "CRITICAL",
                           ["alertmanager"], [], cooldown_min=0))
rule_am = store3.get_matching_rules("clean_sales", "rejection_rate_pct", "CRITICAL")[0]
try:
    res = svc_real.notify({"metric_name": "rejection_rate_pct", "source": "clean_sales",
                            "severity": "CRITICAL", "value": 45.2,
                            "details": "CRITICAL rejet — envoi réel AlertManager"},
                           rule_am)
    for r in res:
        icon = "📤" if r["status"] == "sent" else "❌"
        print(f"  {icon}  canal={r['channel']}  statut={r['status']}  {r.get('detail','')}")
except Exception as e:
    print(f"  ·  alertmanager : {e}")

# ── 6.5 Désactivation / réactivation d'une règle ─────────────────────────────
print("\n  ── 6.5  Désactivation / réactivation d'une règle")
r_list = store.list_rules()
rule_id = r_list[1].rule_id
store.disable(rule_id)
print(f"  ·  Règle {rule_id[:8]}… désactivée — matching : "
      f"{len(store.get_matching_rules('clean_sales','rejection_rate_pct','HIGH'))} règle(s)")
store.enable(rule_id)
print(f"  ·  Règle réactivée — matching : "
      f"{len(store.get_matching_rules('clean_sales','rejection_rate_pct','HIGH'))} règle(s)")

# ── 6.6 Envoi réel Teams + Outlook (si configuré via variables d'env) ────────
print("\n  ── 6.6  Envoi réel Teams + Outlook (si configuré)")
import os as _os
from spark.alerting.teams_notifier   import TeamsNotifier
from spark.alerting.outlook_notifier import OutlookNotifier

alert_demo = {"pipeline": "clean_sales", "metric": "rejection_rate_pct",
              "severity": "CRITICAL", "value": 45.2,
              "details": "Taux de rejet 45.2% > seuil 20% — démo PFA"}

teams = TeamsNotifier()
if teams.configured:
    res = teams.send_alert(**alert_demo)
    icon = "📤" if res["ok"] else "❌"
    print(f"    {icon}  Teams   → {res['detail']}")
else:
    print("    ⏸  Teams   → non configuré (définir TEAMS_WEBHOOK_URL pour un envoi réel)")

outlook = OutlookNotifier()
if outlook.configured:
    res = outlook.send_alert(**alert_demo)
    icon = "📤" if res["ok"] else "❌"
    print(f"    {icon}  Outlook → {res['detail']}")
else:
    print("    ⏸  Outlook → non configuré (définir OUTLOOK_SMTP_USER / OUTLOOK_SMTP_PASSWORD / OUTLOOK_RECIPIENTS pour un envoi réel)")
EOF

# ─────────────────────────────────────────────────────────────────────────────
step "7 / INCIDENTS — cycle de vie · escalade · récurrence"
# ─────────────────────────────────────────────────────────────────────────────

"$PYTHON" - <<'EOF'
import sys, tempfile
sys.path.insert(0, ".")
from spark.alerting import IncidentRecord, IncidentManager

tmp = tempfile.mkdtemp(prefix="demo_")
mgr = IncidentManager(db_path=f"{tmp}/inc.db")

inc1 = mgr.create(IncidentRecord(pipeline="ingest_sales",
                  title="Volume anormalement bas : 850 lignes", severity="HIGH"))
inc2 = mgr.create(IncidentRecord(pipeline="clean_sales",
                  title="Taux de rejet 45.2%", severity="HIGH"))
inc3 = mgr.create(IncidentRecord(pipeline="aggregate_sales",
                  title="Durée 487s — SLA dépassé", severity="CRITICAL"))

print(f"  ✔  3 incidents créés")
for inc in [inc1, inc2, inc3]:
    print(f"     #{inc.id:02d}  [{inc.severity:8}]  {inc.pipeline:<20}  {inc.status}")

mgr.update_status(inc2.id, "investigating", "Équipe data contactée")
new_sev = mgr.escalate(inc3.id)
print(f"\n  ⚙️  Incident #{inc2.id} → investigating")
print(f"  ⬆️  Incident #{inc3.id} escaladé → {new_sev}")

for _ in range(2):
    mgr.create(IncidentRecord(pipeline="clean_sales", title="Rejet récurrent", severity="HIGH"))
is_rec = mgr.detect_recurring("clean_sales", window_hours=1, threshold=3)
print(f"\n  🔁 Récurrence clean_sales (≥3 incidents/1h) : {is_rec}")

mgr.resolve(inc1.id, "Volume remonté après relance du job amont")
print(f"  ✅ Incident #{inc1.id} résolu")
print(f"\n  Résumé : {mgr.summary()}")
EOF

# ─────────────────────────────────────────────────────────────────────────────
step "8 / RUNBOOKS — 5 pré-chargés · suggestion · exécution · génération auto"
# ─────────────────────────────────────────────────────────────────────────────

"$PYTHON" - <<'EOF'
import sys, tempfile
sys.path.insert(0, ".")
from spark.alerting import RunbookEngine, IncidentRecord, IncidentManager

tmp    = tempfile.mkdtemp(prefix="demo_")
engine = RunbookEngine(db_path=f"{tmp}/rb.db")
mgr    = IncidentManager(db_path=f"{tmp}/inc.db")

print(f"  ✔  {len(engine.get_all())} runbooks pré-chargés :")
for rb in engine.get_all():
    print(f"     [{rb.id}] {rb.title:<45}  ({len(rb.steps)} étapes)")

inc = mgr.create(IncidentRecord(pipeline="clean_sales",
                  title="Rejet élevé", severity="HIGH"))
rb  = engine.find_runbook("rejection_rate_pct")
print(f"\n  ✔  Runbook suggéré : «{rb.title}»")
for step in engine.execute(rb, inc, dry_run=True):
    print(f"     {step}")

# Runbook dynamique pour pipeline récidiviste
rb_auto = engine.register_recurring(pipeline="clean_sales", metric="rejection_rate_pct")
print(f"\n  🤖 Runbook dynamique généré : «{rb_auto.title}»  ({len(rb_auto.steps)} étapes)")
EOF

# ─────────────────────────────────────────────────────────────────────────────
step "9 / DATA CATALOG — score qualité · incidents · lineage dbt"
# ─────────────────────────────────────────────────────────────────────────────

"$PYTHON" - <<'EOF'
import sys, os, tempfile
sys.path.insert(0, ".")
from spark.catalog.catalog import DataCatalog
from spark.catalog.models  import CatalogEntry

tmp     = tempfile.mkdtemp(prefix="demo_")
catalog = DataCatalog(db_path=f"{tmp}/catalog.db")

for name, type_, desc, score, tags in [
    ("ingest_sales",    "pipeline",  "Ingestion ventes brutes (WideWorldImporters)", 0.97, ["spark","sales"]),
    ("clean_sales",     "pipeline",  "Nettoyage et validation (Pandera + Spark)",    0.97, ["spark","sales"]),
    ("aggregate_sales", "pipeline",  "Agrégation journalière des ventes nettoyées",  0.97, ["spark","sales"]),
    ("stg_sales",       "dbt_model", "Vue staging dbt — mapping brut → canonique",   1.0,  ["dbt","staging"]),
    ("sales_summary",   "dbt_model", "Table mart dbt — ventes par jour/produit",      1.0,  ["dbt","mart"]),
]:
    catalog.publish(CatalogEntry(name=name, type=type_, description=desc,
                                  owner="data-team", tags=tags, quality_score=score))

print("  ✔  5 entrées publiées :")
for e in catalog.list_entries():
    print(f"     {e.name:<22}  [{e.type:<10}]  score={e.quality_score:.2f}  {e.tags}")

# Dégradation et rétablissement du score
catalog.publish_from_detection("clean_sales", n_anomalies=3, worst_severity="CRITICAL",
    anomalies=[type('A',(),{'severity':'CRITICAL','level':type('L',(),{'value':'volume'})(),
               'description':'Volume chute 98%'})(),
               type('A',(),{'severity':'HIGH','level':type('L',(),{'value':'volume'})(),
               'description':'Taux rejet 28.5%'})()]
)
print(f"\n  · Score clean_sales après anomalies CRITICAL : {catalog.get('clean_sales').quality_score:.3f}")

incidents = catalog.incidents("clean_sales")
for inc in incidents: catalog.resolve_incident(inc.id)
catalog.publish_from_detection("clean_sales", n_anomalies=0, worst_severity=None, anomalies=[])
print(f"  · Score clean_sales après résolution         : {catalog.get('clean_sales').quality_score:.3f}")

# Lineage dbt
try:
    from spark.catalog.lineage import LineageParser
    manifest = "dbt/target/manifest.json"
    if os.path.exists(manifest):
        graph = LineageParser().from_dbt(manifest)
        print(f"\n  ✔  Lineage dbt : {len(graph.nodes)} nœuds / {len(graph.edges)} arêtes")
        for n in list(graph.nodes.values())[:4]:
            ups = graph.upstream_of(n.name)
            if ups: print(f"     {n.name:<35} ← {ups}")
    else:
        print("\n  ·  Lineage : stg_sales ← raw.sales  →  int_sales_daily  →  sales_summary")
except Exception as e:
    print(f"\n  ·  Lineage : {e}")
EOF

# ─────────────────────────────────────────────────────────────────────────────
step "10 / PROMETHEUS — exposition métriques · targets · AlertManager"
# ─────────────────────────────────────────────────────────────────────────────

"$PYTHON" - <<'EOF'
import urllib.request, json

# Métriques exposées
with urllib.request.urlopen("http://localhost:8090/metrics", timeout=5) as r:
    lines = r.read().decode().splitlines()
meta = [l for l in lines if l.startswith("#")]
data = [l for l in lines if l and not l.startswith("#")]
print(f"  ✔  Endpoint /metrics : {len(meta)} lignes méta + {len(data)} séries")
for l in data[:5]: print(f"     {l}")
print("     …")

# Prometheus targets
tgt = json.loads(urllib.request.urlopen("http://localhost:9090/api/v1/targets", timeout=5).read())
targets = tgt["data"]["activeTargets"]
print(f"\n  ✔  Prometheus targets ({len(targets)}) :")
for t in targets:
    print(f"     ✔  {t['labels'].get('job','?'):<30}  health={t['health']}")

# AlertManager alertes actives
alerts = json.loads(urllib.request.urlopen("http://localhost:9093/api/v2/alerts", timeout=5).read())
print(f"\n  ✔  AlertManager : {len(alerts)} alerte(s) active(s)")
for a in alerts[:4]:
    lbl = a.get("labels", {})
    print(f"     [{lbl.get('severity','?'):8}]  {lbl.get('alertname','?')}")

# Webhook bout-en-bout
payload = json.dumps({"alerts":[{"labels":{"alertname":"DemoE2E","severity":"critical",
                      "pipeline":"clean_sales"},"status":"firing"}]}).encode()
req = urllib.request.Request("http://localhost:8090/api/v1/alerts/webhook",
                              data=payload, method="POST",
                              headers={"Content-Type":"application/json"})
d = json.loads(urllib.request.urlopen(req, timeout=10).read())
print(f"\n  ✔  Webhook AlertManager→SQLite : reçu={d['received']}  persisté={d['persisted']}")
EOF

# ─────────────────────────────────────────────────────────────────────────────
step "11 / SEMAINE 14 — Alerting · Incidents · Runbooks (scénarios complets)"
# ─────────────────────────────────────────────────────────────────────────────

"$PYTHON" livrable_semaine14.py 2>&1

# ─────────────────────────────────────────────────────────────────────────────
step "12 / SEMAINE 15 — Intégration end-to-end (5 scénarios)"
# ─────────────────────────────────────────────────────────────────────────────

"$PYTHON" livrable_semaine15.py 2>&1

# ─────────────────────────────────────────────────────────────────────────────
hr
echo ""
echo "  ╔══════════════════════════════════════════════════════════════╗"
echo "  ║          DATA TRUST AGENT — DÉMO PFA COMPLÈTE  ✔           ║"
echo "  ╠══════════════════════════════════════════════════════════════╣"
echo "  ║  Dashboard Streamlit  →  http://localhost:8501              ║"
echo "  ║  Metrics API (docs)   →  http://localhost:8090/docs         ║"
echo "  ║  Prometheus           →  http://localhost:9090              ║"
echo "  ║  AlertManager         →  http://localhost:9093              ║"
echo "  ╠══════════════════════════════════════════════════════════════╣"
echo "  ║  Ctrl+C  pour arrêter │  docker compose down  pour Docker   ║"
echo "  ╚══════════════════════════════════════════════════════════════╝"
echo ""

tail -f "$LOG_DIR/streamlit.log"
