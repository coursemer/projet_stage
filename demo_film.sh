#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════════════════════════
#  Data Trust Agent — Démo filmable PFA
#  Usage : bash demo_film.sh
# ══════════════════════════════════════════════════════════════════════════════
set -uo pipefail

PYTHON=".venv/bin/python"
STREAMLIT=".venv/bin/streamlit"
LOG_DIR="/tmp/dta_film"
mkdir -p "$LOG_DIR"

# ── Couleurs & helpers ────────────────────────────────────────────────────────
G='\033[1;32m'; C='\033[1;36m'; Y='\033[1;33m'; M='\033[1;35m'; R='\033[0m'
ok()   { echo -e "  ${G}✔${R}  $*"; }
info() { echo -e "  ${C}·${R}  $*"; }
hr()   { echo -e "${C}$(printf '═%.0s' {1..68})${R}"; }
step() {
    echo; hr
    echo -e "  ${M}$*${R}"
    hr; echo
}
pause() {
    echo
    echo -e "  ${Y}▶  Appuyer sur [Entrée] pour continuer...${R}"
    read -r
}
wait_http() {
    local url=$1 label=$2 n=0
    printf "  ${C}·${R}  attente %-28s" "$label"
    until curl -sf "$url" > /dev/null 2>&1; do
        sleep 2; ((n+=2)); printf "."
        (( n > 180 )) && { echo " TIMEOUT"; return 1; }
    done
    echo -e " ${G}OK${R} (${n}s)"
}

[[ -f "$PYTHON" ]] || { echo "ERREUR : .venv introuvable"; exit 1; }
command -v docker > /dev/null || { echo "ERREUR : docker manquant"; exit 1; }

# ══════════════════════════════════════════════════════════════════════════════
step "ÉTAPE 1 — Infrastructure Docker"
# ══════════════════════════════════════════════════════════════════════════════

docker compose up -d prometheus alertmanager influxdb metrics-api \
    > "$LOG_DIR/docker.log" 2>&1
ok "Stack Docker lancé"

wait_http "http://localhost:8090/api/v1/health" "Metrics API   :8090"
wait_http "http://localhost:9090/-/ready"        "Prometheus    :9090"
wait_http "http://localhost:9093/-/ready"        "AlertManager  :9093"

"$PYTHON" - <<'PY'
import urllib.request, json
h = json.loads(urllib.request.urlopen("http://localhost:8090/api/v1/health",  timeout=5).read())
a = json.loads(urllib.request.urlopen("http://localhost:8090/api/v1/alerts/summary", timeout=5).read())
print(f"  ·  Métriques en base : {h.get('total_metrics', 0):,}")
print(f"  ·  Alertes en base   : {a.get('total', 0):,}  (non-acquittées : {a.get('unacknowledged', 0)})")
PY

pause

# ══════════════════════════════════════════════════════════════════════════════
step "ÉTAPE 2 — Dashboard Streamlit"
# ══════════════════════════════════════════════════════════════════════════════

lsof -ti:8501 | xargs kill -9 2>/dev/null || true
sleep 1

"$STREAMLIT" run dashboard_agent.py \
    --server.port 8501 --server.headless true --server.address 0.0.0.0 \
    > "$LOG_DIR/streamlit.log" 2>&1 &
STREAMLIT_PID=$!

wait_http "http://localhost:8501/" "Dashboard     :8501"
open "http://localhost:8501" 2>/dev/null || true
ok "Dashboard ouvert → http://localhost:8501"
info "→ Montrer : santé des pipelines, scores qualité, liste anomalies"

pause

# ══════════════════════════════════════════════════════════════════════════════
step "ÉTAPE 3 — Données source : WideWorldImporters (DuckDB + dbt)"
# ══════════════════════════════════════════════════════════════════════════════

"$PYTHON" - <<'PY'
import sys
sys.path.insert(0, ".")
try:
    import duckdb
    con = duckdb.connect("dbt/dev.duckdb", read_only=True)
    tables = con.execute("SHOW TABLES").fetchall()
    print(f"  ✔  {len(tables)} tables dans dbt/dev.duckdb\n")
    for t in tables:
        try:
            n = con.execute(f"SELECT COUNT(*) FROM {t[0]}").fetchone()[0]
            print(f"     {t[0]:<40}  {n:>10,} lignes")
        except Exception:
            pass
    con.close()
except Exception as e:
    print(f"  · {e}")
PY

echo ""
info "Modèles dbt compilés :"
for f in dbt/models/staging/*.sql dbt/models/intermediate/*.sql dbt/models/marts/*.sql; do
    [[ -f "$f" ]] || continue
    layer=$(echo "$f" | grep -oE 'staging|intermediate|marts')
    name=$(basename "$f" .sql)
    echo -e "     ${G}✔${R}  [$layer]  $name"
done

pause

# ══════════════════════════════════════════════════════════════════════════════
step "ÉTAPE 4 — Injection d'anomalies (simulation production)"
# ══════════════════════════════════════════════════════════════════════════════

info "Injection nulls + valeurs hors plage sur clean_sales (15%)..."
"$PYTHON" inject_to_db.py \
    --pipeline clean_sales --types nulls,out_of_range --rate 0.15 --seed 42 \
    2>&1 | grep -E "✅|alertes|points écrits|Injection"

echo ""
info "Injection doublons sur ingest_sales (10%)..."
"$PYTHON" inject_to_db.py \
    --pipeline ingest_sales --types duplicates --rate 0.10 --seed 99 \
    2>&1 | grep -E "✅|alertes|points écrits|Injection"

pause

# ══════════════════════════════════════════════════════════════════════════════
step "ÉTAPE 5 — Détection multi-algorithmes (threshold · z-score · IQR · trend + ML)"
# ══════════════════════════════════════════════════════════════════════════════

"$PYTHON" - <<'PY'
import sys, urllib.request, json
sys.path.insert(0, ".")

# Détection règles métier via API
req = urllib.request.Request("http://localhost:8090/api/v1/alerts/detect",
                              data=b"", method="POST",
                              headers={"Content-Type": "application/json"})
d = json.loads(urllib.request.urlopen(req, timeout=15).read())
print(f"  ✔  Règles métier    : {d['detected']} anomalies  →  {d['saved']} sauvegardées")
print(f"     Sévérités        : {d['by_severity']}")

# Détection ML multi-couches
try:
    from spark.metrics.storage           import SQLiteMetricsStore
    from spark.metrics.pipeline_adapter  import build_pipeline_snapshots
    from spark.metrics.anomaly_detection import AnomalyDetector as MLDet

    store  = SQLiteMetricsStore()
    snaps  = build_pipeline_snapshots(store)
    ml     = MLDet()
    anoms  = [a for s in snaps for a in ml.detect(s)]
    print(f"\n  ✔  ML multi-couches : {len(anoms)} anomalie(s) sur {len(snaps)} pipeline(s)")
    for a in anoms[:6]:
        print(f"     [{a.severity.value:8}]  {a.pipeline_name:<20}  {a.metric_name}")
    if len(anoms) > 6:
        print(f"     … et {len(anoms)-6} autre(s)")
except Exception as e:
    print(f"  ·  ML : {e}")

# Résumé alertes
a = json.loads(urllib.request.urlopen("http://localhost:8090/api/v1/alerts/summary", timeout=5).read())
print(f"\n  ✔  Total alertes en base : {a['total']}  (unack={a['unacknowledged']})")
for row in a['by_severity']:
    bar = "█" * min(row['count'] // 3, 30)
    print(f"     {row['severity']:10}  {row['count']:>4}  {bar}")
PY

pause

# ══════════════════════════════════════════════════════════════════════════════
step "ÉTAPE 6 — Explication LLM (Mistral AI — RGPD · hébergé France)"
# ══════════════════════════════════════════════════════════════════════════════

"$PYTHON" - <<'PY'
import sys
sys.path.insert(0, ".")
from spark.metrics.llm_explainer import LLMExplainer

explainer = LLMExplainer()

alerts = [
    {"metric_name": "rejection_rate_pct", "source": "clean_sales",
     "severity": "critical", "value": 45.2,
     "details": "Taux de rejet 45.2% > seuil critique 20%"},
    {"metric_name": "rows_output", "source": "ingest_sales",
     "severity": "critical", "value": 850.0,
     "details": "Volume 850 lignes — attendu ~45 000 (chute de 98%)"},
]

for alert in alerts:
    expl = explainer.explain_alert_dict(alert)
    print(f"  [{alert['severity'].upper()}]  {alert['source']} / {alert['metric_name']}  =  {alert['value']}")
    print(f"  « {expl} »")
    print()
PY

pause

# ══════════════════════════════════════════════════════════════════════════════
step "ÉTAPE 7 — Alerting : règles configurables + 4 canaux de notification"
# ══════════════════════════════════════════════════════════════════════════════

"$PYTHON" - <<'PY'
import sys, os, tempfile
sys.path.insert(0, ".")
from spark.alerting import (
    AlertRule, AlertRuleStore, NotificationChannel, NotificationService
)

tmp   = tempfile.mkdtemp(prefix="demo_")
store = AlertRuleStore(db_path=f"{tmp}/rules.db", seed_defaults=False)

store.add_rule(AlertRule("*",            "*",                  "CRITICAL", ["console", "alertmanager"], [],                        cooldown_min=0))
store.add_rule(AlertRule("clean_sales",  "rejection_rate_pct", "HIGH",     ["console", "teams", "email"], ["ops@delomid-it.com"], cooldown_min=0))
store.add_rule(AlertRule("ingest_sales", "rows_output",        "HIGH",     ["console", "alertmanager"], [],                        cooldown_min=0))

print(f"  ✔  {store.count()} règles configurées :\n")
for r in store.list_rules():
    print(f"     {'✅' if r.enabled else '❌'}  [{r.severity_min:8}]  {r.pipeline_name:<20}  → {r.channels}")

svc = NotificationService(
    channels=[
        NotificationChannel("console"),
        NotificationChannel("teams",        {"webhook_url": "https://teams.webhook.office.com/demo"}),
        NotificationChannel("email",        {"smtp_host": "smtp.delomid-it.com", "smtp_port": 587,
                                              "smtp_user": "alerts@delomid-it.com",
                                              "from_addr": "alerts@delomid-it.com"}),
        NotificationChannel("alertmanager", {"url": "http://localhost:9093",
                                              "generator_url": "http://localhost:8090"}),
    ],
    log_db_path=f"{tmp}/notif.db",
    dry_run=True,
)

alerts_demo = [
    {"metric_name": "rejection_rate_pct", "source": "clean_sales",
     "severity": "HIGH",     "value": 45.2,  "details": "Taux rejet 45.2% > seuil 20%"},
    {"metric_name": "rows_output",        "source": "ingest_sales",
     "severity": "HIGH",     "value": 850.0, "details": "Volume 850 < seuil 5 000 (chute 98%)"},
    {"metric_name": "rows_output",        "source": "clean_sales",
     "severity": "CRITICAL", "value": 0.0,   "details": "Pipeline bloqué — aucune ligne produite"},
]

total = 0
for alert in alerts_demo:
    matching = store.get_matching_rules(alert["source"], alert["metric_name"], alert["severity"])
    if not matching:
        continue
    print(f"\n  [{alert['severity']:8}]  {alert['source']}.{alert['metric_name']}  =  {alert['value']}")
    for r in svc.notify_many(alert, matching):
        icon = "📤" if r["status"] == "sent" else "⏸"
        detail = str(r.get("detail", ""))[:70]
        print(f"    {icon}  canal={r['channel']:14}  {r['status']:8}  {detail}")
        if r["status"] == "sent": total += 1

print(f"\n  ✔  {total} notification(s) envoyée(s)")

# Test cooldown
store2 = AlertRuleStore(db_path=f"{tmp}/r2.db", seed_defaults=False)
store2.add_rule(AlertRule("clean_sales", "rejection_rate_pct", "HIGH", ["console"], [], cooldown_min=30))
svc2 = NotificationService(channels=[NotificationChannel("console")],
                            log_db_path=f"{tmp}/n2.db", dry_run=True)
rule2  = store2.get_matching_rules("clean_sales", "rejection_rate_pct", "HIGH")[0]
alert2 = {"metric_name": "rejection_rate_pct", "source": "clean_sales",
           "severity": "HIGH", "value": 45.2, "details": "test"}
r1 = svc2.notify(alert2, rule2)[0]
r2 = svc2.notify(alert2, rule2)[0]
print(f"\n  Cooldown (30 min) : 1er envoi={r1['status']}  2ème envoi={r2['status']}  ✔")
PY

pause

# ══════════════════════════════════════════════════════════════════════════════
step "ÉTAPE 8 — Gestion des incidents (cycle de vie · escalade · récurrence)"
# ══════════════════════════════════════════════════════════════════════════════

"$PYTHON" - <<'PY'
import sys, tempfile
sys.path.insert(0, ".")
from spark.alerting import IncidentRecord, IncidentManager

tmp = tempfile.mkdtemp(prefix="demo_")
mgr = IncidentManager(db_path=f"{tmp}/inc.db")

# Création
i1 = mgr.create(IncidentRecord(pipeline="ingest_sales",    title="Volume bas : 850 lignes",    severity="HIGH"))
i2 = mgr.create(IncidentRecord(pipeline="clean_sales",     title="Taux de rejet 45.2%",         severity="HIGH"))
i3 = mgr.create(IncidentRecord(pipeline="aggregate_sales", title="Durée 487s — SLA dépassé",    severity="CRITICAL"))

print("  ✔  3 incidents créés :\n")
for inc in [i1, i2, i3]:
    print(f"     #{inc.id:02d}  [{inc.severity:8}]  {inc.pipeline:<22}  statut={inc.status}")

# Cycle de vie
mgr.update_status(i2.id, "investigating", "Analyse des logs en cours")
new_sev = mgr.escalate(i3.id)
print(f"\n  ⚙️   Incident #{i2.id} → investigating")
print(f"  ⬆️   Incident #{i3.id} escaladé → {new_sev}")

# Récurrence
for _ in range(2):
    mgr.create(IncidentRecord(pipeline="clean_sales", title="Rejet récurrent", severity="HIGH"))
is_rec = mgr.detect_recurring("clean_sales", window_hours=1, threshold=3)
print(f"\n  🔁  Récurrence clean_sales (≥3 incidents/1h) détectée : {is_rec}")

# Résolution
mgr.resolve(i1.id, "Volume remonté après relance du job amont")
print(f"  ✅  Incident #{i1.id} résolu")
print(f"\n  Résumé : {mgr.summary()}")
PY

pause

# ══════════════════════════════════════════════════════════════════════════════
step "ÉTAPE 9 — Runbooks automatiques (5 pré-chargés + génération dynamique)"
# ══════════════════════════════════════════════════════════════════════════════

"$PYTHON" - <<'PY'
import sys, tempfile
sys.path.insert(0, ".")
from spark.alerting import RunbookEngine, IncidentRecord, IncidentManager

tmp    = tempfile.mkdtemp(prefix="demo_")
engine = RunbookEngine(db_path=f"{tmp}/rb.db")
mgr    = IncidentManager(db_path=f"{tmp}/inc.db")

print(f"  ✔  {len(engine.get_all())} runbooks pré-chargés :\n")
for rb in engine.get_all():
    print(f"     [{rb.id}]  {rb.title:<48}  ({len(rb.steps)} étapes)")

# Exécution dry_run sur un incident réel
inc = mgr.create(IncidentRecord(pipeline="clean_sales", title="Rejet 45%", severity="HIGH"))
rb  = engine.find_runbook("rejection_rate_pct")
print(f"\n  ✔  Runbook suggéré pour «clean_sales / rejection_rate_pct» :\n")
print(f"     «{rb.title}»\n")
for step in engine.execute(rb, inc, dry_run=True):
    print(f"     {step}")

# Génération dynamique pour pipeline récidiviste
rb_auto = engine.register_recurring(pipeline="clean_sales", metric="rejection_rate_pct")
print(f"\n  🤖  Runbook dynamique généré automatiquement :")
print(f"     «{rb_auto.title}»  ({len(rb_auto.steps)} étapes)")
PY

pause

# ══════════════════════════════════════════════════════════════════════════════
step "ÉTAPE 10 — Data Catalog (score qualité · incidents · lineage dbt)"
# ══════════════════════════════════════════════════════════════════════════════

"$PYTHON" - <<'PY'
import sys, os, tempfile
sys.path.insert(0, ".")
from spark.catalog.catalog import DataCatalog
from spark.catalog.models  import CatalogEntry

tmp     = tempfile.mkdtemp(prefix="demo_")
catalog = DataCatalog(db_path=f"{tmp}/catalog.db")

# Publication initiale
entries = [
    ("ingest_sales",    "pipeline",  "Ingestion ventes brutes — WideWorldImporters", ["spark","sales"],    0.97),
    ("clean_sales",     "pipeline",  "Nettoyage et validation Pandera + Spark",       ["spark","cleaning"], 0.97),
    ("aggregate_sales", "pipeline",  "Agrégation journalière des ventes",              ["spark","sales"],    0.97),
    ("stg_sales",       "dbt_model", "Vue staging — mapping brut → canonique",         ["dbt","staging"],    1.00),
    ("sales_summary",   "dbt_model", "Table mart — ventes agrégées par jour/produit",  ["dbt","mart"],       1.00),
]
for name, type_, desc, tags, score in entries:
    catalog.publish(CatalogEntry(name=name, type=type_, description=desc,
                                  owner="data-team", tags=tags, quality_score=score))

print("  ✔  Catalogue publié :\n")
print(f"  {'NOM':<22}  {'TYPE':<12}  {'SCORE':>6}  TAGS")
print(f"  {'─'*22}  {'─'*12}  {'─'*6}  {'─'*25}")
for e in catalog.list_entries():
    print(f"  {e.name:<22}  {e.type:<12}  {e.quality_score:>6.2f}  {e.tags}")

# Dégradation du score
class _A:
    def __init__(self, s, d):
        self.severity = s; self.description = d
        self.level = type("L", (), {"value": "volume"})()

catalog.publish_from_detection("clean_sales", n_anomalies=3, worst_severity="CRITICAL",
    anomalies=[_A("CRITICAL","Volume chute 98%"), _A("HIGH","Taux rejet 28.5%"), _A("MEDIUM","null_rate élevé")])

print(f"\n  Score clean_sales après anomalies CRITICAL  : {catalog.get('clean_sales').quality_score:.3f}  ← dégradé")

incidents = catalog.incidents("clean_sales")
print(f"  Incidents créés automatiquement             : {len(incidents)}")
for inc in incidents:
    print(f"    [{inc.severity}]  {inc.description}")

for inc in incidents:
    catalog.resolve_incident(inc.id)
catalog.publish_from_detection("clean_sales", n_anomalies=0, worst_severity=None, anomalies=[])
print(f"\n  Score clean_sales après résolution          : {catalog.get('clean_sales').quality_score:.3f}  ← rétabli")

# Lineage dbt
try:
    from spark.catalog.lineage import LineageParser
    if os.path.exists("dbt/target/manifest.json"):
        graph = LineageParser().from_dbt("dbt/target/manifest.json")
        print(f"\n  ✔  Lineage dbt : {len(graph.nodes)} nœuds  /  {len(graph.edges)} arêtes")
        for n in list(graph.nodes.values())[:4]:
            ups = graph.upstream_of(n.name)
            if ups:
                print(f"     {n.name:<35}  ←  {ups}")
except Exception as e:
    print(f"\n  ·  Lineage : {e}")
PY

pause

# ══════════════════════════════════════════════════════════════════════════════
step "ÉTAPE 11 — Prometheus · AlertManager · Webhook bout-en-bout"
# ══════════════════════════════════════════════════════════════════════════════

"$PYTHON" - <<'PY'
import urllib.request, json

# Métriques Prometheus
with urllib.request.urlopen("http://localhost:8090/metrics", timeout=5) as r:
    lines = r.read().decode().splitlines()
data = [l for l in lines if l and not l.startswith("#")]
meta = [l for l in lines if l.startswith("#")]
print(f"  ✔  /metrics  :  {len(meta)} lignes #HELP/#TYPE  +  {len(data)} séries de données")
for l in data[:5]:
    print(f"     {l}")
print("     …")

# Targets
tgt = json.loads(urllib.request.urlopen("http://localhost:9090/api/v1/targets", timeout=5).read())
targets = tgt["data"]["activeTargets"]
print(f"\n  ✔  Prometheus targets ({len(targets)}) :")
for t in targets:
    health = t["health"]
    icon = "✔" if health == "up" else "✗"
    print(f"     {icon}  {t['labels'].get('job','?'):<30}  health={health}")

# AlertManager
alerts = json.loads(urllib.request.urlopen("http://localhost:9093/api/v2/alerts", timeout=5).read())
print(f"\n  ✔  AlertManager  :  {len(alerts)} alerte(s) active(s)")
for a in alerts[:4]:
    lbl = a.get("labels", {})
    print(f"     [{lbl.get('severity','?'):8}]  {lbl.get('alertname','?')}")

# Webhook bout-en-bout
payload = json.dumps({"alerts": [{
    "labels":      {"alertname": "DemoE2E_PFA", "severity": "critical",
                    "pipeline": "clean_sales", "metric": "rejection_rate_pct"},
    "annotations": {"description": "Alerte bout-en-bout PFA"},
    "status":      "firing"
}]}).encode()
req = urllib.request.Request("http://localhost:8090/api/v1/alerts/webhook",
                              data=payload, method="POST",
                              headers={"Content-Type": "application/json"})
d = json.loads(urllib.request.urlopen(req, timeout=10).read())
print(f"\n  ✔  Webhook AlertManager → SQLite  :  reçu={d['received']}  persisté={d['persisted']}")
print(f"     Flux : Prometheus → règle → AlertManager → webhook → API → SQLite")
PY

info "→ Ouvrir dans le navigateur :"
info "   Prometheus  : http://localhost:9090/graph  (taper data_trust_job_rejection_rate_pct)"
info "   AlertManager: http://localhost:9093"
info "   API Swagger : http://localhost:8090/docs"

pause

# ══════════════════════════════════════════════════════════════════════════════
step "ÉTAPE 12 — Livrable Semaine 14 : Alerting complet (5 scénarios)"
# ══════════════════════════════════════════════════════════════════════════════

"$PYTHON" livrable_semaine14.py 2>&1
pause

# ══════════════════════════════════════════════════════════════════════════════
step "ÉTAPE 13 — Livrable Semaine 15 : Intégration end-to-end (5 scénarios)"
# ══════════════════════════════════════════════════════════════════════════════

"$PYTHON" livrable_semaine15.py 2>&1
pause

# ══════════════════════════════════════════════════════════════════════════════
hr
echo ""
echo -e "  ${G}╔══════════════════════════════════════════════════════════════╗${R}"
echo -e "  ${G}║        DATA TRUST AGENT — DÉMO PFA COMPLÈTE  ✔             ║${R}"
echo -e "  ${G}╠══════════════════════════════════════════════════════════════╣${R}"
echo -e "  ${G}║  Dashboard   →  http://localhost:8501                       ║${R}"
echo -e "  ${G}║  API Swagger →  http://localhost:8090/docs                  ║${R}"
echo -e "  ${G}║  Prometheus  →  http://localhost:9090                       ║${R}"
echo -e "  ${G}║  AlertManager→  http://localhost:9093                       ║${R}"
echo -e "  ${G}╚══════════════════════════════════════════════════════════════╝${R}"
echo ""
echo -e "  Logs Streamlit (Ctrl+C pour quitter) :"
tail -f "$LOG_DIR/streamlit.log"
