#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════════════════
#  Data Trust Agent — Démo complète
#  Usage : bash demo.sh
# ══════════════════════════════════════════════════════════════════════
set -euo pipefail

PYTHON=".venv/bin/python"
STREAMLIT=".venv/bin/streamlit"
LOG_DIR="/tmp/data_trust_demo"
mkdir -p "$LOG_DIR"

hr()  { printf '\n%s\n' "$(printf '═%.0s' {1..68})"; }
ok()  { echo "  ✔  $*"; }
info(){ echo "  ·  $*"; }
step(){ hr; echo "  $*"; hr; }
wait_http() {
    local url=$1 label=$2 max=${3:-120} n=0
    printf "  ·  attente %-30s" "$label"
    until curl -sf "$url" > /dev/null 2>&1; do
        sleep 3; ((n+=3))
        if (( n > max )); then echo " TIMEOUT (${max}s)"; return 1; fi
        printf "."
    done
    echo " OK (${n}s)"
}

# ─────────────────────────────────────────────────────────────────────
step "0 / VÉRIFICATION PRÉREQUIS"
# ─────────────────────────────────────────────────────────────────────

[[ -f "$PYTHON" ]]     || { echo "ERREUR : .venv introuvable — lancez : python -m venv .venv && .venv/bin/pip install -r requirements-api.txt"; exit 1; }
command -v docker      > /dev/null || { echo "ERREUR : docker non trouvé"; exit 1; }
command -v docker compose > /dev/null 2>&1 || docker compose version > /dev/null || { echo "ERREUR : docker compose non trouvé"; exit 1; }

ok "Python   : $($PYTHON --version)"
ok "Docker   : $(docker --version | cut -d' ' -f3 | tr -d ',')"
ok "Streamlit: $($STREAMLIT version 2>/dev/null | head -1)"

# ─────────────────────────────────────────────────────────────────────
step "1 / DOCKER STACK — Prometheus · AlertManager · Metrics API · InfluxDB"
# ─────────────────────────────────────────────────────────────────────

docker compose up -d prometheus alertmanager influxdb metrics-api \
    > "$LOG_DIR/docker.log" 2>&1
ok "docker compose up -d"

info "metrics-api démarre lentement (pip install au 1er démarrage)…"
wait_http "http://localhost:8090/api/v1/health" "metrics-api:8090" 180
wait_http "http://localhost:9090/-/ready"       "prometheus:9090"   60
wait_http "http://localhost:9093/-/ready"       "alertmanager:9093" 60

# Résumé santé
$PYTHON - <<'EOF'
import urllib.request, json
def get(url):
    try:
        with urllib.request.urlopen(url, timeout=5) as r:
            return json.load(r)
    except Exception as e:
        return {"error": str(e)}

h = get("http://localhost:8090/api/v1/health")
a = get("http://localhost:8090/api/v1/alerts/summary")
print(f"  ·  metrics en base  : {h.get('total_metrics', '?')}")
print(f"  ·  alertes en base  : {a.get('total', '?')}  (unack={a.get('unacknowledged','?')})")
EOF

# ─────────────────────────────────────────────────────────────────────
step "2 / INJECTION D'ANOMALIES — clean_sales · ingest_sales"
# ─────────────────────────────────────────────────────────────────────

$PYTHON inject_to_db.py \
    --pipeline clean_sales \
    --types nulls,out_of_range \
    --rate 0.15 --seed 42 \
    2>&1 | grep -E "✅|alertes|points écrits|Injection"

$PYTHON inject_to_db.py \
    --pipeline ingest_sales \
    --types duplicates \
    --rate 0.10 --seed 99 \
    2>&1 | grep -E "✅|alertes|points écrits|Injection"

# ─────────────────────────────────────────────────────────────────────
step "3 / DÉTECTION D'ANOMALIES — API REST"
# ─────────────────────────────────────────────────────────────────────

# Détection par seuil (règles fixes)
$PYTHON - <<'EOF'
import urllib.request, json
req = urllib.request.Request(
    "http://localhost:8090/api/v1/alerts/detect",
    data=b"", method="POST",
    headers={"Content-Type": "application/json"}
)
with urllib.request.urlopen(req, timeout=15) as r:
    d = json.load(r)
print(f"  ✔  Détection seuil  : {d['detected']} anomalies  → {d['saved']} sauvegardées")
print(f"  ·  Par sévérité     : {d['by_severity']}")
EOF

# Résumé alertes courant
$PYTHON - <<'EOF'
import urllib.request, json
with urllib.request.urlopen("http://localhost:8090/api/v1/alerts/summary", timeout=5) as r:
    d = json.load(r)
print(f"  ✔  Total alertes    : {d['total']}   (unack={d['unacknowledged']})")
for row in d['by_severity']:
    print(f"     {row['severity']:10} : {row['count']}")
EOF

# ─────────────────────────────────────────────────────────────────────
step "4 / MÉTRIQUES PROMETHEUS — exposition format texte"
# ─────────────────────────────────────────────────────────────────────

$PYTHON - <<'EOF'
import urllib.request
with urllib.request.urlopen("http://localhost:8090/metrics", timeout=5) as r:
    lines = r.read().decode().splitlines()
meta  = [l for l in lines if l.startswith("#")]
data  = [l for l in lines if l and not l.startswith("#")]
print(f"  ✔  {len(meta)} lignes méta (#HELP/#TYPE) + {len(data)} séries")
print()
for l in lines[:12]:
    print(f"  {l}")
print("  …")
EOF

# Targets Prometheus
$PYTHON - <<'EOF'
import urllib.request, json
with urllib.request.urlopen("http://localhost:9090/api/v1/targets", timeout=5) as r:
    targets = json.load(r)["data"]["activeTargets"]
print(f"\n  Prometheus targets ({len(targets)}) :")
for t in targets:
    print(f"  ✔  {t['labels'].get('job','?'):30} health={t['health']}")
EOF

# ─────────────────────────────────────────────────────────────────────
step "5 / ALERTMANAGER — alertes actives"
# ─────────────────────────────────────────────────────────────────────

$PYTHON - <<'EOF'
import urllib.request, json
with urllib.request.urlopen("http://localhost:9093/api/v2/alerts", timeout=5) as r:
    alerts = json.load(r)
print(f"  ✔  {len(alerts)} alerte(s) active(s) dans AlertManager")
for a in alerts[:5]:
    lbl = a.get("labels", {})
    print(f"     [{lbl.get('severity','?'):8}] {lbl.get('alertname','?')} — pipeline={lbl.get('pipeline','?')}")
if len(alerts) > 5:
    print(f"     … et {len(alerts)-5} autre(s)")
EOF

# ─────────────────────────────────────────────────────────────────────
step "6 / WEBHOOK ALERTMANAGER → SQLite (test end-to-end)"
# ─────────────────────────────────────────────────────────────────────

$PYTHON - <<'EOF'
import urllib.request, json, time

payload = json.dumps({"alerts": [{
    "labels":      {"alertname": "DemoE2E", "severity": "critical",
                    "pipeline": "demo_pipeline", "metric": "rows_output"},
    "annotations": {"description": "Alerte de démo bout-en-bout"},
    "status":      "firing"
}]}).encode()

req = urllib.request.Request(
    "http://localhost:8090/api/v1/alerts/webhook",
    data=payload, method="POST",
    headers={"Content-Type": "application/json"}
)
with urllib.request.urlopen(req, timeout=10) as r:
    d = json.load(r)
print(f"  ✔  Webhook reçu : {d['received']} alerte — persisted={d['persisted']}")
EOF

# ─────────────────────────────────────────────────────────────────────
step "7 / DEMO S14 — Alerting, Incidents, Runbooks"
# ─────────────────────────────────────────────────────────────────────

$PYTHON livrable_semaine14.py 2>&1

# ─────────────────────────────────────────────────────────────────────
step "8 / DEMO S15 — Intégration end-to-end (5 scénarios)"
# ─────────────────────────────────────────────────────────────────────

$PYTHON livrable_semaine15.py 2>&1

# ─────────────────────────────────────────────────────────────────────
step "9 / DASHBOARD HTML — génération et ouverture"
# ─────────────────────────────────────────────────────────────────────

$PYTHON spark/metrics/dashboard.py \
    --db spark/data/metrics.db \
    --out spark/data/dashboard.html 2>&1 | grep -E "généré|erreur"

open spark/data/dashboard.html 2>/dev/null || xdg-open spark/data/dashboard.html 2>/dev/null || true
ok "Dashboard HTML : spark/data/dashboard.html"

# ─────────────────────────────────────────────────────────────────────
step "10 / DASHBOARD STREAMLIT — lancement"
# ─────────────────────────────────────────────────────────────────────

# Tuer un éventuel process existant sur 8501
lsof -ti:8501 | xargs kill -9 2>/dev/null || true
sleep 1

$STREAMLIT run dashboard_agent.py \
    --server.port 8501 \
    --server.headless true \
    --server.address 0.0.0.0 \
    > "$LOG_DIR/streamlit.log" 2>&1 &
STREAMLIT_PID=$!
echo "$STREAMLIT_PID" > "$LOG_DIR/streamlit.pid"

wait_http "http://localhost:8501/" "streamlit:8501"
open http://localhost:8501 2>/dev/null || xdg-open http://localhost:8501 2>/dev/null || true
ok "Streamlit PID=$STREAMLIT_PID"

# ─────────────────────────────────────────────────────────────────────
hr
echo ""
echo "  DATA TRUST AGENT — DÉMO COMPLÈTE"
echo ""
echo "  Interface                 URL"
echo "  ─────────────────────     ──────────────────────────────"
echo "  Dashboard Streamlit       http://localhost:8501"
echo "  Metrics API (docs)        http://localhost:8090/docs"
echo "  Prometheus                http://localhost:9090"
echo "  AlertManager              http://localhost:9093"
echo "  InfluxDB                  http://localhost:8086"
echo ""
echo "  Logs Streamlit  : $LOG_DIR/streamlit.log"
echo "  Logs Docker     : $LOG_DIR/docker.log"
echo ""
echo "  Pour arrêter Streamlit   : kill \$(cat $LOG_DIR/streamlit.pid)"
echo "  Pour arrêter Docker      : docker compose down"
hr
echo ""
