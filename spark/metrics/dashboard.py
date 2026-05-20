"""
Générateur de tableau de bord HTML — Data Trust Agent.

Usage :
    python spark/metrics/dashboard.py
    python spark/metrics/dashboard.py --db spark/data/metrics.db --out spark/data/dashboard.html
    python spark/metrics/dashboard.py --open
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import webbrowser
from datetime import datetime, timezone
from html import escape

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

try:
    from .storage          import SQLiteMetricsStore, DEFAULT_DB
    from .alert_manager    import AlertManager
    from .pipeline_adapter import build_pipeline_snapshots
except ImportError:
    from spark.metrics.storage          import SQLiteMetricsStore, DEFAULT_DB
    from spark.metrics.alert_manager    import AlertManager
    from spark.metrics.pipeline_adapter import build_pipeline_snapshots

DEFAULT_OUT = os.path.join(BASE_DIR, "spark", "data", "dashboard.html")


# ── Data helpers ──────────────────────────────────────────────────────────────

def _parse_dbt_models(rows: list[dict]) -> dict:
    """Return {model_name: {duration, success, rows, ts}} from latest values."""
    models: dict = {}
    for row in sorted(rows, key=lambda r: r.get("ts", ""), reverse=True):
        parts = row.get("name", "").rsplit(".", 1)
        if len(parts) != 2:
            continue
        model, metric = parts
        m = models.setdefault(model, {"duration": None, "success": None, "rows": None, "ts": None})
        if metric == "model_duration_seconds" and m["duration"] is None:
            m["duration"] = row["value"]
            m["ts"] = str(row.get("ts", ""))[:19].replace("T", " ")
        elif metric == "model_success" and m["success"] is None:
            m["success"] = row["value"]
        elif metric == "rows_affected" and m["rows"] is None:
            m["rows"] = row["value"]
    return models


# ── HTML pieces ───────────────────────────────────────────────────────────────

_CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
       background: #0f1117; color: #e2e8f0; min-height: 100vh; }

header { background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
         border-bottom: 1px solid #334155; padding: 20px 32px;
         display: flex; justify-content: space-between; align-items: center; }
header h1 { font-size: 1.3rem; font-weight: 700; color: #f1f5f9; }
header h1 span { color: #38bdf8; }
.gen-ts { font-size: .8rem; color: #64748b; }

main { max-width: 1200px; margin: 0 auto; padding: 28px 20px; }

.kpi-row { display: grid; grid-template-columns: repeat(auto-fit, minmax(155px, 1fr));
           gap: 14px; margin-bottom: 32px; }
.kpi { background: #1e293b; border-radius: 12px; padding: 18px 20px;
       border: 1px solid #334155; position: relative; overflow: hidden; }
.kpi::before { content: ''; position: absolute; top: 0; left: 0; right: 0;
               height: 3px; background: var(--accent, #38bdf8); }
.kpi.green { --accent: #22c55e; }
.kpi.red   { --accent: #ef4444; }
.kpi.amber { --accent: #f59e0b; }
.kpi.blue  { --accent: #38bdf8; }
.kpi .num  { font-size: 2.2rem; font-weight: 800; line-height: 1; color: #f1f5f9; }
.kpi .lbl  { font-size: .72rem; color: #64748b; text-transform: uppercase;
             letter-spacing: .6px; margin-top: 6px; }

.section { background: #1e293b; border-radius: 12px; border: 1px solid #334155;
           padding: 22px 24px; margin-bottom: 24px; }
.section h2 { font-size: .85rem; font-weight: 600; color: #94a3b8;
              text-transform: uppercase; letter-spacing: .6px; margin-bottom: 18px; }

.model-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(210px, 1fr));
              gap: 12px; margin-bottom: 24px; }
.model-card { background: #0f172a; border-radius: 10px; padding: 16px;
              border-left: 4px solid var(--mc, #475569); }
.model-card.ok   { --mc: #22c55e; }
.model-card.fail { --mc: #ef4444; }
.model-name { font-weight: 600; font-size: .9rem; color: #e2e8f0;
              margin-bottom: 8px; word-break: break-all; }
.mbadge { display: inline-block; font-size: .7rem; font-weight: 700;
          padding: 2px 8px; border-radius: 20px; letter-spacing: .5px; }
.mbadge.ok   { background: #14532d; color: #4ade80; }
.mbadge.fail { background: #450a0a; color: #f87171; }
.mstats { margin-top: 10px; font-size: .8rem; color: #64748b;
          display: flex; flex-direction: column; gap: 3px; }
.mstats span { color: #94a3b8; }

.charts-row { display: grid; grid-template-columns: 3fr 2fr; gap: 16px; }
.chart-box { background: #0f172a; border-radius: 10px; padding: 18px;
             border: 1px solid #1e293b; }
.chart-box h3 { font-size: .75rem; color: #64748b; margin-bottom: 14px;
                text-transform: uppercase; letter-spacing: .5px; }

table { width: 100%; border-collapse: collapse; font-size: .85rem; }
th { padding: 10px 14px; text-align: left; font-size: .72rem; color: #64748b;
     text-transform: uppercase; letter-spacing: .5px; border-bottom: 1px solid #334155; }
td { padding: 10px 14px; border-bottom: 1px solid #1e293b; color: #cbd5e1; }
tr:last-child td { border-bottom: none; }
tr:hover td { background: #0f172a; }

.tag { display: inline-block; padding: 2px 8px; border-radius: 20px;
       font-size: .72rem; font-weight: 600; }
.tag-dbt     { background: #1e3a5f; color: #60a5fa; }
.tag-spark   { background: #3b1f0a; color: #fb923c; }
.tag-airflow { background: #1a2e1a; color: #4ade80; }
.tag-other   { background: #2d1f3d; color: #c084fc; }

.sev { display: inline-block; padding: 2px 8px; border-radius: 20px;
       font-size: .72rem; font-weight: 700; }
.sev.critical { background: #e74c3c; color: #fff; }
.sev.warning  { background: #431407; color: #fb923c; }
.sev.info     { background: #0c1a3b; color: #60a5fa; }

.empty { color: #475569; font-style: italic; text-align: center;
         padding: 28px; font-size: .9rem; }

/* ML pipeline cards */
.pipeline-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(220px,1fr));
                 gap: 12px; }
.pipeline-card { background: #0f172a; border-radius: 10px; padding: 16px;
                 border-left: 4px solid var(--pc, #475569); }
.pipeline-card.ok       { --pc: #22c55e; }
.pipeline-card.critical { --pc: #e74c3c; }
.pipeline-card.warning  { --pc: #f59e0b; }
.pipeline-card.info     { --pc: #38bdf8; }
.pipeline-name { font-weight: 700; font-size: .9rem; color: #e2e8f0; margin-bottom: 8px; }
.pipeline-stat { font-size: .78rem; color: #64748b; margin-top: 3px; }
.pipeline-stat span { color: #94a3b8; }
.llm-explanation { margin-top: 10px; font-size: .75rem; color: #94a3b8; line-height: 1.45;
                   border-top: 1px solid #1e293b; padding-top: 8px; font-style: italic; }

@media(max-width: 750px) { .charts-row { grid-template-columns: 1fr; } }
"""


def _kpi(num: int | str, label: str, cls: str = "blue") -> str:
    return (f'<div class="kpi {cls}">'
            f'<div class="num">{escape(str(num))}</div>'
            f'<div class="lbl">{escape(label)}</div></div>')


def _source_tag(source: str) -> str:
    cls = f"tag-{source}" if source in ("dbt", "spark", "airflow") else "tag-other"
    return f'<span class="tag {cls}">{escape(source)}</span>'


def _sev_badge(sev: str) -> str:
    return f'<span class="sev {escape(sev)}">{escape(sev.upper())}</span>'


def _model_cards(models: dict) -> str:
    if not models:
        return '<p class="empty">Aucun modèle dbt trouvé. Lancez dbt run puis python extractors/dbt_extractor.py</p>'
    cards = []
    for name in sorted(models):
        m    = models[name]
        ok   = m["success"] == 1.0
        fail = m["success"] == 0.0
        cls  = "ok" if ok else "fail" if fail else ""
        badge_cls = "ok" if ok else "fail" if fail else ""
        badge_lbl = "SUCCESS" if ok else "FAILED" if fail else "UNKNOWN"
        dur  = f"{m['duration']:.3f}s" if m["duration"] is not None else "—"
        rows = f"{int(m['rows']):,}" if m["rows"] else "—"
        ts   = m["ts"] or "—"
        cards.append(
            f'<div class="model-card {cls}">'
            f'<div class="model-name">{escape(name)}</div>'
            f'<span class="mbadge {badge_cls}">{badge_lbl}</span>'
            f'<div class="mstats">'
            f'<span>Duration: {dur}</span>'
            f'<span>Rows: {rows}</span>'
            f'<span style="color:#475569">{ts}</span>'
            f'</div></div>'
        )
    return "\n".join(cards)


def _metrics_table(summary: list[dict]) -> str:
    if not summary:
        return '<p class="empty">Aucune métrique enregistrée.</p>'
    rows = []
    for r in summary:
        val  = f"{r['last_value']:.6g}" if r["last_value"] is not None else "—"
        avg  = f"{r['avg_value']:.4g}"  if r.get("avg_value")  is not None else "—"
        mn   = f"{r['min_value']:.4g}"  if r.get("min_value")  is not None else "—"
        mx   = f"{r['max_value']:.4g}"  if r.get("max_value")  is not None else "—"
        ts   = str(r.get("last_ts", ""))[:19].replace("T", " ")
        rows.append(
            f"<tr>"
            f"<td>{_source_tag(r['source'])}</td>"
            f"<td style='font-family:monospace;font-size:.82rem'>{escape(r['name'])}</td>"
            f"<td style='text-align:right'>{int(r['count'])}</td>"
            f"<td style='text-align:right;font-weight:700;color:#f1f5f9'>{escape(val)}</td>"
            f"<td style='text-align:right;color:#64748b'>{escape(avg)}</td>"
            f"<td style='text-align:right;color:#64748b'>{escape(mn)}</td>"
            f"<td style='text-align:right;color:#64748b'>{escape(mx)}</td>"
            f"<td style='color:#475569;white-space:nowrap'>{escape(ts)}</td>"
            f"</tr>"
        )
    return (
        "<table><tr>"
        "<th>Source</th><th>Métrique</th><th>N</th>"
        "<th>Dernière</th><th>Moy.</th><th>Min</th><th>Max</th><th>Horodatage</th>"
        "</tr>" + "".join(rows) + "</table>"
    )


def _alerts_table(alerts: list[dict]) -> str:
    if not alerts:
        return '<p class="empty">✅ Aucune alerte récente.</p>'
    rows = []
    for a in alerts:
        ts      = str(a.get("ts", ""))[:19].replace("T", " ")
        ack     = "✅" if a.get("acknowledged") else "⏳"
        details = escape(str(a.get("details", ""))[:90])
        val     = f"{a.get('value', 0):.4g}"
        rows.append(
            f"<tr>"
            f"<td>{_sev_badge(a['severity'])}</td>"
            f"<td>{_source_tag(a['source'])}</td>"
            f"<td style='font-family:monospace;font-size:.82rem'>{escape(a['metric_name'])}</td>"
            f"<td>{escape(a['algorithm'])}</td>"
            f"<td style='text-align:right'>{val}</td>"
            f"<td style='color:#64748b;font-size:.8rem'>{details}</td>"
            f"<td style='color:#475569;white-space:nowrap'>{escape(ts)}</td>"
            f"<td style='text-align:center'>{ack}</td>"
            f"</tr>"
        )
    return (
        "<table><tr>"
        "<th>Sévérité</th><th>Source</th><th>Métrique</th><th>Algo</th>"
        "<th>Valeur</th><th>Détails</th><th>Horodatage</th><th>ACK</th>"
        "</tr>" + "".join(rows) + "</table>"
    )


def _ml_pipeline_cards(snapshots: dict, ml_alerts: list[dict]) -> str:
    """Renders one card per pipeline with ML status and key metrics."""
    if not snapshots:
        return '<p class="empty">Aucun pipeline detecté dans le store. Lancez run_collector.py d\'abord.</p>'

    # Count ML alerts per pipeline
    alerts_by_pipeline: dict = {}
    worst_by_pipeline: dict  = {}
    for a in ml_alerts:
        src = a.get("source", "")
        alerts_by_pipeline[src] = alerts_by_pipeline.get(src, 0) + 1
        sev = a.get("severity", "info")
        cur = worst_by_pipeline.get(src, "info")
        if sev == "critical" or (sev == "warning" and cur == "info"):
            worst_by_pipeline[src] = sev

    cards = []
    for pipeline_name, (current, history) in sorted(snapshots.items()):
        n_alerts = alerts_by_pipeline.get(pipeline_name, 0)
        worst    = worst_by_pipeline.get(pipeline_name, "ok") if n_alerts else "ok"
        cls      = worst if n_alerts else "ok"

        rc  = f"{current.row_count:,}" if current.row_count is not None else "—"
        dur = f"{current.duration_seconds:.1f}s" if current.duration_seconds is not None else "—"
        ok  = ("✅ OK" if current.success else "❌ Échec") if current.success is not None else "—"
        hist_days = len(history)

        alert_badge = (
            f'<span class="sev {worst}">{n_alerts} alerte{"s" if n_alerts > 1 else ""}</span>'
            if n_alerts else
            '<span style="color:#4ade80;font-size:.75rem">✅ Sain</span>'
        )

        llm_block = ""
        if n_alerts:
            pipeline_alerts = [a for a in ml_alerts if a.get("source") == pipeline_name]
            for a in pipeline_alerts[:2]:
                tags = a.get("tags", {})
                if isinstance(tags, str):
                    import json as _json
                    try:
                        tags = _json.loads(tags)
                    except Exception:
                        tags = {}
                expl = tags.get("llm_explanation", "")
                bk   = tags.get("llm_backend", "")
                if expl:
                    bk_label = f' <span style="font-size:.7rem;color:#94a3b8">[{escape(bk)}]</span>' if bk else ""
                    llm_block += (
                        f'<div class="llm-explanation">'
                        f'💬{bk_label} {escape(expl[:250])}'
                        f'</div>'
                    )

        cards.append(
            f'<div class="pipeline-card {cls}">'
            f'<div class="pipeline-name">{escape(pipeline_name)}</div>'
            f'{alert_badge}'
            f'<div class="pipeline-stat"><span>Lignes en sortie :</span> {rc}</div>'
            f'<div class="pipeline-stat"><span>Durée :</span> {dur}</div>'
            f'<div class="pipeline-stat"><span>Statut :</span> {ok}</div>'
            f'<div class="pipeline-stat"><span>Historique :</span> {hist_days} jour(s)</div>'
            f'{llm_block}'
            f'</div>'
        )
    return f'<div class="pipeline-grid">{"".join(cards)}</div>'


# ── Main generator ────────────────────────────────────────────────────────────

def generate(db_path: str = DEFAULT_DB, out_path: str = DEFAULT_OUT) -> str:
    store     = SQLiteMetricsStore(db_path=db_path)
    mgr       = AlertManager(db_path=db_path)

    total_pts  = store.count()
    summary    = store.summary()
    sources    = store.sources()
    alert_sum  = mgr.summary()
    alerts     = mgr.query(limit=50)
    ml_alerts  = mgr.query(algorithm="ml:%", limit=100) if hasattr(mgr, "_query_like") else [
        a for a in mgr.query(limit=200) if str(a.get("algorithm", "")).startswith("ml:")
    ]
    dbt_rows   = store.query(source="dbt", limit=2000)
    pipeline_snapshots = build_pipeline_snapshots(store)

    n_critical = next((r["count"] for r in alert_sum["by_severity"] if r["severity"] == "critical"), 0)
    n_warning  = next((r["count"] for r in alert_sum["by_severity"] if r["severity"] == "warning"),  0)
    n_unack    = alert_sum["unacknowledged"]
    generated  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    dbt_models  = _parse_dbt_models(dbt_rows)
    sorted_mdls = sorted(dbt_models)
    n_ok   = sum(1 for m in dbt_models.values() if m["success"] == 1.0)
    n_fail = sum(1 for m in dbt_models.values() if m["success"] == 0.0)

    # Chart.js data
    chart_labels    = json.dumps(sorted_mdls)
    chart_durations = json.dumps([round(dbt_models[m]["duration"] or 0, 4) for m in sorted_mdls])
    chart_colors    = json.dumps([
        "#22c55e" if dbt_models[m]["success"] == 1.0
        else "#ef4444" if dbt_models[m]["success"] == 0.0
        else "#64748b"
        for m in sorted_mdls
    ])
    donut_data   = json.dumps([n_ok, n_fail, len(dbt_models) - n_ok - n_fail])
    donut_colors = json.dumps(["#22c55e", "#ef4444", "#64748b"])

    html = f"""<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Data Trust — Pipeline Dashboard</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>{_CSS}</style>
</head>
<body>

<header>
  <div>
    <h1>Data Trust <span>Pipeline Dashboard</span></h1>
    <div class="gen-ts">Généré le {escape(generated)} &nbsp;·&nbsp; {escape(db_path)}</div>
  </div>
</header>

<main>

<!-- KPI row -->
<div class="kpi-row">
  {_kpi(total_pts,    "Points collectés",    "blue")}
  {_kpi(len(sources), "Sources actives",     "blue")}
  {_kpi(n_ok,         "Modèles OK",          "green" if n_ok   else "blue")}
  {_kpi(n_fail,       "Modèles en échec",    "red"   if n_fail else "green")}
  {_kpi(n_critical,   "Alertes critiques",   "red"   if n_critical else "green")}
  {_kpi(n_warning,    "Warnings",            "amber" if n_warning  else "green")}
  {_kpi(n_unack,      "Non traitées",        "amber" if n_unack    else "green")}
</div>

<!-- dbt section -->
<div class="section">
  <h2>🔷 dbt Pipeline — statut des modèles</h2>
  <div class="model-grid">
    {_model_cards(dbt_models)}
  </div>

  {'<div class="charts-row">' if dbt_models else ''}
  {'<div class="chart-box"><h3>Durée d&apos;exécution par modèle (s)</h3><canvas id="durChart"></canvas></div>' if dbt_models else ''}
  {'<div class="chart-box"><h3>Taux de succès</h3><canvas id="donutChart"></canvas></div>' if dbt_models else ''}
  {'</div>' if dbt_models else ''}
</div>

<!-- All metrics table -->
<div class="section">
  <h2>📊 Métriques — dernière valeur par série</h2>
  {_metrics_table(summary)}
</div>

<!-- ML Pipelines -->
<div class="section">
  <h2>🤖 Pipelines — Analyse ML (Semaine 10)</h2>
  {_ml_pipeline_cards(pipeline_snapshots, ml_alerts)}
</div>

<!-- Alerts -->
<div class="section">
  <h2>🚨 Alertes récentes (50 dernières)</h2>
  {_alerts_table(alerts)}
</div>

</main>

<script>
const labels    = {chart_labels};
const durations = {chart_durations};
const colors    = {chart_colors};

if (labels.length > 0) {{
  new Chart(document.getElementById('durChart'), {{
    type: 'bar',
    data: {{
      labels,
      datasets: [{{
        label: 'Durée (s)',
        data: durations,
        backgroundColor: colors,
        borderRadius: 6,
      }}]
    }},
    options: {{
      indexAxis: 'y',
      responsive: true,
      plugins: {{ legend: {{ display: false }} }},
      scales: {{
        x: {{
          ticks: {{ color: '#64748b' }},
          grid:  {{ color: '#1e293b' }},
        }},
        y: {{
          ticks: {{ color: '#94a3b8' }},
          grid:  {{ color: '#1e293b' }},
        }}
      }}
    }}
  }});

  new Chart(document.getElementById('donutChart'), {{
    type: 'doughnut',
    data: {{
      labels: ['Success', 'Failed', 'Unknown'],
      datasets: [{{
        data: {donut_data},
        backgroundColor: {donut_colors},
        borderWidth: 0,
        hoverOffset: 6,
      }}]
    }},
    options: {{
      responsive: true,
      cutout: '65%',
      plugins: {{
        legend: {{
          position: 'bottom',
          labels: {{ color: '#94a3b8', padding: 16, boxWidth: 12 }}
        }}
      }}
    }}
  }});
}}
</script>
</body>
</html>"""

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    return os.path.abspath(out_path)


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Génère le tableau de bord HTML Data Trust")
    parser.add_argument("--db",   default=DEFAULT_DB,  help="Chemin vers metrics.db")
    parser.add_argument("--out",  default=DEFAULT_OUT, help="Fichier HTML de sortie")
    parser.add_argument("--open", action="store_true", help="Ouvre dans le navigateur")
    args = parser.parse_args()

    path = generate(db_path=args.db, out_path=args.out)
    print(f"[dashboard] Dashboard généré : {path}")
    if args.open:
        webbrowser.open(f"file:///{path.replace(os.sep, '/')}")


if __name__ == "__main__":
    main()
