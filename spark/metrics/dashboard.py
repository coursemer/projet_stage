"""
Générateur de tableau de bord HTML — Data Trust Agent.

Lit directement depuis SQLite (metrics + alerts) et produit un fichier
HTML autonome (aucune dépendance JS externe) affichant :
  - Résumé des métriques par source
  - Alertes récentes avec sévérité colorée
  - Compteurs globaux

Usage :
    python spark/metrics/dashboard.py
    python spark/metrics/dashboard.py --db spark/data/metrics.db --out spark/data/dashboard.html
    python spark/metrics/dashboard.py --open   # ouvre dans le navigateur après génération
"""

from __future__ import annotations

import argparse
import os
import sys
import webbrowser
from datetime import datetime, timezone
from html import escape

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

try:
    from .storage       import SQLiteMetricsStore, DEFAULT_DB
    from .alert_manager import AlertManager
except ImportError:
    from spark.metrics.storage       import SQLiteMetricsStore, DEFAULT_DB
    from spark.metrics.alert_manager import AlertManager

DEFAULT_OUT = os.path.join(BASE_DIR, "spark", "data", "dashboard.html")

SEV_COLOR = {
    "critical": "#e74c3c",
    "warning":  "#f39c12",
    "info":     "#3498db",
}
SEV_BG = {
    "critical": "#fdecea",
    "warning":  "#fef9e7",
    "info":     "#eaf4fb",
}


# ── HTML helpers ──────────────────────────────────────────────────────────────

def _html_head(title: str) -> str:
    return f"""<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{escape(title)}</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         margin: 0; background: #f5f6fa; color: #2c3e50; }}
  header {{ background: #2c3e50; color: #fff; padding: 18px 32px; }}
  header h1 {{ margin: 0; font-size: 1.4rem; }}
  header p  {{ margin: 4px 0 0; opacity: .7; font-size: .85rem; }}
  main {{ max-width: 1100px; margin: 28px auto; padding: 0 20px; }}
  .cards {{ display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 28px; }}
  .card {{ background: #fff; border-radius: 10px; padding: 18px 24px;
           flex: 1; min-width: 160px; box-shadow: 0 1px 4px rgba(0,0,0,.08); }}
  .card .num {{ font-size: 2rem; font-weight: 700; }}
  .card .lbl {{ font-size: .8rem; color: #7f8c8d; text-transform: uppercase; letter-spacing:.5px }}
  .card.crit {{ border-top: 4px solid #e74c3c; }}
  .card.warn {{ border-top: 4px solid #f39c12; }}
  .card.ok   {{ border-top: 4px solid #2ecc71; }}
  .card.info {{ border-top: 4px solid #3498db; }}
  section {{ background: #fff; border-radius: 10px; padding: 20px 24px;
             margin-bottom: 24px; box-shadow: 0 1px 4px rgba(0,0,0,.08); }}
  section h2 {{ margin: 0 0 16px; font-size: 1rem; color: #34495e; }}
  table {{ width: 100%; border-collapse: collapse; font-size: .88rem; }}
  th {{ background: #f0f2f5; text-align: left; padding: 8px 12px;
        font-weight: 600; color: #555; }}
  td {{ padding: 7px 12px; border-bottom: 1px solid #ecf0f1; }}
  tr:last-child td {{ border-bottom: none; }}
  .badge {{ display: inline-block; padding: 2px 8px; border-radius: 12px;
            font-size: .78rem; font-weight: 600; }}
  .empty {{ color: #bdc3c7; font-style: italic; text-align: center; padding: 20px; }}
</style>
</head>
<body>
"""


def _card(num: int | str, label: str, cls: str = "info") -> str:
    return (
        f'<div class="card {cls}">'
        f'<div class="num">{escape(str(num))}</div>'
        f'<div class="lbl">{escape(label)}</div>'
        f'</div>'
    )


def _badge(severity: str) -> str:
    color = SEV_COLOR.get(severity, "#95a5a6")
    bg    = SEV_BG.get(severity, "#f8f9fa")
    return (
        f'<span class="badge" style="color:{color};background:{bg}">'
        f'{escape(severity.upper())}</span>'
    )


# ── Génération principale ─────────────────────────────────────────────────────

def generate(db_path: str = DEFAULT_DB, out_path: str = DEFAULT_OUT) -> str:
    """Génère le HTML et l'écrit dans out_path. Retourne le chemin absolu."""
    store   = SQLiteMetricsStore(db_path=db_path)
    mgr     = AlertManager(db_path=db_path)

    total_pts  = store.count()
    summary    = store.summary()
    sources    = store.sources()

    alert_sum  = mgr.summary()
    alerts     = mgr.query(limit=50)

    n_critical = next((r["count"] for r in alert_sum["by_severity"] if r["severity"] == "critical"), 0)
    n_warning  = next((r["count"] for r in alert_sum["by_severity"] if r["severity"] == "warning"),  0)
    n_unack    = alert_sum["unacknowledged"]
    generated  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    html_parts: list[str] = [
        _html_head("Data Trust — Dashboard"),
        f'<header><h1>Data Trust Dashboard</h1>'
        f'<p>Généré le {escape(generated)} · DB : {escape(db_path)}</p></header>',
        "<main>",
        # ── Cartes compteurs ──
        '<div class="cards">',
        _card(total_pts,    "Points collectés", "ok"),
        _card(len(sources), "Sources actives",  "info"),
        _card(n_critical,   "Critiques (non ack)", "crit"),
        _card(n_warning,    "Warnings (non ack)",  "warn"),
        _card(n_unack,      "Alertes non traitées", "warn" if n_unack else "ok"),
        "</div>",
    ]

    # ── Tableau des métriques ─────────────────────────────────────────────────
    html_parts.append('<section><h2>Métriques — dernière valeur par série</h2>')
    if summary:
        html_parts.append(
            "<table><tr>"
            "<th>Source</th><th>Métrique</th><th>N points</th>"
            "<th>Dernière valeur</th><th>Dernière collecte</th>"
            "</tr>"
        )
        for r in summary:
            val_str = f"{r['last_value']:.6g}" if r["last_value"] is not None else "—"
            ts_str  = str(r["last_ts"])[:19].replace("T", " ")
            html_parts.append(
                f"<tr>"
                f"<td><b>{escape(r['source'])}</b></td>"
                f"<td>{escape(r['name'])}</td>"
                f"<td style='text-align:right'>{int(r['count'])}</td>"
                f"<td style='text-align:right'><b>{escape(val_str)}</b></td>"
                f"<td style='color:#7f8c8d'>{escape(ts_str)}</td>"
                f"</tr>"
            )
        html_parts.append("</table>")
    else:
        html_parts.append('<p class="empty">Aucune métrique enregistrée.</p>')
    html_parts.append("</section>")

    # ── Tableau des alertes ───────────────────────────────────────────────────
    html_parts.append('<section><h2>Alertes récentes (50 dernières)</h2>')
    if alerts:
        html_parts.append(
            "<table><tr>"
            "<th>Sévérité</th><th>Source / Métrique</th><th>Algorithme</th>"
            "<th>Valeur</th><th>Détails</th><th>Horodatage</th><th>Traité</th>"
            "</tr>"
        )
        for a in alerts:
            ts_str   = str(a["ts"])[:19].replace("T", " ")
            ack_str  = "✅" if a.get("acknowledged") else "⏳"
            sev      = a["severity"]
            row_bg   = SEV_BG.get(sev, "#fff")
            val_str  = f"{a.get('value', 0):.4g}"
            details  = escape(str(a.get("details", ""))[:80])
            html_parts.append(
                f"<tr style='background:{row_bg}'>"
                f"<td>{_badge(sev)}</td>"
                f"<td><b>{escape(a['source'])}</b> / {escape(a['metric_name'])}</td>"
                f"<td>{escape(a['algorithm'])}</td>"
                f"<td style='text-align:right'>{val_str}</td>"
                f"<td style='color:#555'>{details}</td>"
                f"<td style='color:#7f8c8d;white-space:nowrap'>{escape(ts_str)}</td>"
                f"<td style='text-align:center'>{ack_str}</td>"
                f"</tr>"
            )
        html_parts.append("</table>")
    else:
        html_parts.append('<p class="empty">✅ Aucune alerte récente.</p>')
    html_parts.append("</section>")

    html_parts.append("</main></body></html>")

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(html_parts))

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
        webbrowser.open(f"file://{path}")


if __name__ == "__main__":
    main()
