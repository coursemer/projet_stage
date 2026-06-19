"""
Dashboard — Data Trust Agent  (Semaine 13)

Démarrage :
    source airflow_env/bin/activate
    streamlit run dashboard_agent.py

Pages :
  1. Vue d'ensemble  — santé des 3 pipelines
  2. Anomalies       — détail avec explications LLM + incidents
  3. Trends          — graphiques temporels + search sémantique
  4. 🧪 Injection    — injection contrôlée + détection en temps réel
  5. Règles          — approbation / rejet des tests générés
"""
from __future__ import annotations

import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import streamlit as st

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from spark.anomaly_injector                             import AnomalyInjector, ANOMALY_CATALOG
from spark.catalog.catalog                              import DataCatalog
from spark.catalog.lineage                              import LineageParser
from spark.catalog.models                               import CatalogEntry, Incident
from spark.catalog.rule_store                           import RuleStore
from spark.catalog.search                               import SemanticSearch
from spark.metrics.anomaly_detection.anomaly_detector   import AnomalyDetector
from spark.metrics.anomaly_detection.models             import PipelineMetrics
from spark.metrics.llm_explainer                        import LLMExplainer
from spark.metrics.validation                           import TestGenerator

# ── Chemins ───────────────────────────────────────────────────────────────────
CATALOG_DB   = os.path.join(BASE_DIR, "spark", "data", "catalog.db")
RULES_DB     = os.path.join(BASE_DIR, "spark", "data", "rules.db")
RAW_SALES    = os.path.join(BASE_DIR, "spark", "data", "raw", "sales.csv")
DBT_MANIFEST = os.path.join(BASE_DIR, "dbt", "target", "manifest.json")
JOBS_DIR     = os.path.join(BASE_DIR, "spark", "jobs")

PIPELINES  = ["ingest_sales", "clean_sales", "aggregate_sales"]
SEV_COLOR  = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡", "LOW": "🔵", "none": "🟢"}

PIPELINE_CSV = {
    "ingest_sales":    RAW_SALES,
    "clean_sales":     RAW_SALES,
    "aggregate_sales": RAW_SALES,
}


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

def _pipeline_desc(name: str) -> str:
    descs = {
        "ingest_sales":    "Ingestion quotidienne des ventes brutes depuis la source.",
        "clean_sales":     "Nettoyage et validation des ventes avec Pandera + Spark.",
        "aggregate_sales": "Agrégation journalière des métriques de ventes par région.",
    }
    return descs.get(name, f"Pipeline {name}.")


def _df_to_pipeline_metrics(
    df: pd.DataFrame,
    pipeline: str,
    duration: float = 30.0,
    task_failures: int = 0,
) -> PipelineMetrics:
    """Calcule un PipelineMetrics depuis un DataFrame pandas."""
    col_stats: Dict[str, Dict[str, float]] = {}
    for col in df.columns:
        series = df[col]
        null_rate = float(series.isna().mean())
        if pd.api.types.is_numeric_dtype(series):
            col_stats[col] = {
                "mean":      float(series.mean()) if not series.empty else 0.0,
                "std":       float(series.std())  if len(series) > 1 else 0.0,
                "null_rate": null_rate,
                "min":       float(series.min())  if not series.empty else 0.0,
                "max":       float(series.max())  if not series.empty else 0.0,
            }
        else:
            col_stats[col] = {"null_rate": null_rate}

    schema = {col: str(df[col].dtype) for col in df.columns}

    return PipelineMetrics(
        pipeline_name=pipeline,
        row_count=len(df),
        duration_seconds=duration,
        success=True,
        task_failures=task_failures,
        column_stats=col_stats,
        schema_snapshot=schema,
    )


def _build_history(pipeline: str, base_rows: int, n: int = 30) -> List[PipelineMetrics]:
    rng = random.Random(42)
    return [
        PipelineMetrics(
            pipeline_name=pipeline,
            row_count=int(base_rows + rng.gauss(0, base_rows * 0.02)),
            duration_seconds=30.0 + rng.gauss(0, 2.0),
            success=True, task_failures=0,
        )
        for _ in range(n)
    ]


# ══════════════════════════════════════════════════════════════════════════════
# Cache / ressources
# ══════════════════════════════════════════════════════════════════════════════

@st.cache_resource
def get_catalog():
    return DataCatalog(db_path=CATALOG_DB)


@st.cache_resource
def get_rule_store():
    return RuleStore(db_path=RULES_DB)


@st.cache_resource
def get_explainer():
    return LLMExplainer()


@st.cache_data(ttl=60)
def load_pipeline_data():
    catalog = get_catalog()
    rng     = random.Random(42)
    result  = {}

    scenarios = {
        "ingest_sales":    {"rows": 50_000, "duration": 45.0, "anom_rows": None,  "dur_anom": None},
        "clean_sales":     {"rows": 48_000, "duration": 28.0, "anom_rows": 3_840, "dur_anom": None},
        "aggregate_sales": {"rows": 47_000, "duration": 60.0, "anom_rows": None,  "dur_anom": 540.0},
    }

    for pipeline, cfg in scenarios.items():
        history = _build_history(pipeline, cfg["rows"])
        current = PipelineMetrics(
            pipeline_name=pipeline,
            row_count=cfg["anom_rows"] if cfg["anom_rows"] else cfg["rows"],
            duration_seconds=cfg["dur_anom"] if cfg["dur_anom"] else cfg["duration"],
            success=True,
            task_failures=2 if pipeline == "aggregate_sales" else 0,
        )

        detector   = AnomalyDetector.for_pipeline(pipeline, sla_seconds=120.0, min_history=7, ml_min_train=20)
        det_result = detector.detect(current, history)
        anomalies  = det_result.anomalies
        if anomalies:
            get_explainer().enrich_anomalies(anomalies)

        score = max(0.0, round(1.0 - len(anomalies) * 0.08, 2))
        catalog.publish(CatalogEntry(
            name=pipeline, type="pipeline",
            description=_pipeline_desc(pipeline),
            owner="data-team", tags=["spark", "sales"],
            quality_score=score,
        ))
        for a in anomalies:
            if a.severity in ("HIGH", "CRITICAL"):
                catalog.record_incident(Incident(
                    entry_name=pipeline, severity=str(a.severity),
                    description=a.description, anomaly_type=str(a.level),
                ))

        result[pipeline] = {
            "current":   current,
            "history":   history,
            "anomalies": anomalies,
            "score":     score,
            "n_anomaly": len(anomalies),
            "severity":  str(det_result.worst_severity.value) if det_result.worst_severity else "none",
        }
    return result


@st.cache_data(ttl=120)
def load_rules():
    store = get_rule_store()
    rng   = random.Random(42)
    for pipeline in PIPELINES:
        history = _build_history(pipeline, 50_000)
        rules   = TestGenerator(sigma_factor=3.0).generate(history)
        store.save_rules(rules, overwrite=False)
    return store.list_rules()


@st.cache_data
def load_raw_csv(path: str, nrows: int = 50_000) -> pd.DataFrame:
    return pd.read_csv(path, nrows=nrows)


# ══════════════════════════════════════════════════════════════════════════════
# Layout
# ══════════════════════════════════════════════════════════════════════════════

st.set_page_config(
    page_title="Data Trust Agent",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

with st.sidebar:
    st.title("🛡️ Data Trust Agent")
    st.caption("Phase 4 — Semaine 13")
    st.divider()
    page = st.radio(
        "Navigation",
        ["Vue d'ensemble", "Anomalies", "Trends", "🧪 Injection", "Règles"],
        index=0,
    )
    st.divider()
    st.caption(f"🕒 {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    if st.button("🔄 Actualiser", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

data = load_pipeline_data()


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — Vue d'ensemble
# ══════════════════════════════════════════════════════════════════════════════
if page == "Vue d'ensemble":
    st.title("Vue d'ensemble — Santé des pipelines")

    col1, col2, col3, col4 = st.columns(4)
    total_anomalies = sum(d["n_anomaly"] for d in data.values())
    open_incidents  = get_catalog().incident_summary().get("open", 0)
    healthy         = sum(1 for d in data.values() if d["n_anomaly"] == 0)

    col1.metric("Pipelines surveillés", len(PIPELINES))
    col2.metric("Anomalies détectées",  total_anomalies,
                delta=f"+{total_anomalies}" if total_anomalies else None, delta_color="inverse")
    col3.metric("Incidents ouverts",    open_incidents, delta_color="inverse")
    col4.metric("Pipelines sains",      f"{healthy}/{len(PIPELINES)}")

    st.divider()
    st.subheader("Statut par pipeline")
    for pipeline, d in data.items():
        icon = SEV_COLOR.get(d["severity"], "🟢")
        with st.expander(f"{icon} **{pipeline}** — score : {d['score']:.0%} | {d['n_anomaly']} anomalie(s)", expanded=True):
            c1, c2, c3 = st.columns(3)
            c1.metric("Lignes",        f"{d['current'].row_count:,}")
            c2.metric("Durée (s)",     f"{d['current'].duration_seconds:.1f}s")
            c3.metric("Score qualité", f"{d['score']:.0%}")
            if d["anomalies"]:
                st.warning(f"⚠️ {d['n_anomaly']} anomalie(s) — pire sévérité : **{d['severity']}**")
            else:
                st.success("✅ Pipeline sain")

    st.divider()
    st.subheader("Catalog")
    entries = get_catalog().list_entries()
    if entries:
        df_cat = pd.DataFrame([{
            "Nom": e.name, "Type": e.type,
            "Qualité": f"{e.quality_score:.0%}" if e.quality_score else "—",
            "Tags": ", ".join(e.tags),
            "Description": (e.description[:55] + "…") if len(e.description) > 55 else e.description,
        } for e in entries])
        st.dataframe(df_cat, use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("Lineage dbt")
    if os.path.exists(DBT_MANIFEST):
        graph    = LineageParser().from_dbt(DBT_MANIFEST)
        st.caption(f"{len(graph.nodes)} nœuds · {len(graph.edges)} arêtes")
        selected = st.selectbox("Explorer un nœud", sorted(n.name for n in graph.nodes))
        c1, c2 = st.columns(2)
        c1.write("**Upstream**");   c1.write(graph.ancestors(selected)   or "_aucune_")
        c2.write("**Downstream**"); c2.write(graph.descendants(selected) or "_aucun_")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — Anomalies
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Anomalies":
    st.title("Anomalies détectées")
    pipeline_sel = st.selectbox("Pipeline", PIPELINES)
    d = data[pipeline_sel]

    if not d["anomalies"]:
        st.success(f"✅ **{pipeline_sel}** — aucune anomalie.")
    else:
        st.error(f"🔴 **{pipeline_sel}** — {len(d['anomalies'])} anomalie(s) · pire sévérité : {d['severity']}")
        for i, a in enumerate(d["anomalies"]):
            icon = SEV_COLOR.get(str(a.severity), "🔵")
            with st.expander(f"{icon} [{a.level.upper()}] {a.metric_name} — {a.severity} (score={a.severity_score:.1f})", expanded=(i == 0)):
                c1, c2, c3 = st.columns(3)
                c1.metric("Observé",  f"{a.observed_value:.4g}" if a.observed_value is not None else "—")
                c2.metric("Attendu",  f"{a.expected_value:.4g}" if a.expected_value is not None else "—")
                c3.metric("Score",    f"{a.severity_score:.1f}/100")
                st.write(f"**Description :** {a.description}")
                expl = a.context.get("llm_explanation", "")
                back = a.context.get("llm_backend", "template")
                if expl:
                    st.info(f"💬 **Explication** _(via {back})_\n\n{expl}")

    st.divider()
    st.subheader(f"Incidents — {pipeline_sel}")
    incs = get_catalog().incidents(pipeline_sel)
    if incs:
        df_inc = pd.DataFrame([{
            "ID": i.id, "Sévérité": i.severity, "Type": i.anomaly_type,
            "Description": i.description[:80],
            "Résolu": "✅" if i.resolved else "❌",
            "Date": i.created_at.strftime("%Y-%m-%d %H:%M"),
        } for i in incs])
        st.dataframe(df_inc, use_container_width=True, hide_index=True)
        open_incs = [i for i in incs if not i.resolved]
        if open_incs:
            sel = st.selectbox("Résoudre", [f"#{i.id} — {i.description[:50]}" for i in open_incs])
            if st.button("Marquer comme résolu"):
                get_catalog().resolve_incident(int(sel.split("#")[1].split(" ")[0]))
                st.cache_data.clear(); st.rerun()
    else:
        st.info("Aucun incident enregistré.")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — Trends
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Trends":
    st.title("Historique & Trends")
    pipeline_sel = st.selectbox("Pipeline", PIPELINES)
    d            = data[pipeline_sel]
    metric_sel   = st.selectbox("Métrique", ["row_count", "duration_seconds"])

    history = d["history"]
    base    = datetime.now(timezone.utc) - timedelta(days=len(history))
    ts_list = [base + timedelta(days=i) for i in range(len(history))]
    values  = [getattr(h, metric_sel) for h in history]

    df_hist = pd.DataFrame({"date": pd.to_datetime(ts_list), metric_sel: values})
    st.line_chart(df_hist.set_index("date"), use_container_width=True)

    mean_val  = sum(values) / len(values)
    std_val   = (sum((v - mean_val) ** 2 for v in values) / len(values)) ** 0.5
    cur_val   = getattr(d["current"], metric_sel)
    sigma3_hi = mean_val + 3 * std_val
    sigma3_lo = mean_val - 3 * std_val

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Valeur actuelle",    f"{cur_val:,.0f}")
    c2.metric("Moyenne historique", f"{mean_val:,.0f}")
    c3.metric("Écart-type",         f"{std_val:,.1f}")
    c4.metric("Seuil ±3σ",          f"[{sigma3_lo:,.0f}, {sigma3_hi:,.0f}]")

    if cur_val > sigma3_hi or cur_val < sigma3_lo:
        st.error(f"⚠️ Valeur actuelle hors des bornes ±3σ")
    else:
        st.success("✅ Valeur dans la plage normale (±3σ)")

    st.divider()
    st.subheader("🔍 Recherche sémantique")
    q = st.text_input("Requête", placeholder="ex: pipeline nettoyage ventes")
    if q:
        s = SemanticSearch(use_embeddings=False)
        s.index(get_catalog().list_entries())
        for r in s.query(q, top_k=5):
            st.write(f"**{r.entry.name}** _(score={r.score:.3f})_ — {r.entry.description}")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — Injection
# ══════════════════════════════════════════════════════════════════════════════
elif page == "🧪 Injection":
    st.title("🧪 Injection contrôlée d'anomalies")
    st.caption("Injecte des anomalies dans les données CSV réelles et observe la détection en temps réel.")

    # ── Paramètres ────────────────────────────────────────────────────────────
    col_left, col_right = st.columns([1, 2])

    with col_left:
        st.subheader("Paramètres")
        pipeline_sel = st.selectbox("Pipeline cible", PIPELINES)
        rate         = st.slider("Taux d'injection", 0.01, 0.30, 0.05, 0.01,
                                 format="%.0f%%", help="Proportion de lignes affectées")
        seed         = st.number_input("Graine aléatoire", 0, 9999, 42)

        st.markdown("**Types d'anomalies à injecter**")
        selected_types = []
        for atype, info in ANOMALY_CATALOG.items():
            if st.checkbox(atype, key=f"chk_{atype}", help=info["description"]):
                selected_types.append(atype)

        inject_btn = st.button("🚀 Lancer l'injection", type="primary", use_container_width=True,
                               disabled=len(selected_types) == 0)

    with col_right:
        if not os.path.exists(RAW_SALES):
            st.error(f"Fichier source introuvable : {RAW_SALES}")
            st.stop()

        st.subheader("Données source")
        df_clean = load_raw_csv(RAW_SALES, nrows=50_000)
        c1, c2, c3 = st.columns(3)
        c1.metric("Lignes (clean)",  f"{len(df_clean):,}")
        c2.metric("Colonnes",        len(df_clean.columns))
        c3.metric("Taux nuls global", f"{df_clean.isna().mean().mean():.2%}")
        st.dataframe(df_clean.head(5), use_container_width=True, hide_index=True)

    # ── Injection ─────────────────────────────────────────────────────────────
    if inject_btn and selected_types:
        st.divider()
        st.subheader("Résultats de l'injection")

        with st.spinner(f"Injection de {', '.join(selected_types)} @ {rate:.0%}…"):
            t0       = time.perf_counter()
            injector = AnomalyInjector(seed=int(seed), injection_rate=rate)
            df_dirty, report = injector.inject(df_clean.copy(), anomaly_types=selected_types)
            elapsed  = time.perf_counter() - t0

        # ── Rapport injection ─────────────────────────────────────────────
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Lignes injectées",  f"{len(df_dirty):,}")
        total_injected = sum(r["injected_count"] for r in report.get("injections", []))
        c2.metric("Anomalies injectées", f"{total_injected:,}")
        c3.metric("Taux nuls après",
                  f"{df_dirty.isna().mean().mean():.2%}",
                  delta=f"+{(df_dirty.isna().mean().mean() - df_clean.isna().mean().mean()):.2%}")
        c4.metric("Temps injection",    f"{elapsed:.2f}s")

        # Détail par type
        if report.get("injections"):
            df_rep = pd.DataFrame([{
                "Type":     r["type"],
                "Injectées": r["injected_count"],
                "Colonnes":  ", ".join(r.get("columns", [])),
            } for r in report["injections"]])
            st.dataframe(df_rep, use_container_width=True, hide_index=True)

        # Aperçu avant/après sur les colonnes numériques
        st.divider()
        st.subheader("Comparaison avant / après")
        num_cols = df_clean.select_dtypes(include="number").columns.tolist()
        if num_cols:
            col_sel = st.selectbox("Colonne numérique", num_cols)
            df_cmp = pd.DataFrame({
                "clean":  df_clean[col_sel].dropna().sample(min(500, len(df_clean)), random_state=42).values,
                "dirty":  pd.to_numeric(df_dirty[col_sel], errors="coerce").dropna()
                           .sample(min(500, len(df_dirty)), random_state=42).values,
            })
            st.bar_chart(df_cmp.describe().loc[["mean", "std", "min", "max"]].T,
                         use_container_width=True)

        # ── Détection sur les données injectées ───────────────────────────
        st.divider()
        st.subheader("🔍 Détection d'anomalies sur les données injectées")

        with st.spinner("Calcul des métriques et détection…"):
            current_metrics = _df_to_pipeline_metrics(
                df_dirty, pipeline_sel,
                duration=30.0 + len(selected_types) * 5.0,
                task_failures=len(selected_types),
            )
            history_metrics = _build_history(pipeline_sel, len(df_clean))

            detector   = AnomalyDetector.for_pipeline(
                pipeline_sel, sla_seconds=120.0, min_history=7, ml_min_train=20
            )
            det_result = detector.detect(current_metrics, history_metrics)
            anomalies  = det_result.anomalies
            if anomalies:
                get_explainer().enrich_anomalies(anomalies)

        if not anomalies:
            st.success("✅ Aucune anomalie détectée après injection.")
            st.info("💡 Essayez un taux d'injection plus élevé ou plusieurs types combinés.")
        else:
            worst = det_result.worst_severity.value if det_result.worst_severity else "?"
            icon  = SEV_COLOR.get(worst, "🔴")
            st.error(f"{icon} **{len(anomalies)} anomalie(s) détectée(s)** — pire sévérité : **{worst}**")

            for i, a in enumerate(anomalies):
                sev_icon = SEV_COLOR.get(str(a.severity), "🔵")
                with st.expander(
                    f"{sev_icon} [{a.level.upper()}] {a.metric_name} — {a.severity} (score={a.severity_score:.1f})",
                    expanded=(i < 3),
                ):
                    c1, c2, c3 = st.columns(3)
                    c1.metric("Observé",  f"{a.observed_value:.4g}" if a.observed_value is not None else "—")
                    c2.metric("Attendu",  f"{a.expected_value:.4g}" if a.expected_value is not None else "—")
                    c3.metric("Score",    f"{a.severity_score:.1f}/100")
                    st.write(f"**Description :** {a.description}")
                    expl = a.context.get("llm_explanation", "")
                    back = a.context.get("llm_backend", "template")
                    if expl:
                        st.info(f"💬 **Explication** _(via {back})_\n\n{expl}")

        # Résumé
        summary = det_result.summary()
        st.divider()
        col_s1, col_s2 = st.columns(2)
        col_s1.metric("Anomalies injectées", total_injected)
        col_s2.metric("Anomalies détectées", len(anomalies))

        df_sum = pd.DataFrame([
            {"Niveau": lvl, "Détectées": cnt}
            for lvl, cnt in summary["by_level"].items() if cnt > 0
        ])
        if not df_sum.empty:
            st.bar_chart(df_sum.set_index("Niveau"), use_container_width=True)

    elif not selected_types:
        st.info("👆 Sélectionne au moins un type d'anomalie, puis clique sur **Lancer l'injection**.")


# ══════════════════════════════════════════════════════════════════════════════
# PAGE 5 — Règles
# ══════════════════════════════════════════════════════════════════════════════
elif page == "Règles":
    st.title("Approbation des règles générées")
    rule_store = get_rule_store()
    load_rules()

    summary = rule_store.summary()
    c1, c2, c3 = st.columns(3)
    c1.metric("En attente", summary.get("pending",  0))
    c2.metric("Approuvées", summary.get("approved", 0))
    c3.metric("Rejetées",   summary.get("rejected", 0))

    st.divider()
    col_f1, col_f2 = st.columns(2)
    pipeline_f = col_f1.selectbox("Pipeline", ["Tous"] + PIPELINES)
    status_f   = col_f2.selectbox("Statut",   ["Tous", "pending", "approved", "rejected"])

    filtered = rule_store.list_rules(
        pipeline_name=None if pipeline_f == "Tous" else pipeline_f,
        status=None       if status_f   == "Tous" else status_f,
    )

    if not filtered:
        st.info("Aucune règle correspondant aux filtres.")
    else:
        df_r = pd.DataFrame([{
            "ID":          r["rule_id"],
            "Pipeline":    r["pipeline_name"],
            "Métrique":    r["metric_name"],
            "Borne basse": r["threshold_low"],
            "Borne haute": r["threshold_high"],
            "σ":           r["sigma_factor"],
            "Confiance":   f"{r['confidence']:.0%}",
            "Statut":      r["status"],
            "FP":          r["fp_count"],
        } for r in filtered])
        st.dataframe(df_r, use_container_width=True, hide_index=True)

        st.divider()
        st.subheader("Action")
        rule_sel = st.selectbox("Règle", [r["rule_id"] for r in filtered])
        col_a, col_r, col_rst = st.columns(3)
        if col_a.button("✅ Approuver", use_container_width=True):
            rule_store.approve(rule_sel)
            st.success(f"Approuvée : **{rule_sel}**")
            st.cache_data.clear(); st.rerun()
        note = st.text_input("Note de rejet")
        if col_r.button("❌ Rejeter", use_container_width=True):
            rule_store.reject(rule_sel, note=note)
            st.error(f"Rejetée : **{rule_sel}**")
            st.cache_data.clear(); st.rerun()
        if col_rst.button("🔄 Pending", use_container_width=True):
            rule_store.reset(rule_sel)
            st.cache_data.clear(); st.rerun()
