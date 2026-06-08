"""
Tests — Semaine 12 : Data Catalog enrichi

T1  : catalog_publish_get          — publish + get aller-retour
T2  : catalog_upsert               — republier met à jour sans doublon
T3  : catalog_list_filters         — filtre par type, tag, quality_score
T4  : catalog_quality_score        — update_quality_score
T5  : catalog_incidents            — record + query + resolve
T6  : catalog_incident_summary     — résumé des incidents
T7  : catalog_from_detection       — publish_from_detection auto
T8  : lineage_dbt                  — parse manifest.json réel
T9  : lineage_spark_jobs           — parse Spark jobs dir
T10 : lineage_merge                — fusion dbt + Spark
T11 : lineage_ancestors_descendants — navigation amont/aval
T12 : search_tfidf_basic           — requête TF-IDF simple
T13 : search_tfidf_ranking         — le résultat le plus proche est en tête
T14 : search_reindex               — réindexer efface l'ancien index
T15 : end_to_end_catalog           — catalog complet : publish → lineage → search
"""
from __future__ import annotations

import os
import sys
import tempfile

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from spark.catalog import (
    CatalogEntry, DataCatalog, Incident, LineageParser, SemanticSearch,
)

results: dict[str, str] = {}

DBT_MANIFEST  = os.path.join(BASE_DIR, "dbt", "target", "manifest.json")
SPARK_JOBS_DIR = os.path.join(BASE_DIR, "spark", "jobs")


def _catalog(tmp_dir: str) -> DataCatalog:
    return DataCatalog(db_path=os.path.join(tmp_dir, "catalog.db"))


# ══════════════════════════════════════════════════════════════════════════════
# T1 — publish + get
# ══════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("T1 — catalog_publish_get")
print("=" * 60)
try:
    with tempfile.TemporaryDirectory() as tmp:
        cat = _catalog(tmp)
        cat.publish(CatalogEntry(
            name="ingest_sales",
            type="pipeline",
            description="Ingestion quotidienne des ventes.",
            owner="data-team",
            tags=["spark", "sales"],
            quality_score=0.97,
        ))
        entry = cat.get("ingest_sales")
        assert entry is not None
        assert entry.name == "ingest_sales"
        assert entry.type == "pipeline"
        assert entry.quality_score == 0.97
        assert "spark" in entry.tags
        assert cat.get("unknown") is None

    print("  publish + get  ✓")
    results["T1-catalog_publish_get"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T1-catalog_publish_get"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T2 — upsert
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T2 — catalog_upsert")
print("=" * 60)
try:
    with tempfile.TemporaryDirectory() as tmp:
        cat = _catalog(tmp)
        cat.publish(CatalogEntry(name="clean_sales", description="v1"))
        cat.publish(CatalogEntry(name="clean_sales", description="v2"))
        assert cat.count() == 1
        assert cat.get("clean_sales").description == "v2"

    print("  upsert → 1 seule entrée, description mise à jour  ✓")
    results["T2-catalog_upsert"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T2-catalog_upsert"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T3 — list avec filtres
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T3 — catalog_list_filters")
print("=" * 60)
try:
    with tempfile.TemporaryDirectory() as tmp:
        cat = _catalog(tmp)
        cat.publish(CatalogEntry(name="ingest_sales",    type="pipeline", tags=["spark"], quality_score=0.97))
        cat.publish(CatalogEntry(name="clean_sales",     type="pipeline", tags=["spark", "dbt"], quality_score=0.80))
        cat.publish(CatalogEntry(name="stg_sales",       type="model",    tags=["dbt"], quality_score=0.99))
        cat.publish(CatalogEntry(name="raw_sales",       type="dataset",  tags=["raw"], quality_score=0.50))

        pipelines = cat.list_entries(type="pipeline")
        assert len(pipelines) == 2, f"2 pipelines attendus, obtenu {len(pipelines)}"

        spark_entries = cat.list_entries(tag="spark")
        assert len(spark_entries) == 2

        high_quality = cat.list_entries(min_quality=0.90)
        assert len(high_quality) == 2   # ingest_sales + stg_sales

    print("  type, tag, min_quality  ✓")
    results["T3-catalog_list_filters"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T3-catalog_list_filters"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T4 — update_quality_score
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T4 — catalog_quality_score")
print("=" * 60)
try:
    with tempfile.TemporaryDirectory() as tmp:
        cat = _catalog(tmp)
        cat.publish(CatalogEntry(name="agg_sales", quality_score=1.0))
        cat.update_quality_score("agg_sales", score=0.72, n_anomalies=3)
        assert cat.get("agg_sales").quality_score == 0.72

    print("  quality_score 1.0 → 0.72  ✓")
    results["T4-catalog_quality_score"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T4-catalog_quality_score"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T5 — incidents
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T5 — catalog_incidents")
print("=" * 60)
try:
    with tempfile.TemporaryDirectory() as tmp:
        cat = _catalog(tmp)
        cat.publish(CatalogEntry(name="clean_sales"))

        id1 = cat.record_incident(Incident(
            entry_name="clean_sales", severity="HIGH",
            description="Chute volume 92%.", anomaly_type="volume",
        ))
        id2 = cat.record_incident(Incident(
            entry_name="clean_sales", severity="LOW",
            description="Null rate légèrement élevé.", anomaly_type="distribution",
        ))
        all_inc = cat.incidents("clean_sales")
        assert len(all_inc) == 2

        open_inc = cat.incidents("clean_sales", resolved=False)
        assert len(open_inc) == 2

        ok = cat.resolve_incident(id1)
        assert ok is True
        assert len(cat.incidents("clean_sales", resolved=False)) == 1
        assert len(cat.incidents("clean_sales", resolved=True))  == 1

    print(f"  2 incidents enregistrés, 1 résolu  ✓")
    results["T5-catalog_incidents"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T5-catalog_incidents"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T6 — incident_summary
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T6 — catalog_incident_summary")
print("=" * 60)
try:
    with tempfile.TemporaryDirectory() as tmp:
        cat = _catalog(tmp)
        cat.publish(CatalogEntry(name="p1"))
        cat.publish(CatalogEntry(name="p2"))
        cat.record_incident(Incident(entry_name="p1", severity="HIGH",     description="a"))
        cat.record_incident(Incident(entry_name="p1", severity="CRITICAL", description="b"))
        cat.record_incident(Incident(entry_name="p2", severity="LOW",      description="c"))
        id1 = cat.incidents()[0].id
        cat.resolve_incident(id1)

        s = cat.incident_summary()
        assert s["total"]    == 3
        assert s["open"]     == 2
        assert s["resolved"] == 1
        assert "HIGH" in s["by_severity"]

    print(f"  total=3 open=2 resolved=1  ✓")
    results["T6-catalog_incident_summary"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T6-catalog_incident_summary"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T7 — publish_from_detection
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T7 — catalog_from_detection")
print("=" * 60)
try:
    from spark.metrics.anomaly_detection.models import Anomaly, AnomalyLevel, Severity

    with tempfile.TemporaryDirectory() as tmp:
        cat = _catalog(tmp)

        anomalies = [
            Anomaly(id="a1", pipeline_name="clean_sales", level=AnomalyLevel.VOLUME,
                    severity=Severity.HIGH, description="Volume drop 92%", metric_name="row_count"),
        ]
        cat.publish_from_detection("clean_sales", n_anomalies=1, worst_severity="HIGH", anomalies=anomalies)

        entry = cat.get("clean_sales")
        assert entry is not None
        assert entry.quality_score is not None and entry.quality_score < 1.0

        incidents = cat.incidents("clean_sales")
        assert len(incidents) >= 1
        assert incidents[0].severity == "HIGH"

    print(f"  entrée auto-publiée, score<1, 1 incident  ✓")
    results["T7-catalog_from_detection"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T7-catalog_from_detection"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T8 — lineage dbt manifest
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T8 — lineage_dbt")
print("=" * 60)
try:
    assert os.path.exists(DBT_MANIFEST), f"manifest.json introuvable : {DBT_MANIFEST}"
    parser = LineageParser()
    graph  = parser.from_dbt(DBT_MANIFEST)

    assert len(graph.nodes) >= 3, f"Au moins 3 nœuds attendus, obtenu {len(graph.nodes)}"
    assert len(graph.edges) >= 2, f"Au moins 2 arêtes attendues, obtenu {len(graph.edges)}"

    # Le modèle sales_summary dépend de int_sales_daily
    ups = graph.upstream_of("sales_summary")
    assert "int_sales_daily" in ups, f"int_sales_daily non trouvé en upstream de sales_summary : {ups}"

    print(f"  {len(graph.nodes)} nœuds, {len(graph.edges)} arêtes  ✓")
    print(f"  upstream(sales_summary) = {ups}  ✓")
    results["T8-lineage_dbt"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T8-lineage_dbt"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T9 — lineage Spark jobs
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T9 — lineage_spark_jobs")
print("=" * 60)
try:
    assert os.path.isdir(SPARK_JOBS_DIR), f"Répertoire jobs introuvable : {SPARK_JOBS_DIR}"
    parser = LineageParser()
    graph  = parser.from_spark_jobs(SPARK_JOBS_DIR)

    assert len(graph.nodes) >= 3, f"Au moins 3 nœuds attendus, obtenu {len(graph.nodes)}"
    jobs = [n for n in graph.nodes if n.type == "job"]
    assert len(jobs) >= 1, "Au moins 1 job attendu"

    print(f"  {len(graph.nodes)} nœuds ({len(jobs)} jobs), {len(graph.edges)} arêtes  ✓")
    for j in jobs:
        down = graph.downstream_of(j.name)
        up   = graph.upstream_of(j.name)
        print(f"    {j.name} : reads={up}  writes={down}")
    results["T9-lineage_spark_jobs"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T9-lineage_spark_jobs"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T10 — lineage merge dbt + Spark
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T10 — lineage_merge")
print("=" * 60)
try:
    parser     = LineageParser()
    dbt_graph   = parser.from_dbt(DBT_MANIFEST)
    spark_graph = parser.from_spark_jobs(SPARK_JOBS_DIR)
    full        = dbt_graph.merge(spark_graph)

    assert len(full.nodes) >= len(dbt_graph.nodes), "La fusion doit avoir au moins autant de nœuds que dbt seul"
    assert len(full.edges) >= len(dbt_graph.edges)

    print(f"  dbt={len(dbt_graph.nodes)}n/{len(dbt_graph.edges)}e  "
          f"spark={len(spark_graph.nodes)}n/{len(spark_graph.edges)}e  "
          f"full={len(full.nodes)}n/{len(full.edges)}e  ✓")
    results["T10-lineage_merge"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T10-lineage_merge"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T11 — lineage ancestors / descendants
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T11 — lineage_ancestors_descendants")
print("=" * 60)
try:
    parser = LineageParser()
    graph  = parser.from_dbt(DBT_MANIFEST)

    # sales_summary ← int_sales_daily ← stg_sales ← (source)
    ancestors = graph.ancestors("sales_summary")
    assert "int_sales_daily" in ancestors, f"int_sales_daily pas dans ancestors : {ancestors}"
    assert "stg_sales"       in ancestors, f"stg_sales pas dans ancestors : {ancestors}"

    descendants = graph.descendants("stg_sales")
    assert "int_sales_daily" in descendants, f"int_sales_daily pas dans descendants : {descendants}"
    assert "sales_summary"   in descendants, f"sales_summary pas dans descendants : {descendants}"

    print(f"  ancestors(sales_summary)  = {ancestors}  ✓")
    print(f"  descendants(stg_sales)    = {descendants}  ✓")
    results["T11-lineage_ancestors_descendants"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T11-lineage_ancestors_descendants"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T12 — search TF-IDF basique
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T12 — search_tfidf_basic")
print("=" * 60)
try:
    entries = [
        CatalogEntry(name="ingest_sales",    description="Ingestion quotidienne des données de ventes brutes."),
        CatalogEntry(name="clean_sales",     description="Nettoyage et validation des ventes avec Pandera."),
        CatalogEntry(name="stg_events",      description="Staging des événements web utilisateur."),
        CatalogEntry(name="inventory_scd2",  description="Historisation SCD Type 2 de l'inventaire."),
    ]
    search = SemanticSearch(use_embeddings=False)
    search.index(entries)
    assert search.n_indexed == 4
    assert search.backend == "tfidf"

    results_q = search.query("nettoyage ventes", top_k=3)
    assert len(results_q) >= 1
    # clean_sales doit être en tête
    assert results_q[0].entry.name == "clean_sales", (
        f"clean_sales attendu en 1er, obtenu {results_q[0].entry.name}"
    )

    print(f"  {len(results_q)} résultats, 1er = {results_q[0].entry.name} (score={results_q[0].score})  ✓")
    results["T12-search_tfidf_basic"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T12-search_tfidf_basic"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T13 — search ranking
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T13 — search_tfidf_ranking")
print("=" * 60)
try:
    entries = [
        CatalogEntry(name="inventory_scd2", description="Historisation SCD2 inventaire produits."),
        CatalogEntry(name="clean_sales",    description="Nettoyage validation ventes Pandera Spark."),
        CatalogEntry(name="stg_events",     description="Staging événements web logs utilisateurs."),
    ]
    search = SemanticSearch(use_embeddings=False)
    search.index(entries)

    r = search.query("inventaire historisation", top_k=3)
    assert r[0].entry.name == "inventory_scd2", (
        f"inventory_scd2 attendu en 1er, obtenu {r[0].entry.name}"
    )
    # scores décroissants
    for i in range(len(r) - 1):
        assert r[i].score >= r[i+1].score

    print(f"  ranking correct, 1er={r[0].entry.name} score={r[0].score}  ✓")
    results["T13-search_tfidf_ranking"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T13-search_tfidf_ranking"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T14 — search réindexation
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T14 — search_reindex")
print("=" * 60)
try:
    search = SemanticSearch(use_embeddings=False)
    search.index([CatalogEntry(name="old_entry", description="ancien pipeline.")])
    assert search.n_indexed == 1

    search.index([
        CatalogEntry(name="new_a", description="nouveau pipeline alpha."),
        CatalogEntry(name="new_b", description="nouveau pipeline beta."),
    ])
    assert search.n_indexed == 2, f"Attendu 2 après réindex, obtenu {search.n_indexed}"
    r = search.query("pipeline beta")
    assert r[0].entry.name == "new_b"

    print(f"  réindex 1→2 entrées, requête ok  ✓")
    results["T14-search_reindex"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T14-search_reindex"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T15 — End-to-end catalog complet
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T15 — end_to_end_catalog")
print("=" * 60)
try:
    from spark.metrics.anomaly_detection.models import Anomaly, AnomalyLevel, Severity

    with tempfile.TemporaryDirectory() as tmp:
        cat    = _catalog(tmp)
        parser = LineageParser()
        search = SemanticSearch(use_embeddings=False)

        # 1. Publication automatique des 3 pipelines
        pipeline_data = [
            ("ingest_sales",    0, None,   "Ingestion des ventes brutes depuis la source."),
            ("clean_sales",     3, "HIGH", "Nettoyage et validation des ventes."),
            ("aggregate_sales", 2, "HIGH", "Agrégation journalière des métriques de ventes."),
        ]
        for name, n_anom, sev, desc in pipeline_data:
            cat.publish(CatalogEntry(name=name, type="pipeline", description=desc,
                                     tags=["spark", "sales"], quality_score=1.0))
            if n_anom > 0:
                anomalies = [
                    Anomaly(id=f"{name}_a{i}", pipeline_name=name,
                            level=AnomalyLevel.VOLUME, severity=Severity.HIGH,
                            description="Anomalie de test", metric_name="row_count")
                    for i in range(n_anom)
                ]
                cat.publish_from_detection(name, n_anom, sev, anomalies)

        assert cat.count() == 3

        # 2. Lineage dbt
        dbt_graph = parser.from_dbt(DBT_MANIFEST)
        assert len(dbt_graph.nodes) >= 3

        # 3. Search sémantique
        search.index(cat.list_entries())
        r = search.query("nettoyage validation ventes", top_k=2)
        assert r[0].entry.name == "clean_sales"

        # 4. Incidents
        inc_summary = cat.incident_summary()
        assert inc_summary["total"] >= 2   # au moins 1 par pipeline anomalie

        # 5. Score qualité dégradé sur les pipelines en anomalie
        clean_entry = cat.get("clean_sales")
        ingest_entry = cat.get("ingest_sales")
        assert clean_entry.quality_score < ingest_entry.quality_score, (
            "clean_sales (anomalie) devrait avoir un score < ingest_sales (nominal)"
        )

        print(f"  3 pipelines publiés  ✓")
        print(f"  Lineage : {len(dbt_graph.nodes)} nœuds dbt  ✓")
        print(f"  Search : requête → {r[0].entry.name} (score={r[0].score})  ✓")
        print(f"  Incidents : {inc_summary}  ✓")
        print(f"  Scores : ingest={ingest_entry.quality_score}  clean={clean_entry.quality_score}  ✓")

    results["T15-end_to_end_catalog"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T15-end_to_end_catalog"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# RÉSUMÉ
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("RÉSUMÉ — Semaine 12")
print("=" * 60)
all_ok = True
for label, status in results.items():
    icon = "✅" if status == "OK" else "❌"
    print(f"  {icon}  {label:<42} {status}")
    if status != "OK":
        all_ok = False
print("=" * 60)
print("RÉSULTAT GLOBAL :", "✅ TOUS OK" if all_ok else "❌ ÉCHECS DÉTECTÉS")
