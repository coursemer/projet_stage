"""
Tests — Semaine 13 : Dashboard & API REST

T1  : rule_store_save_list         — save + list avec filtres
T2  : rule_store_approve_reject    — workflow approbation/rejet
T3  : rule_store_reject_fp         — reject incrémente fp_count
T4  : rule_store_summary           — résumé pending/approved/rejected
T5  : api_catalog_publish_get      — POST /catalog + GET /catalog/{name}
T6  : api_catalog_list_filter      — GET /catalog?type=pipeline
T7  : api_catalog_incidents        — POST + GET /catalog/{name}/incidents
T8  : api_catalog_search           — GET /catalog/search?q=...
T9  : api_catalog_lineage          — GET /catalog/{name}/lineage
T10 : api_rules_generate           — POST /rules/generate/{pipeline}
T11 : api_rules_list               — GET /rules + filtres
T12 : api_rules_approve_reject     — POST /rules/{id}/approve + reject
T13 : api_rules_summary            — GET /rules/summary
T14 : api_incidents_summary        — GET /catalog/incidents/summary
T15 : end_to_end_s13               — workflow complet catalog + règles
"""
from __future__ import annotations

import os
import sys
import tempfile

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from fastapi.testclient import TestClient
from fastapi import FastAPI

from spark.catalog.catalog    import DataCatalog
from spark.catalog.models     import CatalogEntry, Incident
from spark.catalog.rule_store import RuleStore
from spark.metrics.validation import TestGenerator
from spark.metrics.anomaly_detection.models import PipelineMetrics

results: dict[str, str] = {}

DBT_MANIFEST = os.path.join(BASE_DIR, "dbt", "target", "manifest.json")


def _history(pipeline: str, n: int = 20):
    import random
    rng = random.Random(42)
    return [
        PipelineMetrics(
            pipeline_name=pipeline,
            row_count=int(50_000 + rng.gauss(0, 1000)),
            duration_seconds=30.0 + rng.gauss(0, 2.0),
            success=True, task_failures=0,
        )
        for _ in range(n)
    ]


def _make_api(tmp_dir: str) -> TestClient:
    from spark.catalog.api_catalog import create_catalog_router
    app    = FastAPI()
    router = create_catalog_router(
        catalog_db=os.path.join(tmp_dir, "catalog.db"),
        rules_db=os.path.join(tmp_dir, "rules.db"),
    )
    app.include_router(router)
    return TestClient(app)


# ══════════════════════════════════════════════════════════════════════════════
# T1 — RuleStore save + list
# ══════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("T1 — rule_store_save_list")
print("=" * 60)
try:
    with tempfile.TemporaryDirectory() as tmp:
        store = RuleStore(db_path=os.path.join(tmp, "rules.db"))
        rules = TestGenerator(sigma_factor=3.0).generate(_history("ingest_sales"))
        n = store.save_rules(rules)
        assert n >= 3
        assert store.count() == n

        all_r = store.list_rules()
        assert len(all_r) == n

        ingest = store.list_rules(pipeline_name="ingest_sales")
        assert len(ingest) == n

        pending = store.list_rules(status="pending")
        assert len(pending) == n

    print(f"  {n} règles sauvegardées + listées  ✓")
    results["T1-rule_store_save_list"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T1-rule_store_save_list"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T2 — approve / reject
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T2 — rule_store_approve_reject")
print("=" * 60)
try:
    with tempfile.TemporaryDirectory() as tmp:
        store = RuleStore(db_path=os.path.join(tmp, "rules.db"))
        rules = TestGenerator().generate(_history("clean_sales"))
        store.save_rules(rules)

        first_id = rules[0].rule_id
        second_id = rules[1].rule_id

        assert store.approve(first_id)
        assert store.reject(second_id, note="Trop strict")
        assert not store.approve("unknown_id")

        assert store.count("approved") == 1
        assert store.count("rejected") == 1
        assert store.count("pending")  == len(rules) - 2

        # reset
        assert store.reset(first_id)
        assert store.count("approved") == 0

    print(f"  approve/reject/reset  ✓")
    results["T2-rule_store_approve_reject"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T2-rule_store_approve_reject"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T3 — reject incrémente fp_count
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T3 — rule_store_reject_fp")
print("=" * 60)
try:
    with tempfile.TemporaryDirectory() as tmp:
        store = RuleStore(db_path=os.path.join(tmp, "rules.db"))
        rules = TestGenerator().generate(_history("agg_sales"))
        store.save_rules(rules)
        rid   = rules[0].rule_id

        store.reject(rid)
        store.reset(rid)
        store.reject(rid)   # 2 rejets → fp_count=2

        r = store.get(rid)
        assert r is not None
        assert r.fp_count == 2, f"fp_count={r.fp_count} attendu 2"

    print("  2 rejets → fp_count=2  ✓")
    results["T3-rule_store_reject_fp"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T3-rule_store_reject_fp"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T4 — RuleStore summary
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T4 — rule_store_summary")
print("=" * 60)
try:
    with tempfile.TemporaryDirectory() as tmp:
        store = RuleStore(db_path=os.path.join(tmp, "rules.db"))
        rules = TestGenerator().generate(_history("p1"))
        store.save_rules(rules)
        store.approve(rules[0].rule_id)
        store.reject(rules[1].rule_id)

        s = store.summary()
        assert s.get("pending",  0) == len(rules) - 2
        assert s.get("approved", 0) == 1
        assert s.get("rejected", 0) == 1

    print(f"  summary={s}  ✓")
    results["T4-rule_store_summary"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T4-rule_store_summary"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T5 — API POST /catalog + GET /catalog/{name}
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T5 — api_catalog_publish_get")
print("=" * 60)
try:
    with tempfile.TemporaryDirectory() as tmp:
        client = _make_api(tmp)
        r = client.post("/api/v1/catalog", json={
            "name": "ingest_sales", "type": "pipeline",
            "description": "Ingestion des ventes.", "tags": ["spark"],
            "quality_score": 0.95,
        })
        assert r.status_code == 201

        r2 = client.get("/api/v1/catalog/ingest_sales")
        assert r2.status_code == 200
        assert r2.json()["name"] == "ingest_sales"
        assert r2.json()["quality_score"] == 0.95

        r3 = client.get("/api/v1/catalog/unknown")
        assert r3.status_code == 404

    print("  POST 201 + GET 200 + GET 404  ✓")
    results["T5-api_catalog_publish_get"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T5-api_catalog_publish_get"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T6 — GET /catalog?type=pipeline
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T6 — api_catalog_list_filter")
print("=" * 60)
try:
    with tempfile.TemporaryDirectory() as tmp:
        client = _make_api(tmp)
        for name, t in [("p1", "pipeline"), ("m1", "model"), ("p2", "pipeline")]:
            client.post("/api/v1/catalog", json={"name": name, "type": t, "quality_score": 0.9})

        r = client.get("/api/v1/catalog?type=pipeline")
        assert r.status_code == 200
        assert r.json()["count"] == 2

        r2 = client.get("/api/v1/catalog?min_quality=0.85")
        assert r2.json()["count"] == 3

    print("  filtres type + min_quality  ✓")
    results["T6-api_catalog_list_filter"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T6-api_catalog_list_filter"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T7 — Incidents via API
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T7 — api_catalog_incidents")
print("=" * 60)
try:
    with tempfile.TemporaryDirectory() as tmp:
        client = _make_api(tmp)
        client.post("/api/v1/catalog", json={"name": "clean_sales"})

        r = client.post("/api/v1/catalog/clean_sales/incidents", json={
            "severity": "HIGH", "description": "Volume drop.", "anomaly_type": "volume",
        })
        assert r.status_code == 201
        inc_id = r.json()["id"]

        r2 = client.get("/api/v1/catalog/clean_sales/incidents")
        assert r2.json()["count"] == 1

        r3 = client.post(f"/api/v1/catalog/clean_sales/incidents/{inc_id}/resolve")
        assert r3.status_code == 200

        r4 = client.get("/api/v1/catalog/clean_sales/incidents?resolved=true")
        assert r4.json()["count"] == 1

    print("  POST incident + resolve + query  ✓")
    results["T7-api_catalog_incidents"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T7-api_catalog_incidents"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T8 — Search
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T8 — api_catalog_search")
print("=" * 60)
try:
    with tempfile.TemporaryDirectory() as tmp:
        client = _make_api(tmp)
        for name, desc in [
            ("clean_sales",  "Nettoyage et validation des ventes Pandera."),
            ("stg_events",   "Staging des événements web utilisateurs."),
            ("inventory",    "Historisation SCD2 inventaire produits."),
        ]:
            client.post("/api/v1/catalog", json={"name": name, "description": desc})

        r = client.get("/api/v1/catalog/search?q=nettoyage ventes")
        assert r.status_code == 200
        data = r.json()
        assert data["results"][0]["name"] == "clean_sales"

    print(f"  search 'nettoyage ventes' → {data['results'][0]['name']} (score={data['results'][0]['score']})  ✓")
    results["T8-api_catalog_search"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T8-api_catalog_search"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T9 — Lineage via API
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T9 — api_catalog_lineage")
print("=" * 60)
try:
    with tempfile.TemporaryDirectory() as tmp:
        client = _make_api(tmp)
        r = client.get("/api/v1/catalog/sales_summary/lineage")
        assert r.status_code == 200
        data = r.json()
        assert "int_sales_daily" in data["upstream"]

    print(f"  upstream(sales_summary)={data['upstream']}  ✓")
    results["T9-api_catalog_lineage"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T9-api_catalog_lineage"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T10 — POST /rules/generate/{pipeline}
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T10 — api_rules_generate")
print("=" * 60)
try:
    with tempfile.TemporaryDirectory() as tmp:
        client = _make_api(tmp)
        r = client.post("/api/v1/rules/generate/ingest_sales")
        assert r.status_code == 201
        data = r.json()
        assert data["pipeline"] == "ingest_sales"
        assert data["generated"] >= 3
        assert data["saved"] >= 3

    print(f"  {data['generated']} règles générées et sauvegardées  ✓")
    results["T10-api_rules_generate"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T10-api_rules_generate"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T11 — GET /rules
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T11 — api_rules_list")
print("=" * 60)
try:
    with tempfile.TemporaryDirectory() as tmp:
        client = _make_api(tmp)
        client.post("/api/v1/rules/generate/ingest_sales")
        client.post("/api/v1/rules/generate/clean_sales")

        r = client.get("/api/v1/rules")
        assert r.status_code == 200
        total = r.json()["count"]
        assert total >= 6

        r2 = client.get("/api/v1/rules?pipeline=ingest_sales")
        assert r2.json()["count"] >= 3

        r3 = client.get("/api/v1/rules?status=pending")
        assert r3.json()["count"] == total

    print(f"  total={total}  filtres pipeline+status  ✓")
    results["T11-api_rules_list"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T11-api_rules_list"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T12 — approve / reject via API
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T12 — api_rules_approve_reject")
print("=" * 60)
try:
    with tempfile.TemporaryDirectory() as tmp:
        client = _make_api(tmp)
        client.post("/api/v1/rules/generate/ingest_sales")
        rules = client.get("/api/v1/rules?pipeline=ingest_sales").json()["rules"]
        r1_id = rules[0]["rule_id"]
        r2_id = rules[1]["rule_id"]

        r = client.post(f"/api/v1/rules/{r1_id}/approve")
        assert r.status_code == 200
        assert r.json()["approved"] == r1_id

        r = client.post(f"/api/v1/rules/{r2_id}/reject", json={"note": "trop strict"})
        assert r.status_code == 200
        assert r.json()["rejected"] == r2_id

        r = client.post("/api/v1/rules/unknown_rule/approve")
        assert r.status_code == 404

    print(f"  approve + reject + 404  ✓")
    results["T12-api_rules_approve_reject"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T12-api_rules_approve_reject"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T13 — GET /rules/summary
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T13 — api_rules_summary")
print("=" * 60)
try:
    with tempfile.TemporaryDirectory() as tmp:
        client = _make_api(tmp)
        client.post("/api/v1/rules/generate/ingest_sales")
        rules = client.get("/api/v1/rules").json()["rules"]
        client.post(f"/api/v1/rules/{rules[0]['rule_id']}/approve")
        client.post(f"/api/v1/rules/{rules[1]['rule_id']}/reject")

        r = client.get("/api/v1/rules/summary")
        assert r.status_code == 200
        s = r.json()
        assert s.get("approved", 0) == 1
        assert s.get("rejected", 0) == 1

    print(f"  summary={s}  ✓")
    results["T13-api_rules_summary"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T13-api_rules_summary"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T14 — GET /catalog/incidents/summary
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T14 — api_incidents_summary")
print("=" * 60)
try:
    with tempfile.TemporaryDirectory() as tmp:
        client = _make_api(tmp)
        client.post("/api/v1/catalog", json={"name": "p1"})
        client.post("/api/v1/catalog/p1/incidents", json={"severity": "HIGH",     "description": "a"})
        client.post("/api/v1/catalog/p1/incidents", json={"severity": "CRITICAL", "description": "b"})

        r = client.get("/api/v1/catalog/incidents/summary")
        assert r.status_code == 200
        s = r.json()
        assert s["total"] == 2
        assert s["open"]  == 2

    print(f"  summary total=2 open=2  ✓")
    results["T14-api_incidents_summary"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T14-api_incidents_summary"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# T15 — End-to-end S13
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T15 — end_to_end_s13")
print("=" * 60)
try:
    with tempfile.TemporaryDirectory() as tmp:
        client = _make_api(tmp)

        # 1. Publier 3 pipelines
        for name, score in [("ingest_sales", 1.0), ("clean_sales", 0.72), ("aggregate_sales", 0.75)]:
            client.post("/api/v1/catalog", json={
                "name": name, "type": "pipeline",
                "quality_score": score, "tags": ["spark", "sales"],
            })

        # 2. Enregistrer incidents sur pipelines dégradés
        for name in ["clean_sales", "aggregate_sales"]:
            client.post(f"/api/v1/catalog/{name}/incidents", json={
                "severity": "HIGH", "description": "Anomalie détectée", "anomaly_type": "volume",
            })

        # 3. Vérifier le catalog
        r = client.get("/api/v1/catalog?type=pipeline")
        assert r.json()["count"] == 3

        # 4. Générer + approuver/rejeter règles pour ingest_sales
        client.post("/api/v1/rules/generate/ingest_sales")
        rules = client.get("/api/v1/rules?pipeline=ingest_sales").json()["rules"]
        assert len(rules) >= 3
        client.post(f"/api/v1/rules/{rules[0]['rule_id']}/approve")
        client.post(f"/api/v1/rules/{rules[1]['rule_id']}/reject", json={"note": "seuil trop bas"})

        # 5. Vérifier le résumé
        s = client.get("/api/v1/rules/summary").json()
        assert s.get("approved", 0) >= 1
        assert s.get("rejected", 0) >= 1

        # 6. Search
        r = client.get("/api/v1/catalog/search?q=nettoyage validation")
        assert r.status_code == 200

        # 7. Lineage
        r = client.get("/api/v1/catalog/sales_summary/lineage")
        assert r.status_code == 200

        # 8. Incidents summary
        r = client.get("/api/v1/catalog/incidents/summary")
        assert r.json()["total"] == 2

        print(f"  3 pipelines publiés  ✓")
        print(f"  règles générées + approuvées/rejetées : {s}  ✓")
        print(f"  incidents={r.json()['total']}  ✓")
        print(f"  search + lineage OK  ✓")

    results["T15-end_to_end_s13"] = "OK"
except Exception:
    import traceback; traceback.print_exc()
    results["T15-end_to_end_s13"] = "FAILED"


# ══════════════════════════════════════════════════════════════════════════════
# RÉSUMÉ
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("RÉSUMÉ — Semaine 13")
print("=" * 60)
all_ok = True
for label, status in results.items():
    icon = "✅" if status == "OK" else "❌"
    print(f"  {icon}  {label:<44} {status}")
    if status != "OK":
        all_ok = False
print("=" * 60)
print("RÉSULTAT GLOBAL :", "✅ TOUS OK" if all_ok else "❌ ÉCHECS DÉTECTÉS")
