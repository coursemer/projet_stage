"""
Tests — Semaine 8 : Détection d'anomalies sur les métriques

T1 : threshold_detection    — seuils statiques (min/max métier)
T2 : zscore_detection       — z-score sur série normale + outlier
T3 : iqr_detection          — IQR sur série asymétrique + outlier
T4 : trend_detection        — tendance croissante / décroissante
T5 : no_false_positives     — séries saines → 0 alerte
T6 : alert_manager_save     — persistance SQLite des alertes
T7 : alert_manager_query    — filtres source / severity / ack
T8 : alert_manager_ack      — acknowledge individuel + masse
T9 : detector_integration   — détection bout-en-bout depuis store
T10: api_alerts_endpoints   — routes /alerts via TestClient
"""

import json
import os
import shutil
import sqlite3
import sys
import tempfile

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from spark.metrics.storage          import MetricPoint, SQLiteMetricsStore
from spark.metrics.anomaly_detector import AnomalyDetector, AnomalyAlert
from spark.metrics.alert_manager    import AlertManager

results: dict[str, str] = {}


# ══════════════════════════════════════════════════════════════════════════════
# T1 — Seuils statiques
# ══════════════════════════════════════════════════════════════════════════════
print("=" * 60)
print("T1 — threshold_detection")
print("=" * 60)

try:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    store = SQLiteMetricsStore(db_path=db_path)

    store.write([
        MetricPoint(source="spark", name="job.rejection_rate_pct", value=25.0),  # > 20 → critical
        MetricPoint(source="dbt",   name="dbt.run.pass_rate",       value=70.0),  # < 80 → critical
    ])

    detector = AnomalyDetector(store, algorithms=["threshold"], min_history=1)
    alerts   = detector.run()

    # 25 % > max=20 → alerte, 70 % < min=80 → alerte
    thresh_alerts = [a for a in alerts if a.algorithm == "threshold"]
    assert len(thresh_alerts) >= 1, f"Au moins 1 alerte attendue, obtenu {len(thresh_alerts)}"
    sev_map = {a.metric_name: a.severity for a in thresh_alerts}
    assert sev_map.get("job.rejection_rate_pct") == "critical"

    print(f"  {len(thresh_alerts)} alerte(s) de seuil  ✓")
    for a in thresh_alerts:
        print(f"    [{a.severity}] {a.metric_name}: {a.details}")
    results["T1-threshold_detection"] = "OK"
except Exception as e:
    import traceback; traceback.print_exc()
    results["T1-threshold_detection"] = "FAILED"
finally:
    os.unlink(db_path)


# ══════════════════════════════════════════════════════════════════════════════
# T2 — Z-score
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T2 — zscore_detection")
print("=" * 60)

try:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    store = SQLiteMetricsStore(db_path=db_path)

    # Série normale autour de 10 s, puis outlier à 80 s
    normal = [MetricPoint(source="spark", name="job.duration_seconds", value=v)
              for v in [9.5, 10.2, 9.8, 10.1, 10.3, 9.9, 10.0, 10.2, 9.7, 80.0]]
    store.write(normal)

    detector = AnomalyDetector(store, zscore_thresh=3.0, algorithms=["zscore"], min_history=5)
    alerts   = detector.run()

    z_alerts = [a for a in alerts if a.algorithm == "zscore"]
    assert len(z_alerts) >= 1, f"1 alerte z-score attendue, obtenu {len(z_alerts)}"
    assert z_alerts[0].value == 80.0

    print(f"  {len(z_alerts)} alerte(s) z-score  ✓")
    print(f"    {z_alerts[0].details}")
    results["T2-zscore_detection"] = "OK"
except Exception as e:
    import traceback; traceback.print_exc()
    results["T2-zscore_detection"] = "FAILED"
finally:
    os.unlink(db_path)


# ══════════════════════════════════════════════════════════════════════════════
# T3 — IQR
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T3 — iqr_detection")
print("=" * 60)

try:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    store = SQLiteMetricsStore(db_path=db_path)

    # Distribution asymétrique : 8 valeurs entre 100-200, outlier à 5000
    values = [100, 120, 110, 130, 115, 125, 118, 122, 5000]
    store.write([MetricPoint(source="spark", name="job.rows_output", value=v)
                 for v in values])

    detector = AnomalyDetector(store, iqr_k=1.5, algorithms=["iqr"], min_history=5)
    alerts   = detector.run()

    iqr_alerts = [a for a in alerts if a.algorithm == "iqr"]
    assert len(iqr_alerts) >= 1, f"1 alerte IQR attendue, obtenu {len(iqr_alerts)}"
    assert iqr_alerts[0].value == 5000

    print(f"  {len(iqr_alerts)} alerte(s) IQR  ✓")
    print(f"    {iqr_alerts[0].details}")
    results["T3-iqr_detection"] = "OK"
except Exception as e:
    import traceback; traceback.print_exc()
    results["T3-iqr_detection"] = "FAILED"
finally:
    os.unlink(db_path)


# ══════════════════════════════════════════════════════════════════════════════
# T4 — Tendance
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T4 — trend_detection")
print("=" * 60)

try:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    store = SQLiteMetricsStore(db_path=db_path)

    # Durée qui augmente régulièrement sur 4 runs
    store.write([MetricPoint(source="spark", name="job.duration_seconds", value=v)
                 for v in [10, 12, 15, 20, 30, 50]])  # 4 hausses consécutives

    # Pass rate qui baisse régulièrement
    store.write([MetricPoint(source="dbt", name="dbt.run.pass_rate", value=v)
                 for v in [100, 95, 90, 85, 80]])     # 4 baisses consécutives

    detector = AnomalyDetector(store, trend_n=3, algorithms=["trend"], min_history=4)
    alerts   = detector.run()

    trend_alerts = [a for a in alerts if a.algorithm == "trend"]
    assert len(trend_alerts) >= 2, f"2 alertes trend attendues, obtenu {len(trend_alerts)}"
    names = {a.metric_name for a in trend_alerts}
    assert "job.duration_seconds" in names
    assert "dbt.run.pass_rate"    in names

    print(f"  {len(trend_alerts)} alerte(s) trend  ✓")
    for a in trend_alerts:
        print(f"    [{a.severity}] {a.metric_name}: {a.details}")
    results["T4-trend_detection"] = "OK"
except Exception as e:
    import traceback; traceback.print_exc()
    results["T4-trend_detection"] = "FAILED"
finally:
    os.unlink(db_path)


# ══════════════════════════════════════════════════════════════════════════════
# T5 — Aucun faux positif sur série saine
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T5 — no_false_positives")
print("=" * 60)

try:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    store = SQLiteMetricsStore(db_path=db_path)

    import random
    rng = random.Random(42)
    # Série parfaitement stable autour de 12 s ± 0.5 s
    store.write([MetricPoint(source="spark", name="job.duration_seconds", value=12 + rng.uniform(-0.5, 0.5))
                 for _ in range(20)])
    # Pass rate toujours à 100 %
    store.write([MetricPoint(source="dbt", name="dbt.run.pass_rate", value=100.0)
                 for _ in range(10)])

    thresholds_override = {
        "job.duration_seconds": {"max": 300.0, "severity": "warning"},
        "dbt.run.pass_rate":    {"min": 80.0,  "severity": "critical"},
    }
    detector = AnomalyDetector(store, thresholds=thresholds_override,
                               algorithms=["zscore", "iqr", "threshold", "trend"],
                               min_history=5)
    alerts = detector.run()

    assert len(alerts) == 0, f"0 alerte attendue sur série saine, obtenu {len(alerts)}: {alerts}"
    print(f"  0 alerte sur séries saines  ✓")
    results["T5-no_false_positives"] = "OK"
except Exception as e:
    import traceback; traceback.print_exc()
    results["T5-no_false_positives"] = "FAILED"
finally:
    os.unlink(db_path)


# ══════════════════════════════════════════════════════════════════════════════
# T6 — AlertManager : persistance
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T6 — alert_manager_save")
print("=" * 60)

try:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    mgr = AlertManager(db_path=db_path)

    assert mgr.count() == 0
    alerts_to_save = [
        AnomalyAlert(source="spark", metric_name="job.rejection_rate_pct",
                     algorithm="threshold", severity="critical",
                     value=25.0, expected="<= 20", details="test"),
        AnomalyAlert(source="airflow", metric_name="dag.duration_seconds",
                     algorithm="zscore", severity="warning",
                     value=400.0, expected="μ=30 ± 3σ=10", details="z=37"),
        AnomalyAlert(source="dbt", metric_name="dbt.run.pass_rate",
                     algorithm="threshold", severity="critical",
                     value=70.0, expected=">= 80", details="test"),
    ]
    n = mgr.save(alerts_to_save)
    assert n == 3
    assert mgr.count() == 3
    assert mgr.count(acknowledged=False) == 3

    rows = mgr.query()
    assert len(rows) == 3
    for r in rows:
        assert "id" in r and "severity" in r and "metric_name" in r
        assert isinstance(r["tags"], dict)
        assert r["acknowledged"] is False

    print(f"  {n} alertes sauvegardées  ✓")
    print(f"  Schéma des rows vérifié  ✓")
    results["T6-alert_manager_save"] = "OK"
except Exception as e:
    import traceback; traceback.print_exc()
    results["T6-alert_manager_save"] = "FAILED"
finally:
    os.unlink(db_path)


# ══════════════════════════════════════════════════════════════════════════════
# T7 — AlertManager : filtres de requête
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T7 — alert_manager_query")
print("=" * 60)

try:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    mgr = AlertManager(db_path=db_path)
    mgr.save([
        AnomalyAlert(source="spark",   metric_name="job.rejection_rate_pct",
                     algorithm="threshold", severity="critical", value=25.0, expected="<= 20"),
        AnomalyAlert(source="airflow", metric_name="dag.status",
                     algorithm="threshold", severity="critical", value=0.0,  expected=">= 1"),
        AnomalyAlert(source="spark",   metric_name="job.duration_seconds",
                     algorithm="zscore",    severity="warning",  value=200.0, expected="μ=12"),
        AnomalyAlert(source="dbt",     metric_name="dbt.run.pass_rate",
                     algorithm="trend",     severity="warning",  value=60.0,  expected=">= 80"),
    ])

    # Filtre source
    spark_alerts = mgr.query(source="spark")
    assert len(spark_alerts) == 2, f"2 attendus, obtenu {len(spark_alerts)}"

    # Filtre severity
    critical = mgr.query(severity="critical")
    assert len(critical) == 2

    # Filtre algorithm
    thresh = mgr.query(algorithm="threshold")
    assert len(thresh) == 2

    # Summary
    s = mgr.summary()
    assert s["total"] == 4
    assert s["unacknowledged"] == 4
    assert len(s["by_severity"]) == 2  # critical + warning

    print(f"  source=spark       : {len(spark_alerts)} alertes  ✓")
    print(f"  severity=critical  : {len(critical)} alertes  ✓")
    print(f"  algorithm=threshold: {len(thresh)} alertes  ✓")
    print(f"  summary ok         : total={s['total']}  ✓")
    results["T7-alert_manager_query"] = "OK"
except Exception as e:
    import traceback; traceback.print_exc()
    results["T7-alert_manager_query"] = "FAILED"
finally:
    os.unlink(db_path)


# ══════════════════════════════════════════════════════════════════════════════
# T8 — AlertManager : acknowledge
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T8 — alert_manager_ack")
print("=" * 60)

try:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    mgr = AlertManager(db_path=db_path)
    mgr.save([
        AnomalyAlert(source="spark", metric_name="job.rejection_rate_pct",
                     algorithm="threshold", severity="critical", value=25.0, expected="<= 20"),
        AnomalyAlert(source="spark", metric_name="job.duration_seconds",
                     algorithm="zscore",    severity="warning",  value=200.0, expected="μ=12"),
        AnomalyAlert(source="dbt",   metric_name="dbt.run.pass_rate",
                     algorithm="trend",     severity="warning",  value=60.0,  expected=">= 80"),
    ])

    # Acknowledge individuel
    rows = mgr.query()
    first_id = rows[-1]["id"]  # le plus ancien (ORDER BY ts DESC → dernier = plus vieux)
    ok = mgr.acknowledge(first_id)
    assert ok is True
    assert mgr.count(acknowledged=True) == 1
    assert mgr.count(acknowledged=False) == 2

    # Acknowledge par severity — les 2 warnings restantes (id=2 et id=3)
    n_acked = mgr.acknowledge_all(severity="warning")
    assert n_acked == 2, f"2 warnings à acker, obtenu {n_acked}"
    assert mgr.count(acknowledged=False) == 0

    # ID inexistant
    ok2 = mgr.acknowledge(99999)
    assert ok2 is False

    print(f"  acknowledge({first_id}) : ok  ✓")
    print(f"  acknowledge_all(warning) : {n_acked}  ✓")
    print(f"  acknowledge(99999) → False  ✓")
    results["T8-alert_manager_ack"] = "OK"
except Exception as e:
    import traceback; traceback.print_exc()
    results["T8-alert_manager_ack"] = "FAILED"
finally:
    os.unlink(db_path)


# ══════════════════════════════════════════════════════════════════════════════
# T9 — Intégration bout-en-bout
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T9 — detector_integration")
print("=" * 60)

try:
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name
    store = SQLiteMetricsStore(db_path=db_path)
    mgr   = AlertManager(db_path=db_path)

    # Alimenter le store avec des métriques mixtes (bonnes + mauvaises)
    store.write([
        # rejection_rate trop haute → threshold critical
        MetricPoint(source="spark", name="job.rejection_rate_pct", value=25.0),
        # duration qui monte → trend
        *[MetricPoint(source="spark", name="job.duration_seconds", value=v)
          for v in [10, 15, 22, 35, 55, 90]],
        # pass_rate saine
        *[MetricPoint(source="dbt", name="dbt.run.pass_rate", value=100.0)
          for _ in range(6)],
    ])

    detector = AnomalyDetector(store, min_history=5)
    alerts   = detector.run()
    n_saved  = mgr.save(alerts)

    assert len(alerts) >= 2, f"≥ 2 alertes attendues, obtenu {len(alerts)}"
    assert n_saved == len(alerts)
    assert mgr.count() == n_saved

    # Vérifier que pass_rate saine n'a pas déclenché d'alerte
    dbt_alerts = [a for a in alerts if a.source == "dbt"]
    # Le pass_rate à 100 % ne devrait pas déclencher threshold (min=80) ni trend
    pass_rate_alerts = [a for a in dbt_alerts if a.metric_name == "dbt.run.pass_rate"]
    assert len(pass_rate_alerts) == 0, f"Faux positif sur pass_rate=100 : {pass_rate_alerts}"

    summary = mgr.summary()
    print(f"  {len(alerts)} alertes détectées et sauvegardées  ✓")
    print(f"  dbt.run.pass_rate sain → 0 alerte  ✓")
    print(f"  Summary : {summary['by_severity']}  ✓")
    results["T9-detector_integration"] = "OK"
except Exception as e:
    import traceback; traceback.print_exc()
    results["T9-detector_integration"] = "FAILED"
finally:
    os.unlink(db_path)


# ══════════════════════════════════════════════════════════════════════════════
# T10 — API : routes /alerts
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("T10 — api_alerts_endpoints")
print("=" * 60)

try:
    from fastapi.testclient import TestClient
    from spark.metrics.api import create_app

    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    # Préparer des données avec anomalie
    store = SQLiteMetricsStore(db_path=db_path)
    store.write([
        MetricPoint(source="spark", name="job.rejection_rate_pct", value=25.0),
        *[MetricPoint(source="spark", name="job.duration_seconds", value=v)
          for v in [10, 15, 22, 35, 55, 90]],
    ])

    app    = create_app(db_path=db_path)
    client = TestClient(app)

    # POST /detect
    r = client.post("/api/v1/alerts/detect")
    assert r.status_code == 201
    detected = r.json()["detected"]
    assert detected >= 1
    print(f"  POST /alerts/detect    201  ✓  detected={detected}")

    # GET /alerts
    r = client.get("/api/v1/alerts")
    assert r.status_code == 200
    assert r.json()["count"] == detected
    print(f"  GET  /alerts           200  ✓  count={r.json()['count']}")

    # GET /alerts?severity=critical
    r = client.get("/api/v1/alerts?severity=critical")
    assert r.status_code == 200
    print(f"  GET  /alerts?severity=critical  200  ✓  count={r.json()['count']}")

    # GET /alerts/summary
    r = client.get("/api/v1/alerts/summary")
    assert r.status_code == 200
    s = r.json()
    assert s["total"] >= 1
    print(f"  GET  /alerts/summary   200  ✓  total={s['total']}")

    # PATCH /alerts/{id}/acknowledge
    alerts_list = client.get("/api/v1/alerts").json()["alerts"]
    first_id    = alerts_list[0]["id"]
    r = client.patch(f"/api/v1/alerts/{first_id}/acknowledge")
    assert r.status_code == 200
    assert r.json()["acknowledged"] is True
    print(f"  PATCH /alerts/{first_id}/acknowledge  200  ✓")

    # PATCH id inexistant → 404
    r = client.patch("/api/v1/alerts/99999/acknowledge")
    assert r.status_code == 404
    print(f"  PATCH /alerts/99999/acknowledge  404  ✓")

    # POST /acknowledge-all
    r = client.post("/api/v1/alerts/acknowledge-all")
    assert r.status_code == 200
    print(f"  POST /alerts/acknowledge-all  200  ✓  acknowledged={r.json()['acknowledged']}")

    results["T10-api_alerts_endpoints"] = "OK"
except Exception as e:
    import traceback; traceback.print_exc()
    results["T10-api_alerts_endpoints"] = "FAILED"
finally:
    try:
        os.unlink(db_path)
    except Exception:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# RÉSUMÉ
# ══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 60)
print("RÉSUMÉ")
print("=" * 60)
all_ok = True
for label, status in results.items():
    icon = "✅" if status == "OK" else "❌"
    print(f"  {icon}  {label:<38} {status}")
    if status != "OK":
        all_ok = False
print("=" * 60)
print("RÉSULTAT GLOBAL :", "✅ TOUS OK" if all_ok else "❌ ÉCHECS DÉTECTÉS")
