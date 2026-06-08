# Rapport Phase 3 — Data Trust Agent : Core (Semaines 7-11)

**Date :** 2026-06-08  
**Auteur :** Reema  
**Projet :** Data Trust Agent — PFE  
**Stack :** Airflow · Spark · dbt · Python 3.9 · SQLite · Mistral AI

---

## Résumé exécutif

La Phase 3 implémente le cœur du Data Trust Agent : un système autonome de surveillance, détection d'anomalies, explication et auto-amélioration des règles de qualité pour les pipelines de données. L'agent est opérationnel sur 3 pipelines (`ingest_sales`, `clean_sales`, `aggregate_sales`) et couvre l'intégralité des objectifs planifiés (Semaines 7 à 11).

---

## Architecture

```
┌────────────────────────────────────────────────────────────────┐
│                      Data Trust Agent                          │
│                                                                │
│  Airflow ──┐                                                   │
│  Spark  ───┼──► Extractors ──► SQLite Time-Series             │
│  dbt    ──┘        (S7)          Store (S7)                   │
│                                    │                           │
│                                    ▼                           │
│                         AnomalyDetector (S8-9)                │
│                    ┌───────────────────────────┐              │
│                    │ VolumeDetector            │              │
│                    │ DistributionDetector      │              │
│                    │ SchemaDetector            │              │
│                    │ PerformanceDetector       │              │
│                    │ SeasonalityDetector       │              │
│                    │ TrendDetector             │              │
│                    │ CorrelationDetector       │              │
│                    │ MLBaselineDetector (IF)   │              │
│                    │ SeverityScorer            │              │
│                    └───────────────────────────┘              │
│                                    │                           │
│                                    ▼                           │
│                         LLMExplainer (S10)                    │
│                    Mistral AI → Ollama → Template             │
│                                    │                           │
│                                    ▼                           │
│               ┌────────────────────────────────┐              │
│               │  Validation Module (S11)        │              │
│               │  TestGenerator                 │              │
│               │  HistoricalValidator           │              │
│               │  ABTesting                     │              │
│               │  FeedbackLoop                  │              │
│               └────────────────────────────────┘              │
└────────────────────────────────────────────────────────────────┘
```

---

## Semaine 7 — Collecte & centralisation

### Ce qui a été livré

| Composant | Fichier | Description |
|---|---|---|
| Extracteur Airflow | `extractors/airflow_extractor.py` | Extrait durée, statut DAG, task failures |
| Extracteur Spark | `extractors/spark_extractor.py` | Extrait row_count, durée, rejection_rate par job |
| Extracteur dbt | `extractors/dbt_extractor.py` | Extrait pass_rate des tests, statut des modèles |
| Time-series store | `spark/metrics/storage.py` | SQLite local + InfluxDB optionnel (Docker) |
| API centralisée | `spark/metrics/api.py` | FastAPI — endpoints `/alerts`, `/detect`, `/metrics` |

### Schéma SQLite

```sql
metric_points(id, ts, source, name, tags_json, value, extra_json)
```

---

## Semaines 8-9 — Détection d'anomalies avancée

### Détecteurs implémentés

| Détecteur | Niveau | Méthode |
|---|---|---|
| `VolumeDetector` | volume | Z-score + drop % absolu |
| `DistributionDetector` | distribution | Z-score par colonne (mean, std, null_rate) |
| `SchemaDetector` | schema | Comparaison colonne/type vs référence |
| `PerformanceDetector` | performance | SLA breach + failure_rate + trend |
| `SeasonalityDetector` | temporal | Baseline par jour de semaine |
| `TrendDetector` | temporal | Régression linéaire glissante |
| `CorrelationDetector` | temporal | Corrélations inter-pipelines |
| `MLBaselineDetector` | ml | Isolation Forest (scikit-learn) |

### Scoring de sévérité

Le `SeverityScorer` calcule un score 0-100 pour chaque anomalie selon :
- Le niveau de détection (ML et temporal ont des poids plus élevés)
- Le nombre de déviations σ
- Le poids du pipeline
- La récurrence de l'anomalie
- Un bonus si la même métrique est signalée par plusieurs détecteurs

**Seuils de sévérité :**

| Score | Sévérité |
|---|---|
| ≥ 75 | CRITICAL |
| ≥ 55 | HIGH |
| ≥ 35 | MEDIUM |
| < 35 | LOW |

---

## Semaine 10 — Intégration LLM

### Stratégie

Conformément au document d'analyse (`analyse_llm.docx`) :

1. **Mistral AI** (primaire) — `mistral-large-latest` — hébergé OVHcloud, RGPD-compatible
2. **Ollama** (fallback local) — zéro transfert de données
3. **Template** (fallback ultime) — règles statiques, toujours disponible

### Prompt système

L'agent adopte le rôle d'un Data Reliability Engineer et génère pour chaque anomalie :
- La cause probable (pas seulement le symptôme)
- Une action corrective immédiate
- Une évaluation si l'anomalie est bénigne

### Cache

Les réponses LLM sont mises en cache sur disque (`spark/data/llm_cache/`) par hash SHA-256 du prompt, pour optimiser les coûts et la latence.

### Variables d'environnement

| Variable | Défaut | Description |
|---|---|---|
| `MISTRAL_API_KEY` | — | Clé API Mistral (requis pour les appels cloud) |
| `METRICS_LLM_MODEL` | `mistral-large-latest` | Override du modèle |
| `METRICS_LLM_OLLAMA_URL` | `http://localhost:11434` | URL Ollama |
| `METRICS_LLM_CACHE_DIR` | `spark/data/llm_cache` | Répertoire du cache |

---

## Semaine 11 — Génération automatique & validation

### TestGenerator

Génère automatiquement des règles de seuils depuis l'historique `PipelineMetrics` :

```python
gen   = TestGenerator(sigma_factor=3.0)
rules = gen.generate(history_snapshots)   # → List[GeneratedRule]
gen.save_rules(rules, "rules.json")
```

Règles générées par métrique :

| Métrique | Borne basse | Borne haute |
|---|---|---|
| `row_count` | 0 | mean + 3σ |
| `duration_seconds` | 0 | mean + 3σ |
| `task_failures` | 0 | mean + 3σ |
| `success` | 1.0 (True) | 1.0 |
| `col:<name>.<stat>` | mean − 3σ | mean + 3σ |

### HistoricalValidator

Évalue les règles sur un dataset labelisé (anomalie connue / normale) et calcule precision, recall, F1 :

```python
validator = HistoricalValidator()
results   = validator.validate(rules, labeled_snapshots)
report    = validator.report(results)
# → {"mean_f1": 0.85, "mean_precision": 0.88, "mean_recall": 0.82, ...}
```

### A/B Testing

Compare deux configurations de règles (ex. σ=2.0 vs σ=3.0) sur le même dataset :

```python
ab     = ABTesting()
result = ab.compare_sigmas(history, labeled, sigma_a=2.0, sigma_b=3.0)
print(result.winner)          # "sigma=2.0" | "sigma=3.0" | "tie"
print(result.improvement_pct) # +12.5%
```

### Feedback Loop

Les opérateurs signalent les erreurs de classification ; les seuils sont ajustés automatiquement :

| Feedback | Effet |
|---|---|
| `false_positive` | σ augmente de +0.25 (seuils s'élargissent) |
| `false_negative` | σ diminue de −0.25 (seuils se resserrent) |

```python
loop = FeedbackLoop(db_path="feedback.db")
loop.record(FeedbackEntry(rule_id="...", feedback_type="false_positive"))
adjusted_rules = loop.adjust_rules(rules)
```

---

## Livrable — Agent fonctionnel sur 3 pipelines

Script : `livrable_agent_phase3.py`

```
python livrable_agent_phase3.py
# ou avec Mistral :
MISTRAL_API_KEY=sk-... python livrable_agent_phase3.py
```

### Résultats d'exécution (2026-06-08)

| Pipeline | Scénario | Anomalies | Pire sévérité | Règles | F1 moyen |
|---|---|---|---|---|---|
| 🟢 `ingest_sales` | Nominal | 0 | — | 7 | 0.429 |
| 🔴 `clean_sales` | Chute volume (−92%) | 3 | HIGH | 7 | 0.429 |
| 🔴 `aggregate_sales` | SLA dépassé (×4.5) | 3 | HIGH | 7 | 0.429 |

### Détail des anomalies détectées

#### clean_sales — Chute de volume

| # | Niveau | Sévérité | Score | Métrique | Observé | Attendu |
|---|---|---|---|---|---|---|
| 1 | VOLUME | HIGH | 75.0 | `row_count` | 3 840 | 47 887 |
| 2 | TEMPORAL | HIGH | 60.0 | `row_count` | 3 840 | 48 032 (baseline lundi) |
| 3 | ML | LOW | 35.0 | `isolation_forest_score` | −0.665 | — |

**Explication LLM :** Volume anormal détecté — chute de 92% vs historique. Vérifier la source de données et les logs d'ingestion pour détecter une perte ou un doublon.

#### aggregate_sales — Dépassement SLA

| # | Niveau | Sévérité | Score | Métrique | Observé | Attendu |
|---|---|---|---|---|---|---|
| 1 | PERFORMANCE | MEDIUM | 55.0 | `duration_seconds` | 540s | SLA=120s |
| 2 | PERFORMANCE | MEDIUM | 55.0 | `task_failures` | 2 | 0 |
| 3 | TEMPORAL | HIGH | 60.0 | `duration_seconds` | 540s | baseline lundi=60.5s |

**Explication LLM :** SLA breach — pipeline 4.5× au-dessus du SLA. Vérifier les ressources Spark et identifier les goulots d'étranglement.

---

## Structure des fichiers

```
projet_stage/
├── extractors/
│   ├── airflow_extractor.py          # S7 — Extraction métriques Airflow
│   ├── dbt_extractor.py              # S7 — Extraction métriques dbt
│   └── spark_extractor.py            # S7 — Extraction métriques Spark
├── spark/metrics/
│   ├── storage.py                    # S7 — Time-series store SQLite/InfluxDB
│   ├── api.py                        # S7 — API REST FastAPI
│   ├── collector.py                  # S7 — Collecteur unifié
│   ├── alert_manager.py              # S8 — Gestion des alertes SQLite
│   ├── llm_explainer.py              # S10 — Explication LLM (Mistral/Ollama)
│   ├── pipeline_adapter.py           # Bridge MetricPoints → PipelineMetrics
│   ├── anomaly_detection/
│   │   ├── models.py                 # S8 — Modèles (Anomaly, PipelineMetrics…)
│   │   ├── anomaly_detector.py       # S8-9 — Orchestrateur tous détecteurs
│   │   ├── detectors.py              # S8 — Volume, Distribution, Schema, Perf
│   │   ├── temporal.py               # S9 — Saisonnalité, Trend, Corrélation
│   │   ├── ml.py / ml_detector.py    # S9 — Isolation Forest
│   │   └── scoring.py                # S9 — SeverityScorer
│   └── validation/
│       ├── models.py                 # S11 — GeneratedRule, ValidationResult…
│       ├── test_generator.py         # S11 — Génération de règles
│       ├── historical_validator.py   # S11 — Validation historique
│       ├── ab_testing.py             # S11 — A/B testing
│       └── feedback_loop.py          # S11 — Feedback opérateur
├── spark/tests/
│   ├── test_anomaly_detection.py     # Tests S8 (10 tests)
│   ├── test_level_detectors.py       # Tests S8-9
│   ├── test_temporal_and_ml.py       # Tests S9
│   ├── test_scoring_and_integration.py
│   └── test_semaine11.py             # Tests S11 (11 tests — tous OK)
└── livrable_agent_phase3.py          # ✅ Livrable Phase 3
```

---

## Tests

| Suite | Tests | Statut |
|---|---|---|
| `test_anomaly_detection.py` | 10 | ✅ OK |
| `test_semaine11.py` | 11 | ✅ OK |

```bash
# Lancer les tests Semaine 11
source airflow_env/bin/activate
python spark/tests/test_semaine11.py
```

---

## Décisions techniques

| Décision | Choix | Raison |
|---|---|---|
| LLM primaire | Mistral AI | RGPD, hébergement UE (OVHcloud), qualité |
| LLM fallback | Ollama local | Zéro transfert, disponibilité offline |
| Store métriques | SQLite | Aucune dépendance externe, toujours disponible |
| Détection ML | Isolation Forest | Unsupervised, efficace sur données tabulaires |
| Seuils dynamiques | σ-factor | Adaptatif, calibré sur historique réel |
| Persistance feedback | SQLite | Simple, cohérent avec le reste du projet |

---

## Prochaines étapes — Phase 4 (Semaines 12-14)

- **S12** — Data Catalog enrichi : publication automatique des descriptions, lineage dbt+Spark, search sémantique via LLM embeddings
- **S13** — Dashboard web (Streamlit/Gradio) : vue d'ensemble santé pipelines, détail anomalies, approbation des règles générées
- **S14** — Alerting & notifications : Teams/Email, gestion d'incidents, runbook automatique
