# Rapport Semaine 3 — Pipeline Sales Analytics (Batch Quotidien)

**Date** : 08 avril 2026  
**Auteur** : ruby-rue  
**Statut** : En cours — pipeline construit, blocage environnement Windows (winutils)

---

## Objectif de la semaine

Construire le **Pipeline 1 : Sales Analytics** en mode batch quotidien selon le flux :

```
Ingestion → Nettoyage → Agrégations
```

---

## Fichiers créés / modifiés

### Données de test

| Fichier | Description |
|---|---|
| `spark/data/raw/sales/date=2026-04-08/sales.csv` | 20 lignes de ventes brutes avec cas valides et invalides (null order_id, quantity négative, customer_id manquant) |

Dossiers créés (vides, générés par les jobs) :
- `spark/data/staging/sales/`
- `spark/data/staging/sales_clean/`
- `spark/data/marts/sales/`

---

### Spark Jobs

#### `spark/jobs/ingest_sales.py` *(corrigé + adapté local)*
- **Étape 1 — Ingestion**
- Corrige le bug de nommage (`injest_sales.py` → `ingest_sales.py`)
- Lit les CSV bruts depuis `spark/data/raw/sales/date=<date>/`
- Applique le schéma SALES_SCHEMA (9 colonnes)
- Filtre les lignes invalides : `order_id` null, `quantity ≤ 0`, `unit_price ≤ 0`
- Dérive la colonne `revenue = quantity × unit_price`
- Standardise `currency`, `channel`, `region` en majuscules
- Ajoute `ingested_at` (timestamp) et `run_date`
- Écrit en Parquet partitionné par `run_date / region`
- Mode Spark : `local[*]` (compatible Windows sans cluster)

#### `spark/jobs/clean_sales.py` *(nouveau)*
- **Étape 2 — Nettoyage**
- Lit le Parquet staging produit par l'ingestion
- Déduplique sur `order_id`
- Recalcule `revenue` pour garantir la cohérence
- Remplace les `customer_id` null par `UNKNOWN`
- Valide avec **Pandera** (schéma strict) :
  - `order_id` : regex `^ORD-\d+$`
  - `quantity` : > 0
  - `unit_price` : > 0
  - `channel` : isin `[ONLINE, STORE, MOBILE]`
  - `region` : isin `[NORTH, SOUTH, EAST, WEST]`
  - `currency` : exactement 3 caractères
- Ajoute `cleaned_at` (timestamp)
- Écrit en Parquet partitionné par `run_date / region`

#### `spark/jobs/aggregate_sales.py` *(nouveau)*
- **Étape 3 — Agrégations**
- Lit le Parquet clean
- Agrège par `sale_date / region / channel`
- KPIs calculés :
  - `nb_orders` — nombre de commandes distinctes
  - `nb_customers` — nombre de clients distincts
  - `total_units` — quantité totale vendue
  - `total_revenue` — chiffre d'affaires (arrondi 2 décimales)
  - `avg_order_value` — panier moyen
  - `cumulative_revenue_by_region` — CA cumulatif par région (window function)
- Affiche un tableau récapitulatif en console
- Écrit en Parquet partitionné par `run_date / region`

---

### dbt Models

#### `dbt/models/staging/stg_sales.sql` *(nouveau)*
- Vue SQL sur la source `raw.sales`
- Cast des types (date, integer, numeric)
- Filtres qualité : `order_id` not null, `quantity > 0`, `unit_price > 0`
- Colonnes standardisées en majuscules

#### `dbt/models/intermediate/int_sales_daily.sql` *(nouveau)*
- Vue SQL référençant `stg_sales`
- Agrégation journalière par `sale_date / region / channel`
- Calcule : `nb_orders`, `nb_customers`, `total_units`, `total_revenue`, `avg_order_value`

#### `dbt/models/marts/sales_summary.sql` *(nouveau)*
- Table SQL référençant `int_sales_daily`
- Ajoute deux métriques analytiques via window functions :
  - `cumulative_revenue_by_region` — CA cumulatif par région
  - `revenue_pct_of_day` — part du CA par channel sur la journée

---

### Configuration dbt

#### `dbt/models/sources.yml` *(mis à jour)*
- Ajout de la source `raw.sales` avec documentation des 9 colonnes
- Ajout des modèles `stg_sales`, `int_sales_daily`, `sales_summary` avec tests dbt :
  - `stg_sales.order_id` : `not_null` + `unique`
  - `stg_sales.sale_date` : `not_null`
  - `stg_sales.revenue` : `not_null`

#### `dbt/dbt_project.yml` *(mis à jour)*
- Ajout de la configuration de matérialisation pour la couche `intermediate` :
  ```yaml
  intermediate:
    materialized: view
  ```

---

### DAG Airflow

#### `dags/sales_analytics.py` *(réécrit)*
- Remplace `SparkSubmitOperator` (nécessite cluster Spark) par `BashOperator` avec appels Python directs
- Remplace les chemins S3 (`s3a://`) par des chemins locaux relatifs au projet
- Utilise l'API Airflow 3.x (`schedule` au lieu de `schedule_interval`)
- `start_date` fixe au lieu de `days_ago(1)`
- **7 étapes avec parallélisme partiel** :

```
ingest_sales_raw
       │
clean_and_validate ──────────────────────────────┐
       │                                          │
aggregate_sales                        dbt_run_stg_sales
       │                                          │
       │                            dbt_run_int_sales_daily
       │                                          │
       │                            dbt_run_sales_summary
       └──────────────────────────────────┐        │
                                    dbt_test_sales (quality gate)
```

| task_id | Outil | Rôle |
|---|---|---|
| `ingest_sales_raw` | BashOperator | Lance `ingest_sales.py` |
| `clean_and_validate_sales` | BashOperator | Lance `clean_sales.py` |
| `aggregate_sales` | BashOperator | Lance `aggregate_sales.py` |
| `dbt_run_stg_sales` | BashOperator | `dbt run stg_sales` |
| `dbt_run_int_sales_daily` | BashOperator | `dbt run int_sales_daily` |
| `dbt_run_sales_summary` | BashOperator | `dbt run sales_summary` |
| `dbt_test_sales` | BashOperator | `dbt test` (quality gate final) |

---

## Résultat attendu du pipeline

Avec les 20 lignes du CSV de test :

```
20 lignes brutes
 → -2 rejetées à l'ingestion (1 order_id null, 1 quantity négative)
 → 18 lignes Parquet (staging)
 → 0 rejetées par Pandera
 → 18 lignes Parquet (staging_clean)
 → GROUP BY region/channel
 → 8 lignes Parquet (mart)
```

Tableau mart final (8 lignes) :

| region | channel | nb_orders | total_revenue | avg_order_value |
|---|---|---|---|---|
| EAST   | ONLINE  | 1 | 249.95  | 249.95 |
| EAST   | STORE   | 4 | 1090.96 | 272.74 |
| NORTH  | ONLINE  | 3 | 188.97  | 62.99  |
| NORTH  | STORE   | 2 | 228.98  | 114.49 |
| SOUTH  | ONLINE  | 3 | 612.00  | 204.00 |
| SOUTH  | STORE   | 1 | 129.00  | 129.00 |
| WEST   | ONLINE  | 3 | 246.00  | 82.00  |
| WEST   | STORE   | 1 | 75.00   | 75.00  |

---

## Problèmes rencontrés

### 1. Java 8 incompatible avec PySpark 4.x
- **Symptôme** : processus tués silencieusement (`SUCCESS: The process with PID X has been terminated`)
- **Cause** : PySpark 4.0.2 requiert Java 11+
- **Fix** : Installer Java 11 ou 17 (Adoptium Temurin) et mettre à jour `JAVA_HOME`

### 2. winutils.exe manquant sur Windows
- **Symptôme** : `UnsatisfiedLinkError: NativeIO$Windows.access0`
- **Cause** : PySpark sur Windows nécessite `winutils.exe` et `hadoop.dll` pour accéder au système de fichiers local via l'API Hadoop
- **Fix** : Télécharger winutils depuis `github.com/cdarlint/winutils` (hadoop-3.3.6), placer dans `C:\hadoop\bin\`, définir `HADOOP_HOME=C:\hadoop`

---

## Prochaines étapes

- [ ] Résoudre winutils sur Windows (ou basculer sur pandas pour le dev local)
- [ ] Exécuter et valider les 3 étapes Spark bout en bout
- [ ] Connecter dbt à une base de données (DuckDB recommandé pour le local)
- [ ] Lancer le DAG dans Airflow et vérifier l'UI
- [ ] Pipeline 2 : Customer 360 (SCD Type 2)
- [ ] Pipeline 3 : Inventory SCD2
