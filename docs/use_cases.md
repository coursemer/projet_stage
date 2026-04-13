# Documentation des Cas d'Usage — Anomaly Engineering

## Vue d'ensemble

Ce document décrit les 10 types d'anomalies supportés par le framework `AnomalyInjector`, leur
justification métier, et comment les utiliser avec les autres outils du projet (génération de
datasets, pipelines Spark, tests).

---

## Architecture du Framework

```
generate_datasets.py          anomaly_injector.py          controlled_failure_pipeline.py
      │                               │                                │
      │  customers.csv                │  sales_dirty.csv               │  detection_manifest.json
      │  products.csv    ────────────▶│  + injection_report.json ─────▶│  (précis ou estimé)
      │  sales.csv                    │  [+ _row_id, _injected_types]  │
      │  inventory.csv                │                                │
      └─ online_retail.csv            └─ --tag-rows active le mode     └─ Parquet lignes valides
         (--source online-retail)        de détection précise
```

---

## Sources de Données (`generate_datasets.py`)

| Mode (`--source`) | Description | Lignes sales | Usage recommandé |
|---|---|---|---|
| `synthetic` (défaut) | Données 100 % synthétiques, distributions contrôlées | 50 000 | Tests unitaires, reproductibilité |
| `online-retail` | Format UCI Online Retail II (e-commerce UK) — téléchargement Kaggle ou simulation | ~542 000 | Tests de volume, données réalistes |
| `blend` | Mélange 50/50 synthétique + Online Retail, colonnes `_source` | ~75 000 | Validation de robustesse cross-source |

### Exemple

```bash
# Données synthétiques (défaut)
python spark/generate_datasets.py --seed 42

# Simulation UCI Online Retail II (sans credentials Kaggle)
python spark/generate_datasets.py --source online-retail --dest spark/data/raw/kaggle

# Mélange
python spark/generate_datasets.py --source blend --dest spark/data/raw/blended
```

> **Note Kaggle** : Pour le téléchargement réel, installez `pip install kaggle` et
> placez vos credentials dans `~/.kaggle/kaggle.json`. Sans credentials, le générateur
> produit une simulation statistiquement fidèle.

---

## Catalogue des 10 Types d'Anomalies

### 1. `nulls` — Valeurs manquantes

**Description** : Injection de `NaN` sur les colonnes clés (`customer_id`, `product_id`, `sale_date`).

**Scénario réel** : Pipeline source défaillant qui n'écrit pas toutes les colonnes obligatoires ; join raté en amont.

**Ce que le pipeline teste** :
- Filtre `order_id IS NOT NULL` et `sale_date IS NOT NULL` dans `ingest_sales`
- Contrainte Pandera `nullable=False` dans `clean_sales`

**Taux de détection attendu** : ~100 % (filtre dur sur les clés)

```python
injector.inject(df, anomaly_types=["nulls"])
```

---

### 2. `duplicates` — Lignes dupliquées

**Description** : Copie exacte de lignes existantes ajoutée en fin de DataFrame (simule double ingestion).

**Scénario réel** : Kafka consumer reprocessant un offset ; retry Airflow sans idempotence.

**Ce que le pipeline teste** :
- `dropDuplicates(["order_id"])` dans `clean_sales`
- Contrainte d'unicité sur `order_id`

**Taux de détection attendu** : ~100 % si déduplication active, 0 % sinon (survivent silencieusement)

```python
injector.inject(df, anomaly_types=["duplicates"])
```

---

### 3. `out_of_range` — Valeurs hors bornes métier

**Description** : `quantity=9 999` ou `unit_price=0.0001` — valeurs techniquement valides mais absurdes métier.

**Scénario réel** : Erreur de saisie manuelle ; bug de conversion d'unités (centimes ↔ euros).

**Ce que le pipeline teste** :
- Filtre `quantity > 0 AND quantity < 1000` (si implémenté)
- `Check.in_range(0.01, 10_000)` Pandera sur `unit_price`

**Taux de détection attendu** : Dépend de la borne configurée — 0 % si pas de borne supérieure

```python
injector.inject(df, anomaly_types=["out_of_range"])
```

---

### 4. `type_mismatch` — Mauvais type de données

**Description** : Valeurs non castables (`"N/A"`, `"--"`, `"null"`) dans des colonnes numériques.

**Scénario réel** : Export CSV depuis Excel avec cellules vides exportées en `"N/A"` ; changement
de format source non communiqué.

**Ce que le pipeline teste** :
- `PERMISSIVE` mode de Spark → parse en `NULL` automatiquement
- Schema enforcement `IntegerType` / `DoubleType` → cast silencieux puis filtre NOT NULL

**Taux de détection attendu** : ~100 % (les non-castables deviennent NULL et sont filtrés)

```python
injector.inject(df, anomaly_types=["type_mismatch"])
```

---

### 5. `format_violation` — Format d'identifiant incorrect

**Description** : `ORD-1234567` devient `ord1234567` ou `ORD_1234567` (regex `^ORD-\d+$` violée).

**Scénario réel** : Changement de convention de nommage dans le système source ; migration de
base de données avec normalisation incorrecte.

**Ce que le pipeline teste** :
- `Check.str_matches(r"^ORD-\d+$")` Pandera dans `clean_sales`
- Filtre `order_id IS NOT NULL` (insuffisant seul — le format corrompu passe le NULL check)

**Taux de détection attendu** : ~100 % si validation regex active, 0 % sinon

```python
injector.inject(df, anomaly_types=["format_violation"])
```

---

### 6. `future_date` — Date dans le futur

**Description** : `sale_date` définie entre aujourd'hui +1 et aujourd'hui +365 jours.

**Scénario réel** : Erreur de timezone (UTC vs. local) ; horodatage serveur incorrect ;
pré-chargement de commandes planifiées.

**Ce que le pipeline teste** :
- `Check.less_than_or_equal_to(date.today())` Pandera (optionnel)
- Alertes de monitoring sur les agrégations quotidiennes aberrantes

**Taux de détection attendu** : 0 % avec les filtres actuels (les dates futures passent)

```python
injector.inject(df, anomaly_types=["future_date"])
```

> Ce type révèle une **lacune** du pipeline : les dates futures survivent sans alerte.
> Cas d'usage pédagogique pour justifier l'ajout d'un check temporel.

---

### 7. `negative_values` — Valeurs négatives sur colonnes positives

**Description** : `quantity` ou `revenue` mis à l'opposé de leur valeur absolue.

**Scénario réel** : Retours/avoirs mal séparés des ventes dans la source ; inversion de signe
lors d'une transformation ETL.

**Ce que le pipeline teste** :
- Filtre `quantity > 0` dans `ingest_sales` → capturé immédiatement
- `Check.greater_than(0)` Pandera sur `revenue`

**Taux de détection attendu** : ~100 % pour `quantity` (filtre hard), ~100 % pour `revenue`
si check Pandera actif

```python
injector.inject(df, anomaly_types=["negative_values"])
```

---

### 8. `referential_break` — Violation d'intégrité référentielle

**Description** : `customer_id` ou `product_id` remplacés par des valeurs fantômes inexistantes
dans les tables de référence (`CUSTOMER-GHOST0001`, etc.).

**Scénario réel** : Suppression de client dans le CRM sans cascade sur les transactions ;
chargement de données de test en production.

**Ce que le pipeline teste** :
- Join `sales LEFT JOIN customers` dans `customer_360` → apparaissent comme orphelins
- Contrôle d'intégrité référentielle post-ETL (absent des pipelines actuels)

**Taux de détection attendu** : 0 % (les IDs fantômes passent tous les filtres actuels)

```python
# Avec validation des IDs de référence
valid_ids = {
    "customer_id": set(customers_df["customer_id"]),
    "product_id":  set(products_df["product_id"]),
}
injector.inject(df, anomaly_types=["referential_break"], valid_ids=valid_ids)
```

> Ce type révèle une **lacune** : aucun pipeline ne valide l'existence des clés étrangères.

---

### 9. `whitespace_corruption` — Espaces parasites

**Description** : Espaces en début/fin ou doubles espaces dans `channel`, `region`, `currency`
(ex. `"ONLINE"` → `"  ONLINE"` ou `"ONLINE  "`).

**Scénario réel** : Export CSV depuis interface web avec padding ; copier-coller humain ;
encodage différent entre systèmes.

**Ce que le pipeline teste** :
- `F.trim()` dans `ingest_sales` normalise les colonnes text → **détection complète**
- `Check.isin(["ONLINE", "STORE", "MOBILE"])` Pandera sans `.str.strip()` préalable → **rate l'anomalie**

**Taux de détection attendu** : 0 % après `UPPER(TRIM(...))` dans `ingest_sales`
(les espaces sont nettoyés avant le check)

```python
injector.inject(df, anomaly_types=["whitespace_corruption"])
```

> Cas d'usage : valider que le `TRIM` est bien appliqué **avant** le check `isin`.

---

### 10. `revenue_inconsistency` — Incohérence de colonne calculée

**Description** : `revenue` multiplié par un facteur aléatoire 0.1–0.5, le rendant incohérent
avec `quantity × unit_price`.

**Scénario réel** : Recalcul partiel après modification de `unit_price` sans mise à jour de
`revenue` ; bug dans un job de transformation intermédiaire.

**Ce que le pipeline teste** :
- Recalcul `revenue = ROUND(quantity * unit_price, 2)` dans `ingest_sales` → **écrase la valeur corrompue**
- Aucun check cross-colonne dans `clean_sales` sur la cohérence `revenue = qty × price`

**Taux de détection attendu** : ~100 % si recalcul actif (la valeur est réécrite),
0 % si `revenue` est conservé tel quel

```python
injector.inject(df, anomaly_types=["revenue_inconsistency"])
```

---

## Mode de Détection Précise (`--tag-rows`)

Par défaut, `controlled_failure_pipeline.py` **estime** les taux de détection en distribuant
le taux de rejet global proportionnellement à chaque type. Avec `--tag-rows`, la mesure devient
**exacte** grâce à un traçage par ligne.

### Principe

```
anomaly_injector.py --tag-rows
  ↓
Ajoute _row_id (int) et _injected_types (pipe-separated) au CSV dirty

controlled_failure_pipeline.py
  ↓
Spark joint les lignes rejetées avec leurs tags
  ↓
Compte précis : caught[type] = nb lignes rejetées portant le tag "type"
```

### Exemple complet

```bash
# 1. Générer les données propres
python spark/generate_datasets.py --seed 42

# 2. Injecter des anomalies avec tagging
python spark/anomaly_injector.py \
    --input  spark/data/raw/sales.csv \
    --output spark/data/raw/sales_dirty.csv \
    --report spark/data/raw/injection_report.json \
    --rate   0.05 --seed 42 --tag-rows

# 3. Lancer le pipeline de détection (mode précis automatique)
python spark/jobs/controlled_failure_pipeline.py \
    --date   2026-04-13 \
    --input  spark/data/raw/sales_dirty.csv \
    --report spark/data/raw/injection_report.json \
    --dest   spark/data/staging/sales_controlled
```

### Extrait de sortie attendu

```
── Manifest de détection [precise] ─────────────────────────────
  Taux de rejet global : 12.4%
  nulls                     injecté= 2500  attrapé= 2500  survécu=    0  (100.0%)
  duplicates                injecté= 2500  attrapé=    0  survécu= 2500  (0.0%)
  out_of_range              injecté= 2500  attrapé=  312  survécu= 2188  (12.5%)
  type_mismatch             injecté= 2500  attrapé= 2500  survécu=    0  (100.0%)
  format_violation          injecté= 2500  attrapé=    0  survécu= 2500  (0.0%)
  future_date               injecté= 2500  attrapé=    0  survécu= 2500  (0.0%)
  negative_values           injecté= 2500  attrapé= 2500  survécu=    0  (100.0%)
  referential_break         injecté= 2500  attrapé=    0  survécu= 2500  (0.0%)
  whitespace_corruption     injecté= 2500  attrapé=    0  survécu= 2500  (0.0%)
  revenue_inconsistency     injecté= 2500  attrapé= 2500  survécu=    0  (100.0%)
```

---

## Matrice de Détection — Pipelines Actuels

| Type d'anomalie | `ingest_sales` | `clean_sales` (Pandera) | `customer_360` | Lacune identifiée |
|---|---|---|---|---|
| nulls | ✅ filtre NOT NULL | ✅ `nullable=False` | — | — |
| duplicates | — | ✅ `dropDuplicates` | — | Doit être avant l'ingest |
| out_of_range | ✅ `qty > 0`, `price > 0` | ✅ `Check.in_range` | — | Pas de borne supérieure |
| type_mismatch | ✅ cast → NULL → filtre | ✅ schema enforcement | — | — |
| format_violation | — | ✅ `Check.str_matches` | — | Dépend de Pandera actif |
| future_date | ❌ non contrôlé | ❌ non contrôlé | — | **Lacune** — ajouter check temporel |
| negative_values | ✅ `qty > 0` | ✅ `Check.gt(0)` | — | `revenue` négatif non filtré |
| referential_break | ❌ non contrôlé | ❌ non contrôlé | ⚠️ orphelins visibles | **Lacune** — ajouter FK check |
| whitespace_corruption | ✅ `TRIM` appliqué | ⚠️ dépend ordre ops | — | Ordre TRIM avant isin critique |
| revenue_inconsistency | ✅ recalcul `qty×price` | — | — | Si recalcul désactivé : 0% |

---

## Référence API

```python
from spark.anomaly_injector import AnomalyInjector, ALL_TYPES, ANOMALY_CATALOG

# Injection sélective avec tagging
injector = AnomalyInjector(seed=42, injection_rate=0.05)
dirty_df, report = injector.inject(
    df,
    anomaly_types=["nulls", "future_date", "referential_break"],
    target_columns={"nulls": ["customer_id"]},   # surcharge colonnes par défaut
    valid_ids={"customer_id": set(customers["customer_id"])},
    tag_rows=True,   # active _row_id et _injected_types
)

# Consulter le catalogue
for atype, meta in ANOMALY_CATALOG.items():
    print(f"{atype}: {meta['use_case']}")
```
