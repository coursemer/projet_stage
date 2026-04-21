# Commandes Windows — Projet Data Engineering

## Prérequis

Avant de commencer, installer :

- [Python 3.9](https://www.python.org/downloads/release/python-3913/) — même version que le projet
- [Java JDK 11+](https://adoptium.net/) — requis par PySpark
- [Git](https://git-scm.com/download/win)

Après installation de Java, définir `JAVA_HOME` dans les variables d'environnement système :

```
JAVA_HOME = C:\Program Files\Eclipse Adoptium\jdk-11.x.x
```

---

## 1. Setup initial

```cmd
git clone https://github.com/coursemer/projet_stage.git
cd projet_stage

python -m venv airflow_env
airflow_env\Scripts\activate
pip install -r requirements.txt
```

---

## 2. Variables d'environnement PySpark

À définir une fois par session (après activation du venv) :

```cmd
set PYSPARK_PYTHON=%CD%\airflow_env\Scripts\python.exe
set PYSPARK_DRIVER_PYTHON=%CD%\airflow_env\Scripts\python.exe
```

Pour les rendre **permanentes** (une seule fois), ajouter ces deux lignes à `airflow_env\Scripts\activate.bat` :

```bat
set PYSPARK_PYTHON=%VIRTUAL_ENV%\Scripts\python.exe
set PYSPARK_DRIVER_PYTHON=%VIRTUAL_ENV%\Scripts\python.exe
```

---

## 3. Lancer le projet

### Activer le venv

```cmd
airflow_env\Scripts\activate
```

### Génération des données

```cmd
python spark\generate_datasets.py --seed 42
```

### Injection d'anomalies

```cmd
python spark\anomaly_injector.py ^
  --input spark\data\raw\sales.csv ^
  --output spark\data\raw\sales_dirty.csv ^
  --report spark\data\raw\injection_report.json ^
  --rate 0.05 --seed 42 --tag-rows ^
  --types nulls,duplicates,out_of_range,type_mismatch,format_violation,future_date,negative_values,referential_break,whitespace_corruption,revenue_inconsistency
```

### Pipelines Spark

```cmd
python spark\jobs\ingest_sales.py --date 2026-04-21
python spark\jobs\clean_sales.py --date 2026-04-21
python spark\jobs\aggregate_sales.py --date 2026-04-21
python spark\jobs\customer_360.py --date 2026-04-21
python spark\jobs\inventory_scd2.py --date 2026-04-21
```

### Controlled Failure Pipeline

```cmd
python spark\jobs\controlled_failure_pipeline.py ^
  --date 2026-04-21 ^
  --input spark\data\raw\sales_dirty.csv ^
  --report spark\data\raw\injection_report.json ^
  --dest spark\data\staging\sales_controlled
```

### Tests

```cmd
python spark\tests\test_anomalies.py
python spark\tests\test_all_pipelines.py
```

---

## Résumé de l'ordre d'exécution

| Étape | Commande |
|-------|----------|
| 1 | `git clone` + `pip install -r requirements.txt` |
| 2 | `generate_datasets.py` |
| 3 | `anomaly_injector.py` |
| 4 | `ingest_sales.py` → `clean_sales.py` → `aggregate_sales.py` |
| 5 | `customer_360.py` |
| 6 | `inventory_scd2.py` |
| 7 | `controlled_failure_pipeline.py` |
| 8 | `test_anomalies.py` + `test_all_pipelines.py` |
