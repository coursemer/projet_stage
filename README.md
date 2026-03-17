# Projet Stage - Plateforme ETL Airflow + Spark

Un environnement de développement complet pour l'orchestration et le traitement de workflows de données. Cette plateforme combine **Apache Airflow** (orchestration), **Apache Spark** (traitement distribué), **dbt** (transformation), et des frameworks de qualité des données pour une solution ETL production-ready.

## 🎯 Objectifs du Projet

- ✅ Orchestrer des pipelines de données complexes avec Airflow
- ✅ Traiter des données distribuées avec Spark
- ✅ Transformer et modéliser les données avec dbt
- ✅ Valider la qualité des données avec plusieurs frameworks
- ✅ Profiler et analyser les données
- ✅ Automatiser et monitorer les workflows

## 🏗️ Architecture

```
Airflow (Orchestration)
    ↓
Spark (Processing) + dbt (Transformation)
    ↓
Quality Frameworks (Validation)
    ↓
Data Profiling & Analytics
```

### Composants Principaux

| Composant | Version | Rôle |
|-----------|---------|------|
| **Apache Airflow** | 3.0.6 | Orchestration des DAGs |
| **Apache Spark** | 4.0.2 (PySpark) | Traitement distribué des données |
| **dbt Core** | 1.8.x | Transformation et modélisation |
| **Great Expectations** | Latest | Validation de schémas et assertions |
| **Soda Core** | Latest | Monitoring et checks de qualité |
| **Pandera** | Latest | Validation de DataFrames |
| **ydata-profiling** | Latest | Générations de rapports de profiling |
| **Docker Compose** | v2.20.2 | Orchestration des conteneurs |

## 📋 Prérequis

- **macOS** ou Linux
- **Python 3.9+**
- **Docker** et **Docker Compose**
- **Git**
- Minimum 4GB RAM disponible

### Versions Testées

- macOS 12+
- Python 3.9.x
- Docker 24.0.5+
- Docker Compose v2.20.2+

## 🚀 Installation et Setup

### 1. Cloner le Projet

```bash
git clone https://github.com/coursemer/projet_stage.git
cd projet_stage
```

### 2. Créer l'Environnement Virtuel

```bash
python3 -m venv airflow_env
source airflow_env/bin/activate
```

### 3. Installer les Dépendances

```bash
pip install --upgrade pip setuptools wheel
pip install apache-airflow==3.0.6 pyspark==4.0.2
pip install dbt-core dbt-postgres
pip install great-expectations soda-core pandera ydata-profiling
pip install pandas numpy pyarrow pyyaml
```

### 4. Initialiser Airflow

```bash
airflow db migrate
```

### 5. Lancer les Services

#### Terminal 1 - Scheduler Airflow
```bash
source airflow_env/bin/activate
airflow scheduler
```

#### Terminal 2 - API Server Airflow
```bash
source airflow_env/bin/activate
PATH=/Users/reema/Desktop/projet_stage/airflow_env/bin:$PATH \
  airflow_env/bin/airflow api-server --dev
```

#### Terminal 3 - Tests (optionnel)
```bash
source airflow_env/bin/activate
python tests/test_connectivity.py
```

## 📁 Structure du Projet

```
projet_stage/
├── README.md                          # Ce fichier
├── rapport_semaine1.md               # Rapport setup environnement
├── rapport_semaine2.md               # Rapport dbt + qualité
├── docker-compose.yml                # Configuration Docker (standby)
├── .gitignore                        # Git ignore rules
│
├── airflow_env/                      # Environnement virtuel Python
│
├── dags/                             # DAGs Airflow
│   └── hello_world.py               # DAG de test
│
├── dbt/                              # Configuration dbt
│   ├── dbt_project.yml              # Projet dbt
│   └── models/
│       ├── sources.yml              # Data catalog
│       └── staging/
│           └── stg_events.sql       # Modèle de staging
│
├── spark/                            # Scripts Spark
│   ├── test_spark.py                # Test Spark basique
│   ├── test_data_quality.py         # Tests frameworks qualité
│   └── data_profiling.py            # Profiling des données
│
├── tests/                            # Tests du projet
│   └── test_connectivity.py         # Tests de connectivité
│
├── logs/                             # Logs Airflow (généré)
│
├── dbt_setup.py                      # Script d'init dbt
│
└── .git/                             # Repository git
```

## 🔧 Utilisation

### Tests de Connectivité

Vérifier que tous les composants fonctionnent :

```bash
python tests/test_connectivity.py
```

**Sortie attendue** :
```
==================================================
Tests de Connectivité - projet_stage
==================================================

=== Test Docker ===
✓ Docker: Docker version 24.0.5, build ced0996

=== Test docker-compose ===
✓ docker-compose: Docker Compose version v2.20.2-desktop.1

=== Test Airflow ===
✓ Airflow: Version 3.0.6

=== Test Spark ===
✓ Spark (PySpark) disponible, version 4.0.2

Résultats: 4/4 tests réussis
```

### Tests de Qualité des Données

Évaluer les 3 frameworks de validation :

```bash
python spark/test_data_quality.py
```

**Frameworks testés** :
- ✅ **Great Expectations** : 4/4 validations
- ✅ **Soda Core** : 4/4 checks  
- ✅ **Pandera** : Validation schéma complète

### Data Profiling

Générer des rapports de profiling :

```bash
python spark/data_profiling.py
```

**Sorties** :
- Rapport HTML interactif (ydata-profiling)
- Statistiques détaillées (profiling manuel)
- Fichier : `spark/profiles/profile_YYYYMMDD_HHMMSS.html`

### Initialiser dbt

Setup complet du projet dbt :

```bash
python dbt_setup.py
```

**Crée** :
- Structure dbt complète
- Fichiers de configuration
- Modèles d'exemple
- Data catalog

### Lancer Airflow Localement

```bash
# Terminal 1: Scheduler
source airflow_env/bin/activate
airflow scheduler

# Terminal 2: Webserver (optionnel, port 8080)
source airflow_env/bin/activate
airflow webserver --port 8080

# Terminal 3: API Server
source airflow_env/bin/activate
PATH=$PWD/airflow_env/bin:$PATH airflow_env/bin/airflow api-server --dev
```

Accès :
- **Webserver** : http://localhost:8080
- **API Server** : http://localhost:8080/api/v1

## 📊 Frameworks de Qualité Intégrés

### Great Expectations

**Cas d'usage** : Validation de schémas, assertions de données

**Checks inclus** :
- Vérification du nombre de lignes
- Détection de valeurs nulles
- Validation de sets de valeurs
- Regex sur colonnes texte

**Fichier de test** : `spark/test_data_quality.py::test_great_expectations()`

### Soda Core

**Cas d'usage** : Monitoring continu de la qualité

**Checks inclus** :
- Comptage des valeurs manquantes
- Validation de ranges numériques
- Vérification de taille dataset
- Détection d'aberrantes

**Fichier de test** : `spark/test_data_quality.py::test_soda()`

### Pandera

**Cas d'usage** : Validation de schémas DataFrame avec type checking

**Features** :
- Validation des types colonnes
- Contraintes complexes
- Regex et custom checks
- Gestion d'index

**Fichier de test** : `spark/test_data_quality.py::test_pandera()`

## 🔄 Flux de Données Recommandé

```
1. Extract (PySpark) → Données brutes
           ↓
2. Transform (dbt) → Modèles staging/marts
           ↓
3. Validate (Great Expectations/Soda/Pandera) → Qualité check
           ↓
4. Profile (ydata-profiling) → Insights
           ↓
5. Load (Spark) → Data Lake/Warehouse
```

## 📈 Semaines du Projet

### Semaine 1 : Fondations
- ✅ Setup Airflow 3.0.6
- ✅ Installation PySpark 4.0.2
- ✅ Configuration Docker Compose
- ✅ Tests de connectivité

### Semaine 2 : Enrichissement
- ✅ Intégration dbt Core + Data Catalog
- ✅ Implémentation 3 frameworks qualité
- ✅ Data profiling avec ydata-profiling
- ✅ Nettoyage projet

### Semaine 3+ : Automation
- [ ] DAGs avancés Airflow+dbt
- [ ] Dashboards de qualité
- [ ] CI/CD automatisé
- [ ] Monitoring en temps réel

Voir [rapport_semaine1.md](rapport_semaine1.md) et [rapport_semaine2.md](rapport_semaine2.md) pour détails.

## 🐛 Troubleshooting

### Problème : "airflow: command not found"

**Solution** :
```bash
source airflow_env/bin/activate
# ou
/Users/reema/Desktop/projet_stage/airflow_env/bin/airflow --version
```

### Problème : Pandera validation fails

**Solution** :
```bash
pip install --upgrade pyarrow==16.0.0
```

### Problème : Port 8080 already in use

**Solution** :
```bash
airflow webserver --port 8081
# ou
lsof -i :8080  # Trouver le processus
kill -9 <PID>  # Tuer le processus
```

### Problème : Great Expectations import error

**Solution** :
```bash
pip install --upgrade great-expectations
```

## 🤝 Contribution

Pour contribuer au projet :

1. **Fork** le repository
2. **Créer une branche** : `git checkout -b feature/my-feature`
3. **Committer les changements** : `git commit -am 'Add feature'`
4. **Pusher la branche** : `git push origin feature/my-feature`
5. **Ouvrir une Pull Request**

### Standards de Code

- Python PEP 8 compliant
- Docstrings pour toutes les fonctions
- Tests pour nouvelles features
- Rapports commit clairs et détaillés

## 📚 Ressources

- [Apache Airflow Docs](https://airflow.apache.org/docs/)
- [Apache Spark Docs](https://spark.apache.org/docs/)
- [dbt Documentation](https://docs.getdbt.com/)
- [Great Expectations](https://docs.greatexpectations.io/)
- [Soda Core Documentation](https://docs.soda.io/)
- [Pandera Docs](https://pandera.readthedocs.io/)

## 📝 Licence

Ce projet est sous licence MIT. Voir `LICENSE` file pour détails.

## ✨ Auteur

Développé comme projet de stage pour l'ingénierie des données.

---

**Last Updated**: Mars 17, 2026  
**Status**: Production-Ready  
**Python Version**: 3.9+  
**Airflow Version**: 3.0.6
