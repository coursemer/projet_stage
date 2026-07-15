# Guide de démonstration — Data Trust Agent

## Présentation générale

Le **Data Trust Agent** est un système de surveillance de la qualité des données pour des pipelines de données industriels (Airflow + Spark + dbt). Il détecte automatiquement les anomalies, génère des alertes, crée des incidents, suggère des runbooks de résolution et expose des métriques à Prometheus.

**Pour lancer toute la démo :**
```bash
bash demo.sh
```

---

## Architecture du système

```
Pipelines de données
  (Airflow / Spark / dbt)
         │
         ▼
  SQLite metrics.db          ← stockage central des métriques
         │
    ┌────┴──────────────────────────────────┐
    │                                       │
    ▼                                       ▼
Anomaly Detector                   Prometheus Exporter
(seuils + ML)                      GET /metrics
    │                                       │
    ▼                                       ▼
Alert Manager                       Prometheus
(alertes SQLite)                    (scrape toutes les 15s)
    │                                       │
    ▼                                       ▼
NotificationService              AlertManager
(console / Teams / Email)        (routage + webhook)
    │                                       │
    ▼                                       ▼
IncidentManager              POST /api/v1/alerts/webhook
(open → investigating → resolved)           │
    │                                       ▼
    ▼                               Alert persist SQLite
RunbookEngine
(5 runbooks pré-chargés)
    │
    ▼
DataCatalog
(score qualité 0.0–1.0)
    │
    ▼
Dashboard Streamlit
http://localhost:8501
```

Le schéma ci-dessus représente le flux de données complet du Data Trust Agent, organisé en deux branches parallèles qui convergent vers un point central.

**Point d'entrée — les pipelines de données.** Trois systèmes produisent des données : Apache Airflow orchestre les workflows, Apache Spark transforme les volumes de données, et dbt matérialise les modèles analytiques. Toutes leurs métriques d'exécution (nombre de lignes, taux de rejet, durée, taux de nulls…) sont centralisées dans une base SQLite appelée `metrics.db`. C'est le cœur du système.

**Branche gauche — détection et gestion des incidents.** L'`Anomaly Detector` lit les métriques et applique quatre algorithmes en parallèle : seuils fixes métier, z-score statistique, IQR (détection par quartiles), et analyse de tendance. Quand une anomalie est détectée, elle devient une alerte stockée par l'`Alert Manager`. La `NotificationService` achemine alors la notification vers les canaux configurés (console en démo, Teams ou Email en production). L'`IncidentManager` prend le relais pour gérer le cycle de vie de l'incident (ouvert → en investigation → résolu), y compris l'escalade automatique de sévérité. Le `RunbookEngine` suggère les étapes de remédiation adaptées parmi cinq runbooks pré-chargés. Enfin, le `DataCatalog` calcule un score qualité de 0 à 1 par pipeline, synthèse de tous les incidents survenus.

**Branche droite — observabilité standard.** Le `Prometheus Exporter` traduit les métriques SQLite au format texte Prometheus et les expose sur l'endpoint `GET /metrics`. Prometheus scrape cet endpoint toutes les 15 secondes et évalue les règles d'alerte définies dans `prometheus_rules.yml`. Quand une règle se déclenche (ex. taux de rejet > 20%), AlertManager reçoit l'alerte, applique ses règles de routage et de silence, puis la renvoie vers l'API via `POST /api/v1/alerts/webhook`. Ce webhook persiste l'alerte dans SQLite, fermant la boucle avec la branche gauche.

**Point de convergence — le Dashboard Streamlit.** Les deux branches alimentent le même `metrics.db`. Le Dashboard Streamlit lit cette base en temps réel et présente une vue unifiée : santé des pipelines, liste des anomalies avec explications LLM, graphiques temporels, interface d'injection de test, et workflow d'approbation des règles.

---

## Étapes de la démo et leurs résultats

### Étape 0 — Vérification des prérequis

**Affichage attendu :**
```
  ✔  Python   : Python 3.14.4
  ✔  Docker   : 24.0.5
  ✔  Streamlit: Streamlit, version 1.58.0
```

**Ce que ça signifie :** les trois composants nécessaires sont présents. Si l'un échoue, un message d'erreur explicite indique comment corriger.

**Ce qu'il faut dire :** *"Le projet tourne entièrement en local. Pas de cloud, pas de credentials, RGPD-compatible."*

---

### Étape 1 — Docker Stack

**Affichage attendu :**
```
  ✔  docker compose up -d
  ·  attente metrics-api:8090              OK (0s)
  ·  attente prometheus:9090               OK (0s)
  ·  attente alertmanager:9093             OK (0s)
  ·  métriques en base : 861
  ·  alertes en base   : 115  (unack=115)
```

**Ce que ça signifie :**
- **OK (0s)** → les services étaient déjà actifs (ou ont démarré instantanément grâce à l'image pré-buildée)
- **861 métriques** → points de données historiques accumulés depuis le début du projet (injections précédentes)
- **115 alertes (unack=115)** → alertes non encore acquittées. Un opérateur doit les traiter. C'est normal en début de démo.

**Services lancés :**

| Service | Port | Rôle |
|---------|------|------|
| `metrics-api` | 8090 | API REST FastAPI — reçoit les métriques et les alertes |
| `prometheus` | 9090 | Collecte et stocke les métriques au format time-series |
| `alertmanager` | 9093 | Gère le routage et l'envoi des alertes |
| `influxdb` | 8086 | Base time-series disponible |

**URLs à montrer :**
- `http://localhost:9090` → Prometheus UI, **Status > Targets** : voir 3 targets en `up`
- `http://localhost:9093` → AlertManager UI : voir les alertes actives
- `http://localhost:8090/docs` → Swagger UI de l'API FastAPI

---

### Étape 2 — Dashboard Streamlit

**Affichage attendu :**
```
  ·  attente streamlit:8501                OK (3s)
  ✔  Dashboard Streamlit ouvert → http://localhost:8501  (PID=12345)
```

**Le navigateur s'ouvre automatiquement sur `http://localhost:8501`.**

#### Ce qu'on voit dans le dashboard

**Page 1 — Vue d'ensemble**

```
Pipeline         Score qualité   Anomalies   Incidents ouverts
clean_sales      0.44            8           2
ingest_sales     0.78            3           1
aggregate_sales  0.95            0           0
```

- Le **score qualité** va de 0.0 (très dégradé) à 1.0 (parfait). Il est recalculé automatiquement à chaque détection d'anomalie. `clean_sales` à 0.44 signifie que ce pipeline a eu des anomalies CRITICAL récentes.
- Les **anomalies** sont cumulées depuis la dernière exécution. 8 sur `clean_sales` = plusieurs métriques dépassent leur seuil.
- Les **incidents ouverts** = tickets non encore résolus.

**Page 2 — Anomalies**

Liste de toutes les alertes avec :
- `[CRITICAL]` / `[WARNING]` / `[INFO]` en couleur
- Nom de la métrique : ex. `clean_sales.rejection_rate_pct = 45.2`
- Algorithme qui a détecté : `threshold` / `zscore` / `iqr` / `trend`
- Explication LLM (Mistral AI) : *"Le taux de rejet de 45% dépasse largement le seuil de 20%. Cela peut indiquer un problème de qualité dans les données sources."*

**Page 3 — Trends**

Graphique temporel du `rows_output` ou `rejection_rate_pct`. On peut voir visuellement le moment où l'anomalie a été injectée : la courbe chute ou monte brutalement.

**Page 4 — Injection (démo interactive)**

C'est ici qu'on montre le système en action en direct :
1. Sélectionner `clean_sales`
2. Cocher `volume_drop`
3. Taux : 30%
4. Cliquer **Injecter**
5. Aller sur Page 1 → le score qualité a baissé, une nouvelle anomalie est apparue

**Page 5 — Règles**

Liste des règles de validation générées automatiquement par le LLM. On peut les approuver ou les rejeter. Les règles approuvées sont ensuite utilisées pour la détection.

---

### Étape 3 — Injection d'anomalies

**Affichage attendu :**
```
[2/4] Injection : nulls, out_of_range  @  15%…
      11 points écrits  (total DB : 872  avant : 861)
      3 alertes détectées  {'warning': 1, 'critical': 2}
      3 alertes sauvegardées dans alerts DB
  ✅  Injection terminée — 37,500 lignes affectées dans 2 type(s)

[2/4] Injection : duplicates  @  10%…
      11 points écrits  (total DB : 883  avant : 872)
      2 alertes détectées  {'critical': 2}
      2 alertes sauvegardées dans alerts DB
  ✅  Injection terminée — 5,000 lignes affectées dans 1 type(s)
```

**Ce que ça signifie ligne par ligne :**
- `11 points écrits` → 11 nouvelles métriques calculées depuis les données injectées (rows_output, rejection_rate_pct, null_rate_pct, duration_sec…)
- `total DB : 872  avant : 861` → compteur cumulatif. Chaque run ajoute des points.
- `3 alertes détectées {'critical': 2}` → le moteur de détection s'est déclenché automatiquement après l'écriture et a trouvé 2 violations critiques et 1 warning.
- `37,500 lignes affectées` → sur 50 000 lignes du CSV source, 37 500 ont été modifiées (15% × 2 types d'anomalies, avec chevauchement possible).

**Ce qu'il faut dire :** *"On simule ce qui arrive en production : un fichier source corrompu, des doublons issus d'un double envoi. Le système le détecte en temps réel."*

---

### Étape 4 — Détection d'anomalies via API

**Affichage attendu :**
```
  ✔  2 anomalies détectées → 2 sauvegardées
  ·  Par sévérité : {'critical': 2}

  ✔  Total alertes : 120   (unack=120)
     critical   : 63
     warning    : 46
     info       : 11
```

**Ce que ça signifie :**
- **2 nouvelles anomalies** détectées par le moteur de règles cette fois (en complément des 5 déjà trouvées à l'injection)
- **Total 120 alertes** → compteur cumulatif depuis le début du projet. En production, on les acquitterait au fil du traitement.
- **63 critical** → métriques qui dépassent les seuils métier définis dans `prometheus_rules.yml`

**Ce qu'il faut dire :** *"La détection est multi-couches : seuils fixes pour les violations évidentes, z-score pour les déviations statistiques subtiles, tendance pour les dégradations progressives."*

**À montrer :** `http://localhost:8090/docs` → exécuter `POST /api/v1/alerts/detect` en direct depuis le Swagger.

---

### Étape 5 — Métriques Prometheus

**Affichage attendu :**
```
  ✔  32 lignes méta + 8 séries exposées

  # HELP data_trust_alerts_total Nombre total d'alertes
  # TYPE data_trust_alerts_total gauge
  # HELP data_trust_alerts_unacknowledged Alertes non acquittées
  # TYPE data_trust_alerts_unacknowledged gauge
  data_trust_alerts_unacknowledged 120.0
  # HELP data_trust_job_rows_output Nombre de lignes produites par job
  # TYPE data_trust_job_rows_output gauge
  data_trust_job_rows_output{pipeline="clean_sales",run_date="2026-06-19"} 48320.0
  …

  Prometheus targets (3) :
  ✔  alertmanager              health=up
  ✔  data-trust-metrics        health=up
  ✔  prometheus                health=up
```

**Ce que ça signifie :**
- **Format Prometheus exposition** : chaque ligne est une métrique avec ses labels. Prometheus vient lire ce fichier toutes les 15 secondes.
- **`pipeline="clean_sales"` et `run_date="2026-06-19"`** → labels qui permettent de filtrer dans Prometheus / Grafana
- **3 targets en `up`** → Prometheus surveille lui-même (self-monitoring), AlertManager et notre API

**Ce qu'il faut montrer dans Prometheus :**
```
http://localhost:9090/graph
→ Taper : data_trust_job_rejection_rate_pct
→ Cliquer "Graph"
→ On voit la courbe monter quand l'anomalie a été injectée
```

---

### Étape 6 — AlertManager : alertes actives + webhook

**Affichage attendu :**
```
  ✔  2 alerte(s) active(s) dans AlertManager
     [warning ] ManyUnacknowledgedAlerts
     [critical] RejectionRateCritical — pipeline=clean_sales

  ✔  Webhook AlertManager → SQLite : reçu=1  persisté=1
```

**Ce que ça signifie :**
- **`ManyUnacknowledgedAlerts`** → règle Prometheus qui se déclenche quand >10 alertes ne sont pas acquittées. C'est attendu.
- **`RejectionRateCritical`** → règle `rejection_rate_pct > 20%` qui s'est déclenchée après l'injection
- **`Webhook reçu=1 persisté=1`** → test bout-en-bout : notre API a reçu l'alerte d'AlertManager et l'a enregistrée en base

**Flux complet démontré :**
```
Prometheus scrape → règle fire → AlertManager → webhook POST → metrics-api → SQLite
```

**Ce qu'il faut dire :** *"En production, au lieu du webhook local, AlertManager enverrait un email ou un message Teams. Il suffit de décommenter 3 lignes dans alertmanager.yml."*

---

### Étape 7 — Démo S14 : Alerting · Incidents · Runbooks

**Affichage attendu (extraits clés) :**
```
  3 règles configurées :
  ✅  [CRITICAL]  *             *                         → ['console']
  ✅  [HIGH    ]  clean_sales   job.rejection_rate_pct    → ['console', 'teams']
  ✅  [MEDIUM  ]  aggregate_sales job.duration_seconds    → ['console']

  📤  [HIGH    ]  canal=console   statut=sent
  ════════════════════════════════════════════
  🔔 ALERTE [HIGH]  2026-06-19 17:00 UTC
  Pipeline : clean_sales
  Métrique : job.rejection_rate_pct  =  45.2
  Détail   : Taux de rejet = 45% (seuil 20%)
  ════════════════════════════════════════════

  📤  [HIGH    ]  canal=teams     statut=sent
           → [dry-run] POST https://teams.webhook.example/hook — 594 bytes

  📋 Incident #02 créé — clean_sales
     Sévérité : HIGH  |  Statut : open

  ⚙️  Incident #2 → investigating
  ⬆️  Incident #3 escaladé → CRITICAL

  🔁 Récurrence clean_sales (3 incidents / 1h) : True

  [STEP] 1. Vérifier les logs du job dans spark/logs/
  [STEP] 2. Inspecter les données sources dans spark/data/raw/
  [STEP] 7. Corriger la source, relancer le pipeline, valider
```

**Ce que ça signifie ligne par ligne :**

- **Règles configurées** → le `AlertRuleStore` contient 3 règles. Le wildcard `*` sur pipeline et métrique signifie "toute alerte CRITICAL". La règle `clean_sales/rejection_rate_pct` est plus spécifique.
- **`canal=teams statut=sent [dry-run]`** → en mode démo, le POST vers Teams est simulé. En production, `dry_run=False` envoie vraiment.
- **`594 bytes`** → taille du payload JSON formaté en Adaptive Card Teams
- **`Incident #3 escaladé → CRITICAL`** → le moteur d'escalade a monté la sévérité d'un cran
- **`Récurrence : True`** → 3 incidents en moins d'1h sur le même pipeline → le système le détecte comme instable
- **`[STEP]`** → étapes du runbook recommandé. En exécution réelle (`dry_run=False`), les lignes `$ commande` sont exécutées automatiquement.

---

### Étape 8 — Démo S15 : 5 scénarios d'intégration

**Scénario 1 — Chute de volume critique**

```
  ✔  3 métriques écrites — clean_sales.rows_output = 850 (attendu ~45 000)
  ✔  Alerte CRITICAL sauvegardée
  ·  [CRITICAL] clean_sales — valeur=850 ([30 000 – 60 000]) | chute de 98%
  ✔  Incident #1 créé — statut : open | sévérité : HIGH
  ✔  Runbook trouvé : « Chute de volume de données »
  ·  [STEP] 1. Vérifier si le fichier source est vide…
  ✔  Incident résolu — statut : resolved
```

Ce qu'il faut dire : *"850 lignes au lieu de 45 000, c'est une chute de 98%. Le système détecte ça en moins d'une seconde, crée l'incident, trouve le bon runbook."*

**Scénario 5 — Data Catalog (le plus visuel)**

```
  ✔  Score initial : 0.97
  ✔  Score après anomalies : 0.440  ← dégradé
  ✔  [CRITICAL] Volume chute de 98 %
  ✔  [HIGH]     Taux de rejet 28.5 %
  ✔  2 incident(s) résolu(s)
  ✔  Score rétabli : 1.000
```

Ce qu'il faut dire : *"Le score qualité est une synthèse automatique de la santé du pipeline. Il chute de 0.97 à 0.44 à cause des anomalies, puis remonte à 1.0 une fois les incidents résolus."*

---

## Points clés à retenir pour la présentation

### Ce qui est innovant
- **Détection multi-couches** : seuils métier + statistiques (z-score, IQR) + tendance combinés
- **LLM embarqué** : Mistral AI (RGPD, hébergé en France) explique les anomalies en langage naturel
- **Runbooks automatiques** : générés dynamiquement pour les pipelines récidivistes
- **Zéro configuration cloud** : tout tourne en local ou on-premise
- **Stack standard** : Prometheus + AlertManager = intégration dans n'importe quelle infra existante

### Chiffres à citer
- 880+ métriques en base après injections
- 120+ alertes (63 critical, 46 warning, 11 info)
- 4 algorithmes de détection (threshold, zscore, IQR, trend)
- 5 runbooks pré-chargés (7 étapes chacun)
- 39 tests unitaires et d'intégration (S14 + S15)
- Démarrage du stack complet : < 10 secondes (image pré-buildée)

### Questions fréquentes

**"Pourquoi SQLite et pas PostgreSQL ?"**
SQLite suffit pour le démonstrateur et élimine une dépendance. En production, le `SQLiteMetricsStore` peut être remplacé par n'importe quel backend via l'interface commune.

**"Comment s'intègre le LLM ?"**
Mistral AI via API (RGPD-conforme, hébergé en France). Fallback Ollama local, puis template si aucun backend disponible. L'explication n'est jamais bloquante.

**"Les runbooks sont-ils manuels ?"**
Non : `register_recurring()` génère automatiquement un runbook personnalisé quand un pipeline est détecté comme récidiviste (≥3 incidents en 24h).

**"Peut-on brancher Teams ou Email réellement ?"**
Oui : dans `configs/alertmanager.yml`, décommenter les blocs `email_configs` ou le webhook Teams. Dans `NotificationService`, passer `dry_run=False`.

---

## Commandes de référence rapide

```bash
# Lancer toute la démo (tout-en-un)
bash demo.sh

# Dashboard uniquement
.venv/bin/streamlit run dashboard_agent.py

# Injecter des anomalies manuellement
.venv/bin/python inject_to_db.py --pipeline clean_sales --types nulls,out_of_range --rate 0.15

# Déclencher la détection
curl -X POST http://localhost:8090/api/v1/alerts/detect

# Voir les alertes
curl http://localhost:8090/api/v1/alerts/summary

# Arrêter tout
Ctrl+C              # stoppe Streamlit
docker compose down # stoppe le stack Docker
```

## URLs de démonstration

| Interface | URL | Ce qu'on y voit |
|-----------|-----|-----------------|
| Dashboard Streamlit | http://localhost:8501 | Santé pipelines, anomalies, injection |
| Metrics API (Swagger) | http://localhost:8090/docs | Tous les endpoints REST interactifs |
| Prometheus | http://localhost:9090 | Graph, targets, règles d'alerte |
| AlertManager | http://localhost:9093 | Alertes actives, routage, silences |
| InfluxDB | http://localhost:8086 | Base time-series |
