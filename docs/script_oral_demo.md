# Script oral — Démonstration Data Trust Agent (PFA)

> Durée estimée : 12–15 minutes  
> Lancer `bash demo_film.sh` dans le terminal avant de commencer.  
> Chaque section se termine par **[Entrée]** — vous contrôlez le rythme.

---

## Introduction (avant de lancer le script)

> *Montrer la slide d'architecture*

« Notre projet répond à un problème concret : dans une plateforme data moderne, les données peuvent se dégrader silencieusement — doublons, valeurs nulles, chutes de volume — sans que personne ne s'en aperçoive avant qu'il soit trop tard.

Le Data Trust Agent est le système que nous avons conçu pour surveiller automatiquement ces pipelines, détecter les anomalies en temps réel, les expliquer en langage naturel grâce à un LLM, et déclencher les actions correctives — le tout sans intervention humaine.

Je vais vous montrer chaque composant en fonctionnement réel. »

---

## Étape 1 — Infrastructure Docker

« On commence par lancer l'infrastructure. Quatre services démarrent en parallèle : la Metrics API qui centralise toutes nos métriques, Prometheus qui les scrape toutes les 15 secondes, AlertManager qui gère le routage des alertes, et InfluxDB comme base time-series.

En quelques secondes, tout est opérationnel. On voit déjà les métriques historiques en base — accumulées depuis le début du projet — ainsi que les alertes non encore traitées. »

---

## Étape 2 — Dashboard Streamlit

> *Le navigateur s'ouvre automatiquement sur http://localhost:8501*

« Le Dashboard Streamlit est le point d'entrée visuel du système. Il donne une vue immédiate de la santé de chaque pipeline.

On voit ici le **score qualité** — un indicateur entre 0 et 1 calculé automatiquement. Un pipeline sans anomalie est à 1.0. Quand des anomalies CRITICAL sont détectées, le score chute. C'est une synthèse instantanée de la confiance qu'on peut accorder aux données.

Le dashboard comporte 5 pages : vue d'ensemble, liste des anomalies avec explications LLM, graphiques temporels, une page d'injection interactive pour les démos en direct, et la gestion des règles générées par le LLM. »

---

## Étape 3 — Données source : WideWorldImporters

« Avant de montrer la détection, il faut comprendre d'où viennent les données. Nous utilisons WideWorldImporters, une base fictive de ventes avec plus de 113 000 lignes, stockée dans DuckDB.

Ces données transitent à travers une pipeline dbt : d'abord les modèles de staging qui nettoient le brut, ensuite les modèles intermédiaires qui agrègent, et enfin les marts qui exposent les tables analytiques finales. C'est ce flux que le Data Trust Agent surveille. »

---

## Étape 4 — Injection d'anomalies

« Pour simuler ce qui se passe en production, on injecte des anomalies contrôlées.

Sur **clean_sales** : on introduit des valeurs nulles et des montants hors plage à 15% des lignes — comme si un fichier source arrivait corrompu.

Sur **ingest_sales** : on injecte des doublons à 10% — comme si un système amont avait envoyé le même batch deux fois.

C'est exactement ce type d'incidents qui arrive en production et qui passe inaperçu sans un système de surveillance. »

---

## Étape 5 — Détection multi-algorithmes

« C'est le cœur du système. La détection s'effectue en deux couches.

La première couche applique quatre algorithmes classiques en parallèle :
- Les **seuils métier** : si le taux de rejet dépasse 20%, c'est une violation immédiate.
- Le **z-score** : il détecte les déviations statistiques subtiles qui ne violent pas de seuil fixe.
- L'**IQR** : robuste aux valeurs extrêmes, il identifie les outliers par quartiles.
- L'analyse de **tendance** : elle capte les dégradations progressives sur plusieurs jours.

La deuxième couche est un détecteur ML multi-couches — VolumeDetector, SchemaDetector, TemporalDetector — qui identifie des patterns plus complexes comme des corrélations ou des anomalies saisonnières.

On voit ici le nombre d'anomalies détectées et leur répartition par sévérité : critical, warning, info. »

---

## Étape 6 — Explication LLM (Mistral AI)

« Une fois l'anomalie détectée, un opérateur a besoin de comprendre ce qui se passe — pas juste voir un chiffre rouge. C'est là qu'intervient le LLM.

Nous utilisons **Mistral AI**, hébergé en France par OVHcloud, ce qui garantit la conformité RGPD — aucune donnée ne sort du territoire européen. En fallback, un modèle Ollama local peut prendre le relais, et si aucun LLM n'est disponible, un template rule-based génère une explication sans appel réseau — le système n'est jamais bloquant.

Regardez les explications générées : pour le taux de rejet à 45%, le LLM identifie la cause probable, la relie au contexte du pipeline, et propose une action corrective immédiate. En trois phrases, l'opérateur sait quoi faire. »

---

## Étape 7 — Alerting : règles + 4 canaux

« Le système d'alerte est entièrement configurable. On définit des règles dans un AlertRuleStore SQLite : pour chaque combinaison pipeline/métrique/sévérité, on précise vers quels canaux envoyer la notification.

Ici on voit trois règles : une règle wildcard qui attrape toutes les alertes CRITICAL et les envoie en console et vers AlertManager. Une règle spécifique pour le taux de rejet sur clean_sales, qui envoie en console, Teams et Email. Et une règle pour la chute de volume sur ingest_sales.

Le système supporte **quatre canaux** :
- **Console** : toujours disponible, fallback garanti
- **Teams** : webhook Microsoft, Adaptive Card formatée
- **Email** : SMTP standard avec HTML
- **AlertManager** : envoi direct vers Prometheus AlertManager qui gère ensuite le routage selon alertmanager.yml

La protection **cooldown** évite le spam : si la même alerte est déjà été envoyée dans la fenêtre configurée — ici 30 minutes — la deuxième tentative est bloquée automatiquement. »

---

## Étape 8 — Gestion des incidents

« Chaque anomalie peut générer un incident. L'IncidentManager gère leur cycle de vie complet.

On crée trois incidents de sévérités différentes. L'incident sur clean_sales passe en statut *investigating* — un opérateur a pris la main. L'incident CRITICAL sur aggregate_sales est escaladé automatiquement d'un cran de sévérité.

Le système détecte aussi la **récurrence** : si un même pipeline génère au moins 3 incidents en moins d'une heure, il est marqué comme instable. C'est ce qu'on voit ici pour clean_sales.

Enfin, une fois le problème résolu, on ferme l'incident avec une note de résolution. Le résumé final montre combien sont ouverts, en cours d'investigation, résolus. »

---

## Étape 9 — Runbooks automatiques

« Pour chaque type d'incident, le système dispose de runbooks : des procédures de remédiation pas à pas.

Il y a cinq runbooks pré-chargés, couvrant les cas les plus fréquents : chute de volume, taux de rejet élevé, durée anormale, schéma modifié, job en échec.

Le RunbookEngine suggère automatiquement le bon runbook selon la métrique en anomalie. Ici, pour une anomalie sur rejection_rate_pct, il trouve le runbook dédié et liste les 7 étapes à exécuter. En mode production, ces étapes avec des commandes shell s'exécutent réellement.

La fonctionnalité la plus innovante : pour un pipeline récidiviste, le système génère dynamiquement un nouveau runbook personnalisé, enregistré en base, prêt à être exécuté automatiquement aux prochains incidents. »

---

## Étape 10 — Data Catalog

« Le Data Catalog est la mémoire du système. Il maintient une fiche pour chaque pipeline et modèle dbt : description, propriétaire, tags, et surtout le **score qualité**.

On voit les 5 entrées publiées : les pipelines Spark à 0.97 et les modèles dbt à 1.0.

Quand des anomalies sont détectées sur clean_sales — une CRITICAL et deux autres — le score chute de 0.97 à 0.44. Des incidents sont créés automatiquement dans le catalog. Une fois ces incidents résolus, le score remonte à 1.0. C'est un indicateur de confiance dynamique, recalculé en continu.

Le lineage dbt est aussi parsé depuis le manifest.json : on voit les dépendances entre modèles — stg_sales alimente int_sales_daily qui alimente sales_summary. »

---

## Étape 11 — Prometheus, AlertManager, Webhook

« Pour l'observabilité standard, le PrometheusExporter traduit nos métriques SQLite au format text/plain de Prometheus. Chaque série porte des labels pipeline et run_date qui permettent de filtrer dans Grafana.

Prometheus scrape cet endpoint toutes les 15 secondes et évalue les règles définies dans prometheus_rules.yml. Quand une règle se déclenche — par exemple rejection_rate_pct > 20% — AlertManager reçoit l'alerte, applique ses règles de routage, et la renvoie vers notre API via webhook.

On le démontre en direct : on envoie une alerte au webhook, et on vérifie qu'elle est reçue et persistée en SQLite. Le flux complet est opérationnel : Prometheus → règle → AlertManager → webhook → API → SQLite.

> *Ouvrir http://localhost:9090/graph et taper : data_trust_job_rejection_rate_pct*
> *Montrer la courbe qui monte au moment de l'injection*

> *Ouvrir http://localhost:9093 — montrer les alertes actives*

> *Ouvrir http://localhost:8090/docs — exécuter POST /api/v1/alerts/detect en direct* »

---

## Étape 12 — Livrable Semaine 14

« Ce livrable démontre le système d'alerting dans sa totalité en 5 sous-scénarios enchaînés : configuration des règles, simulation de trois alertes de sévérités croissantes, envoi multi-canaux, création et escalade des incidents, et exécution du runbook adapté. »

---

## Étape 13 — Livrable Semaine 15

« Ce dernier livrable est l'intégration end-to-end en 5 scénarios :
1. Chute de volume critique de 98% — du métrique à la résolution en 5 étapes
2. Taux de rejet élevé — de la règle à l'escalade automatique
3. Pipeline récidiviste — détection de récurrence et runbook dynamique
4. Exposition Prometheus — format complet avec labels
5. Data Catalog — cycle complet dégradation/rétablissement du score qualité »

---

## Conclusion

> *Revenir sur le dashboard Streamlit*

« Ce que vous venez de voir, c'est un système qui se surveille, s'explique et se corrige lui-même.

En combinant **détection multi-couches** — seuils, statistiques et ML —, **explications LLM** conformes RGPD, **gestion automatique des incidents** avec escalade et runbooks, et **observabilité standard** Prometheus/AlertManager, le Data Trust Agent transforme une plateforme data classique en un système de confiance.

Il s'intègre nativement à n'importe quelle infrastructure existante : Airflow, Spark, dbt, Prometheus, Teams, Email — aucune dépendance propriétaire, tout est standard. »

---

## Commandes de référence rapide (pendant la démo)

```bash
# Relancer la détection manuellement
curl -X POST http://localhost:8090/api/v1/alerts/detect

# Voir le résumé des alertes
curl http://localhost:8090/api/v1/alerts/summary

# Injecter une anomalie en direct
.venv/bin/python inject_to_db.py --pipeline clean_sales --types volume_drop --rate 0.5

# Voir les métriques Prometheus
curl http://localhost:8090/metrics | grep data_trust
```
