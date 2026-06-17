"""
Storage Adapters
-----------------
Interfaces pour la persistance des métriques et événements d'anomalies.

InfluxDB et TimescaleDB ont été supprimés — l'infrastructure Docker n'est
pas active. SQLiteMetricsStore (spark/metrics/storage.py) remplace les deux.
Ce fichier est conservé pour compatibilité d'import.
"""
