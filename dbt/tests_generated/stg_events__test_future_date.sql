-- Proposé par Codestral (cible : future_date) — À REVOIR avant de copier dans dbt/tests/ (ne s'exécute pas automatiquement)
SELECT * FROM {{ ref('stg_events') }} WHERE timestamp > CURRENT_TIMESTAMP