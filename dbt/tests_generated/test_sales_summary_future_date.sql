-- Proposé par Codestral (cible : future_date) — À REVOIR avant de copier dans dbt/tests/ (ne s'exécute pas automatiquement)
SELECT * FROM {{ ref('sales_summary') }} WHERE sale_date > CURRENT_DATE