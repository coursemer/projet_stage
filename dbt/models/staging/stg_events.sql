-- Staging model: Raw data cleanup
{{ config(
    materialized='view',
    schema='dbt_staging'
) }}

SELECT
    {{ dbt_utils.surrogate_key(['id']) }} as surrogate_key,
    *
FROM {{ source('raw', 'events') }}
WHERE _etl_loaded_at IS NOT NULL
