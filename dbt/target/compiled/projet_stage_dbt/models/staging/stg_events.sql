-- Staging model: Raw data cleanup


SELECT
    md5(cast(coalesce(cast(id as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as surrogate_key,
    *
FROM "dev"."raw"."events"
WHERE _etl_loaded_at IS NOT NULL