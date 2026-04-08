-- stg_sales.sql
-- Staging model: clean and standardise raw sales data
-- Materialized as view (see dbt_project.yml)

with source as (
    select * from {{ source('raw', 'sales') }}
),

cleaned as (
    select
        order_id,
        customer_id,
        product_id,
        cast(sale_date as date)          as sale_date,
        cast(quantity as integer)        as quantity,
        cast(unit_price as numeric(10,2)) as unit_price,
        round(quantity * unit_price, 2)  as revenue,
        upper(trim(channel))             as channel,
        upper(trim(region))              as region,
        upper(trim(currency))            as currency,
        current_timestamp                as _loaded_at
    from source
    where order_id is not null
      and sale_date is not null
      and quantity > 0
      and unit_price > 0
)

select * from cleaned
