-- int_sales_daily.sql
-- Intermediate model: daily aggregations by sale_date / region / channel
-- Materialized as view (see dbt_project.yml)

with stg as (
    select * from "dev"."main"."stg_sales"
),

daily as (
    select
        sale_date,
        region,
        channel,
        count(distinct order_id)    as nb_orders,
        count(distinct customer_id) as nb_customers,
        sum(quantity)               as total_units,
        round(sum(revenue), 2)      as total_revenue,
        round(avg(revenue), 2)      as avg_order_value
    from stg
    group by
        sale_date,
        region,
        channel
)

select * from daily