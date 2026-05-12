-- sales_summary.sql
-- Mart model: final sales summary with running totals
-- Materialized as table (see dbt_project.yml)

with daily as (
    select * from "dev"."main"."int_sales_daily"
),

summary as (
    select
        sale_date,
        region,
        channel,
        nb_orders,
        nb_customers,
        total_units,
        total_revenue,
        avg_order_value,

        -- Running revenue total per region (ordered by date)
        round(
            sum(total_revenue) over (
                partition by region
                order by sale_date
                rows between unbounded preceding and current row
            ),
        2) as cumulative_revenue_by_region,

        -- Revenue share per channel within each day
        round(
            total_revenue / nullif(
                sum(total_revenue) over (partition by sale_date),
                0
            ) * 100,
        2) as revenue_pct_of_day

    from daily
)

select * from summary