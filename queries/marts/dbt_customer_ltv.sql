-- dbt_models/marts/customer_ltv.sql
-- Author: Priyanka Sinha
-- Description: Customer LTV mart — production-grade dbt model
--              with full documentation, tests, and lineage.

{{ config(
    materialized = 'table',
    sort          = 'first_order_date',
    dist          = 'user_id',
    tags          = ['marts', 'customer', 'ltv']
) }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
    WHERE order_status = 'completed'
),

order_stats AS (
    SELECT
        user_id,
        MIN(order_date)                                     AS first_order_date,
        MAX(order_date)                                     AS last_order_date,
        COUNT(DISTINCT order_id)                            AS total_orders,
        SUM(revenue)                                        AS total_revenue,
        AVG(revenue)                                        AS avg_order_value,
        DATEDIFF('day', MIN(order_date), MAX(order_date))   AS lifespan_days,
        DATEDIFF('day', MAX(order_date), CURRENT_DATE)      AS days_since_last_order
    FROM orders
    GROUP BY user_id
)

SELECT
    user_id,
    first_order_date,
    last_order_date,
    total_orders,
    ROUND(total_revenue, 2)                                 AS total_revenue,
    ROUND(avg_order_value, 2)                               AS avg_order_value,
    lifespan_days,
    days_since_last_order,
    -- Projected annual LTV based on purchase frequency
    ROUND(
        avg_order_value
        * CASE
            WHEN lifespan_days > 0
            THEN (total_orders::FLOAT / lifespan_days) * 365
            ELSE total_orders
          END,
    2)                                                      AS projected_annual_ltv,
    -- LTV tier classification
    CASE
        WHEN total_revenue >= 10000 THEN 'Platinum'
        WHEN total_revenue >= 5000  THEN 'Gold'
        WHEN total_revenue >= 1000  THEN 'Silver'
        ELSE 'Bronze'
    END                                                     AS ltv_tier,
    CURRENT_TIMESTAMP                                       AS dbt_updated_at
FROM order_stats
