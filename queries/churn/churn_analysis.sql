-- churn_analysis.sql
-- Churn definition and early warning signals
-- Author: Priyanka Sinha
-- Definition: A customer is churned if they have not placed an order
--             in the last 90 days and had at least 2 prior orders.

WITH last_order AS (
    SELECT
        user_id,
        MAX(order_date)         AS last_order_date,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(revenue)             AS total_revenue,
        AVG(revenue)             AS avg_order_value,
        MIN(order_date)          AS first_order_date
    FROM orders
    GROUP BY user_id
),

churn_flags AS (
    SELECT
        user_id,
        last_order_date,
        total_orders,
        total_revenue,
        avg_order_value,
        first_order_date,
        DATEDIFF('day', last_order_date, CURRENT_DATE)  AS days_since_last_order,
        CASE
            WHEN DATEDIFF('day', last_order_date, CURRENT_DATE) > 90
             AND total_orders >= 2
            THEN 1 ELSE 0
        END                                              AS is_churned,
        CASE
            WHEN DATEDIFF('day', last_order_date, CURRENT_DATE) BETWEEN 60 AND 90
            THEN 1 ELSE 0
        END                                              AS at_risk
    FROM last_order
),

-- ── Inter-purchase time stats ──────────────────────────────────
inter_purchase AS (
    SELECT
        user_id,
        AVG(days_between)   AS avg_days_between_orders,
        STDDEV(days_between) AS stddev_days_between
    FROM (
        SELECT
            user_id,
            DATEDIFF('day',
                LAG(order_date) OVER (PARTITION BY user_id ORDER BY order_date),
                order_date
            ) AS days_between
        FROM orders
    ) ipt
    WHERE days_between IS NOT NULL
    GROUP BY user_id
)

SELECT
    cf.*,
    ip.avg_days_between_orders,
    ip.stddev_days_between,
    -- Churn risk score: how many average cycles have been missed?
    CASE
        WHEN ip.avg_days_between_orders > 0
        THEN ROUND(cf.days_since_last_order / ip.avg_days_between_orders, 2)
        ELSE NULL
    END                                                  AS missed_cycles,
    CASE
        WHEN cf.days_since_last_order > ip.avg_days_between_orders * 2
        THEN 'High Risk'
        WHEN cf.days_since_last_order > ip.avg_days_between_orders * 1.5
        THEN 'Medium Risk'
        ELSE 'Low Risk'
    END                                                  AS churn_risk_band
FROM churn_flags cf
LEFT JOIN inter_purchase ip USING (user_id)
ORDER BY missed_cycles DESC NULLS LAST;
