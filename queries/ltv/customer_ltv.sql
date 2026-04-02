-- customer_ltv.sql
-- Customer Lifetime Value modelling using historical order data
-- Author: Priyanka Sinha

-- ── Basic LTV: total historical revenue per customer ──────────
WITH order_stats AS (
    SELECT
        user_id,
        MIN(order_date)                             AS first_order_date,
        MAX(order_date)                             AS last_order_date,
        COUNT(DISTINCT order_id)                    AS total_orders,
        SUM(revenue)                                AS total_revenue,
        AVG(revenue)                                AS avg_order_value,
        STDDEV(revenue)                             AS revenue_stddev,
        DATEDIFF('day', MIN(order_date),
                        MAX(order_date))            AS customer_lifespan_days,
        COUNT(DISTINCT DATE_TRUNC('month',
              order_date))                          AS active_months
    FROM orders
    GROUP BY user_id
),

-- ── Purchase frequency and recency ────────────────────────────
rfm AS (
    SELECT
        user_id,
        DATEDIFF('day', MAX(order_date),
                        CURRENT_DATE)              AS recency_days,
        COUNT(DISTINCT order_id)                   AS frequency,
        SUM(revenue)                               AS monetary
    FROM orders
    GROUP BY user_id
),

-- ── LTV segments based on RFM quintiles ───────────────────────
rfm_scored AS (
    SELECT
        user_id,
        recency_days,
        frequency,
        monetary,
        NTILE(5) OVER (ORDER BY recency_days DESC)  AS r_score,
        NTILE(5) OVER (ORDER BY frequency ASC)      AS f_score,
        NTILE(5) OVER (ORDER BY monetary ASC)       AS m_score
    FROM rfm
)

SELECT
    os.user_id,
    os.first_order_date,
    os.last_order_date,
    os.total_orders,
    ROUND(os.total_revenue, 2)                      AS total_revenue,
    ROUND(os.avg_order_value, 2)                    AS avg_order_value,
    os.customer_lifespan_days,
    os.active_months,
    rfm.recency_days,
    rfm.frequency,
    rfm.r_score,
    rfm.f_score,
    rfm.m_score,
    rfm.r_score + rfm.f_score + rfm.m_score        AS rfm_total_score,
    CASE
        WHEN rfm.r_score + rfm.f_score + rfm.m_score >= 12 THEN 'Champions'
        WHEN rfm.r_score + rfm.f_score + rfm.m_score >= 9  THEN 'Loyal'
        WHEN rfm.r_score + rfm.f_score + rfm.m_score >= 6  THEN 'At Risk'
        ELSE 'Churned'
    END                                             AS ltv_segment,
    -- Rolling 12-month revenue as forward-looking LTV proxy
    ROUND(
        os.avg_order_value
        * (os.total_orders::FLOAT / NULLIF(os.customer_lifespan_days, 0) * 365),
    2)                                              AS projected_annual_ltv
FROM order_stats os
JOIN rfm_scored rfm USING (user_id)
ORDER BY projected_annual_ltv DESC NULLS LAST;
