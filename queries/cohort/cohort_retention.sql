-- cohort_retention.sql
-- Weekly cohort retention matrix
-- Author: Priyanka Sinha
-- Description: Calculates user retention by acquisition cohort.
--              Useful for understanding long-term customer value and churn patterns.

WITH cohort_base AS (
    -- Assign each user to their acquisition cohort (week of first order)
    SELECT
        user_id,
        DATE_TRUNC('week', first_order_date)::DATE  AS cohort_week,
        DATE_TRUNC('week', activity_date)::DATE     AS activity_week
    FROM orders o
    JOIN (
        SELECT
            user_id,
            MIN(order_date) AS first_order_date
        FROM orders
        GROUP BY user_id
    ) cohorts USING (user_id)
    CROSS JOIN LATERAL (
        SELECT DISTINCT DATE_TRUNC('week', order_date)::DATE AS activity_date
        FROM orders o2
        WHERE o2.user_id = o.user_id
    ) activity
),

cohort_size AS (
    -- Count users per cohort
    SELECT
        cohort_week,
        COUNT(DISTINCT user_id) AS cohort_users
    FROM cohort_base
    GROUP BY cohort_week
),

retention AS (
    -- Count active users per cohort per period
    SELECT
        cohort_week,
        DATEDIFF('week', cohort_week, activity_week) AS period_number,
        COUNT(DISTINCT user_id)                       AS active_users
    FROM cohort_base
    GROUP BY 1, 2
)

SELECT
    r.cohort_week,
    r.period_number,
    r.active_users,
    cs.cohort_users,
    ROUND(100.0 * r.active_users / NULLIF(cs.cohort_users, 0), 1) AS retention_pct
FROM retention r
JOIN cohort_size cs USING (cohort_week)
WHERE r.period_number <= 12  -- Show first 12 weeks
ORDER BY r.cohort_week, r.period_number;
