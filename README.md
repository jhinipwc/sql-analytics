# SQL Analytics Showcase

> Advanced SQL queries and dbt models for business analytics — cohort analysis, LTV modelling, funnel metrics, churn analysis, and KPI frameworks on public datasets.

## Overview

A curated collection of production-quality SQL patterns and dbt models covering the most common and complex business analytics use cases. Each query is documented with business context, explaining not just the SQL but why and when you'd use it.

## Tech Stack
`SQL` `dbt` `PostgreSQL` `Snowflake-compatible` `BigQuery-compatible`

## Project Structure
```
sql-analytics/
├── queries/
│   ├── cohort_analysis/
│   │   ├── weekly_cohorts.sql
│   │   └── retention_matrix.sql
│   ├── ltv_modelling/
│   │   ├── basic_ltv.sql
│   │   └── predictive_ltv_features.sql
│   ├── funnel_analysis/
│   │   ├── conversion_funnel.sql
│   │   └── drop_off_analysis.sql
│   ├── churn_analysis/
│   │   ├── churn_definition.sql
│   │   └── churn_risk_scoring.sql
│   └── kpi_frameworks/
│       ├── daily_kpis.sql
│       └── rolling_metrics.sql
├── dbt_models/
│   ├── staging/
│   ├── intermediate/
│   └── marts/
│       ├── customer_ltv.sql
│       ├── cohort_retention.sql
│       └── funnel_metrics.sql
├── README.md
└── dbt_project.yml
```

## Example Queries

### Cohort Retention Matrix
```sql
WITH cohort_base AS (
    SELECT
        user_id,
        DATE_TRUNC('month', first_purchase_date) AS cohort_month,
        DATE_TRUNC('month', order_date)          AS activity_month
    FROM orders o
    JOIN (
        SELECT user_id, MIN(order_date) AS first_purchase_date
        FROM orders GROUP BY user_id
    ) first_orders USING (user_id)
),
cohort_size AS (
    SELECT cohort_month, COUNT(DISTINCT user_id) AS cohort_users
    FROM cohort_base GROUP BY cohort_month
),
retention AS (
    SELECT
        cohort_month,
        DATEDIFF('month', cohort_month, activity_month) AS period_number,
        COUNT(DISTINCT user_id) AS active_users
    FROM cohort_base GROUP BY 1, 2
)
SELECT
    r.cohort_month,
    r.period_number,
    r.active_users,
    cs.cohort_users,
    ROUND(100.0 * r.active_users / cs.cohort_users, 1) AS retention_rate
FROM retention r
JOIN cohort_size cs USING (cohort_month)
ORDER BY 1, 2;
```

### Rolling 30-Day LTV
```sql
SELECT
    user_id,
    order_date,
    SUM(revenue) OVER (
        PARTITION BY user_id
        ORDER BY order_date
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) AS rolling_30d_ltv
FROM orders
ORDER BY user_id, order_date;
```

## dbt Model Example
```sql
-- marts/customer_ltv.sql
{{ config(materialized='table') }}

SELECT
    user_id,
    MIN(order_date)                    AS first_order_date,
    MAX(order_date)                    AS last_order_date,
    COUNT(DISTINCT order_id)           AS total_orders,
    SUM(revenue)                       AS total_revenue,
    AVG(revenue)                       AS avg_order_value,
    DATEDIFF('day',
        MIN(order_date),
        MAX(order_date))               AS customer_lifespan_days
FROM {{ ref('stg_orders') }}
GROUP BY user_id
```

---
**Priyanka Sinha** | [LinkedIn](https://linkedin.com/in/priyanka-sinha) | [Email](mailto:priyankasinhabhu@gmail.com)
