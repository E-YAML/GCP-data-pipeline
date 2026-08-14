-- dbt Model: Gold Layer Daily Sales Aggregate Table
-- Target Dataset: `ecom_gold`

{{ config(
    materialized='table',
    dataset='ecom_gold'
) }}

SELECT
    order_date,
    category,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(quantity) AS total_units_sold,
    SUM(total_amount) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_order_value,
    ROUND(AVG(rating), 2) AS avg_customer_rating
FROM {{ ref('fact_orders') }}
GROUP BY order_date, category
