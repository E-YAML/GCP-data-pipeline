-- dbt Model: Gold Layer Product Dimension Catalog
-- Target Dataset: `ecom_gold`

{{ config(
    materialized='table',
    dataset='ecom_gold'
) }}

SELECT 
    product_id,
    MAX(product_name) AS product_name,
    MAX(category) AS category,
    AVG(unit_price) AS avg_unit_price,
    MIN(event_timestamp) AS first_seen_timestamp,
    MAX(event_timestamp) AS last_updated_timestamp
FROM {{ ref('orders_cleaned') }}
GROUP BY product_id
