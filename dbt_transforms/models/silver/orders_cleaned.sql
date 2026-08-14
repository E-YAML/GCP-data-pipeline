-- dbt Model: Silver Layer Cleaned & Deduplicated Orders
-- Materialized as Table in dataset `ecom_silver`

{{ config(
    materialized='table',
    partition_by={
      "field": "order_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=["category", "customer_id"]
) }}

WITH raw_source AS (
    SELECT *
    FROM {{ source('ecom_bronze', 'raw_orders') }}
    WHERE order_id IS NOT NULL
      AND event_timestamp IS NOT NULL
),

deduplicated AS (
    SELECT
        event_id,
        order_id,
        customer_id,
        product_id,
        UPPER(TRIM(product_name)) AS product_name,
        UPPER(TRIM(category)) AS category,
        CAST(unit_price AS NUMERIC) AS unit_price,
        CAST(quantity AS INT64) AS quantity,
        CAST(total_amount AS NUMERIC) AS total_amount,
        CAST(rating AS INT64) AS rating,
        TRIM(review_text) AS review_text,
        TRIM(customer_city) AS customer_city,
        UPPER(TRIM(payment_method)) AS payment_method,
        event_timestamp,
        DATE(event_timestamp) AS order_date,
        ingestion_timestamp,
        ingestion_source,
        ROW_NUMBER() OVER (
            PARTITION BY order_id 
            ORDER BY ingestion_timestamp DESC
        ) AS rn
    FROM raw_source
)

SELECT
    event_id,
    order_id,
    customer_id,
    product_id,
    product_name,
    category,
    unit_price,
    quantity,
    total_amount,
    rating,
    review_text,
    customer_city,
    payment_method,
    event_timestamp,
    order_date,
    ingestion_timestamp,
    ingestion_source
FROM deduplicated
WHERE rn = 1
