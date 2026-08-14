-- dbt Model: Gold Layer Orders Fact Table
-- Target Dataset: `ecom_gold`

{{ config(
    materialized='table',
    dataset='ecom_gold',
    partition_by={
      "field": "order_date",
      "data_type": "date",
      "granularity": "day"
    },
    cluster_by=["product_id", "customer_id"]
) }}

SELECT
    order_id,
    customer_id,
    product_id,
    category,
    order_date,
    unit_price,
    quantity,
    total_amount,
    rating,
    review_text,
    payment_method,
    customer_city
FROM {{ ref('orders_cleaned') }}
