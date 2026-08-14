-- BigQuery SQL ELT: Bronze (Raw) -> Silver (Cleaned & Deduplicated)
-- Target: `dataeng-505315.ecom_silver.orders_cleaned`
-- Optimization: Partitioned by order_date (DAY) & Clustered by category, customer_id
-- Sandbox Friendly: Uses DDL CREATE OR REPLACE TABLE AS SELECT (100% Free Sandbox Supported)

CREATE OR REPLACE TABLE `dataeng-505315.ecom_silver.orders_cleaned`
PARTITION BY order_date
CLUSTER BY category, customer_id
AS
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
    ingestion_source
FROM `dataeng-505315.ecom_bronze.raw_orders`
WHERE order_id IS NOT NULL
  AND event_timestamp IS NOT NULL
-- BigQuery Window Function Deduplication by order_id
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_id 
    ORDER BY ingestion_timestamp DESC
) = 1;
