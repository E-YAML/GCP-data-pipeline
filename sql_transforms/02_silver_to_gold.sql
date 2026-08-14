-- BigQuery SQL ELT: Silver (Cleaned) -> Gold (Analytics Star Schema & Aggregates)
-- Targets: 
-- 1. `dataeng-505315.ecom_gold.dim_products`
-- 2. `dataeng-505315.ecom_gold.fact_orders`
-- 3. `dataeng-505315.ecom_gold.agg_daily_sales`
-- Sandbox Friendly: Uses DDL CREATE OR REPLACE TABLE AS SELECT (100% Free Sandbox Supported)

--------------------------------------------------------------------------------
-- 1. Dimension Table: Products Catalog
--------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `dataeng-505315.ecom_gold.dim_products`
AS
SELECT 
    product_id,
    MAX(product_name) AS product_name,
    MAX(category) AS category,
    AVG(unit_price) AS avg_unit_price,
    MIN(event_timestamp) AS first_seen_timestamp,
    MAX(event_timestamp) AS last_updated_timestamp
FROM `dataeng-505315.ecom_silver.orders_cleaned`
GROUP BY product_id;

--------------------------------------------------------------------------------
-- 2. Fact Table: Orders
--------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `dataeng-505315.ecom_gold.fact_orders`
PARTITION BY order_date
CLUSTER BY product_id, customer_id
AS
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
FROM `dataeng-505315.ecom_silver.orders_cleaned`;

--------------------------------------------------------------------------------
-- 3. Analytical Aggregates Table: Daily Sales & KPI Metrics (Optimized for Looker)
--------------------------------------------------------------------------------
CREATE OR REPLACE TABLE `dataeng-505315.ecom_gold.agg_daily_sales`
AS
SELECT
    order_date,
    category,
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(quantity) AS total_units_sold,
    SUM(total_amount) AS total_revenue,
    ROUND(AVG(total_amount), 2) AS avg_order_value,
    ROUND(AVG(rating), 2) AS avg_customer_rating
FROM `dataeng-505315.ecom_gold.fact_orders`
GROUP BY order_date, category;
