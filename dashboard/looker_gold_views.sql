-- Looker Studio Analytics Gold Views
-- Target: `dataeng-505315.ecom_gold`
-- Connect these views directly to Looker Studio (100% Free BI Tool)

--------------------------------------------------------------------------------
-- 1. Executive Sales KPI View
--------------------------------------------------------------------------------
CREATE OR REPLACE VIEW `dataeng-505315.ecom_gold.vw_executive_sales_kpis` AS
SELECT
    order_date,
    category,
    total_orders,
    unique_customers,
    total_units_sold,
    total_revenue,
    avg_order_value,
    avg_customer_rating,
    ROUND(total_revenue / NULLIF(unique_customers, 0), 2) AS revenue_per_customer
FROM `dataeng-505315.ecom_gold.agg_daily_sales`;

--------------------------------------------------------------------------------
-- 2. Vertex AI Sentiment & Risk Analysis View for Looker Studio
--------------------------------------------------------------------------------
CREATE OR REPLACE VIEW `dataeng-505315.ecom_gold.vw_customer_sentiment_breakdown` AS
SELECT
    f.order_date,
    f.category,
    f.payment_method,
    ai.ai_sentiment,
    ai.anomaly_flag,
    COUNT(f.order_id) AS total_orders,
    SUM(f.total_amount) AS total_revenue,
    AVG(f.rating) AS avg_rating,
    AVG(ai.ai_confidence_score) AS avg_ai_confidence
FROM `dataeng-505315.ecom_gold.fact_orders` f
LEFT JOIN `dataeng-505315.ecom_gold.ai_enriched_reviews` ai
  ON f.order_id = ai.order_id
GROUP BY f.order_date, f.category, f.payment_method, ai.ai_sentiment, ai.anomaly_flag;
