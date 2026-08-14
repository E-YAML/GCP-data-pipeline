# GCP Modern Real-Time & Batch Data Platform (ETL/ELT)
### *Airflow / Cloud Composer • dbt (data build tool) • GCP Pub/Sub • BigQuery Medallion • Vertex AI Gemini • Looker Studio • Antigravity GCP MCP*

[![GCP](https://img.shields.io/badge/Google_Cloud-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)](https://cloud.google.com/)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![dbt](https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![BigQuery](https://img.shields.io/badge/BigQuery-669DF6?style=for-the-badge&logo=googlecloud&logoColor=white)](https://cloud.google.com/bigquery)
[![Vertex AI](https://img.shields.io/badge/Vertex%20AI-246FDB?style=for-the-badge&logo=google&logoColor=white)](https://cloud.google.com/vertex-ai)
[![Looker](https://img.shields.io/badge/Looker%20Studio-4285F4?style=for-the-badge&logo=looker&logoColor=white)](https://lookerstudio.google.com/)
[![Terraform](https://img.shields.io/badge/Terraform-7B42BC?style=for-the-badge&logo=terraform&logoColor=white)](https://www.terraform.io/)

An enterprise-grade, portfolio-ready Data Engineering platform demonstrating real-time order streaming, batch ELT medallion data warehousing powered by **dbt**, AI sentiment & fraud risk enrichment via Vertex AI Gemini, and automated pipeline orchestration. 

Built with a **100% Zero-Cost Guarantee ($0.00 GCP spend)** using local Docker Compose Airflow (Cloud Composer compatible) and GCP Free Tier limits.

---

## 🏗 Architecture Diagram

```mermaid
flowchart TD
    subgraph Ingestion ["1. Real-Time & Streaming Ingestion"]
        A[Python Event Streamer] -->|Stream JSON Events| B[GCP Pub/Sub Topic]
        B -->|Subscribe / Micro-batch| C[Python Stream Consumer]
    end

    subgraph Warehouse ["2. dbt & BigQuery Medallion Data Warehouse"]
        C -->|Raw JSON Loads| D[([Bronze Layer: ecom_bronze.raw_orders])]
        
        subgraph DBT ["dbt Transformation Models & Automated Quality Tests"]
            D -->|dbt run models/silver/orders_cleaned.sql| E[([Silver Layer: ecom_silver.orders_cleaned])]
            E -->|dbt run models/gold/fact_orders.sql| F[([Gold Layer: fact_orders & dim_products])]
            F -->|dbt run models/gold/agg_daily_sales.sql| G[([Gold Layer: agg_daily_sales])]
            E -->|dbt test schema.yml| H1[Automated Quality Checks: unique, not_null]
        end
    end

    subgraph AI ["3. AI & ML Enrichment (Vertex AI / Gemini)"]
        E -->|Review Payload| H[Vertex AI / Gemini API]
        H -->|Sentiment & Fraud Scores| I[([Gold Layer: ai_enriched_reviews])]
    end

    subgraph Orchestration ["4. Pipeline Orchestration"]
        J[Apache Airflow / Cloud Composer 2] -->|Schedule & Healthcheck| C
        J -->|DAG 03: dbt run & dbt test| DBT
        J -->|Trigger AI Enrichment| H
    end

    subgraph BI ["5. Analytics & Visualization"]
        F --> K[Looker Studio Dashboards]
        G --> K
        I --> K
    end

    classDef gcp fill:#e8f0fe,stroke:#1a73e8,stroke-width:2px,color:#174ea6;
    class A,B,C,D,E,F,G,H,I,J,K gcp;
```

---

## 🌟 Key Features

1. **dbt Data Transformation Engine**:
   - Production **dbt models** organized into Silver (`orders_cleaned.sql`) and Gold (`dim_products.sql`, `fact_orders.sql`, `agg_daily_sales.sql`).
   - Automated quality testing defined in `schema.yml` enforcing `unique` and `not_null` constraints.

2. **Hybrid Streaming & Batch Ingestion**:
   - Streams e-commerce order transactions and product reviews to **GCP Pub/Sub**.
   - Ingests payloads into **BigQuery Bronze Layer** with date partitioning (`DAY`) and clustering (`category`, `customer_id`).

3. **Medallion Data Architecture (Bronze → Silver → Gold)**:
   - **Bronze**: Partitioned raw JSON ingestion layer.
   - **Silver**: Deduplicated and schema-enforced records using dbt CTE window functions.
   - **Gold**: Analytical Star Schema (`fact_orders`, `dim_products`) and pre-aggregated daily sales tables (`agg_daily_sales`).

4. **Vertex AI / Gemini AI Enrichment**:
   - Automated sentiment analysis (`POSITIVE`, `NEGATIVE`, `NEUTRAL`) and transaction anomaly scoring (`SUSPICIOUS`, `NORMAL`) on customer product reviews.

5. **Production Cloud Composer / Airflow Orchestration**:
   - Airflow DAGs (`01_order_streaming_health_dag`, `02_daily_medallion_elt_dag`, `03_dbt_medallion_pipeline_dag`) orchestrating streaming health, native SQL, and dbt models.

---

## 📁 Repository Structure

```
d:\EData\GCP-dataeng\
├── README.md                      # Comprehensive Architecture & Project Guide
├── docker-compose.yml             # Local Airflow environment (Zero-Cost)
├── requirements.txt               # Python GCP SDKs & dbt-bigquery dependencies
├── dbt_transforms/                # dbt Data Transformation Framework
│   ├── dbt_project.yml            # dbt project configuration
│   ├── profiles.yml               # dbt BigQuery target profile connection
│   └── models/
│       ├── bronze/
│       │   └── sources.yml        # Raw Bronze source definitions
│       ├── silver/
│       │   └── orders_cleaned.sql # Cleaned & deduplicated Silver model
│       ├── gold/
│       │   ├── dim_products.sql   # Product catalog dimension model
│       │   ├── fact_orders.sql    # Orders fact table model
│       │   └── agg_daily_sales.sql# Daily KPI aggregate model
│       └── schema.yml             # Automated data quality tests (unique, not_null)
├── streaming/                     # Real-Time Event Pipeline
│   ├── order_producer.py
│   └── pubsub_to_bq_consumer.py
├── sql_transforms/                # BigQuery ELT SQL Transforms & Vertex AI
│   └── 03_vertex_ai_enrichment.py # Gemini AI sentiment & anomaly scoring
├── dags/                          # Airflow / Cloud Composer DAGs
│   ├── 01_order_streaming_pipeline_dag.py
│   ├── 02_daily_elt_analytics_dag.py
│   └── 03_dbt_medallion_pipeline_dag.py # Orchestrates dbt run & dbt test
└── dashboard/                     # Looker Studio Gold Views
    └── looker_gold_views.sql
```

---

## 🚀 How to Run dbt Transformations

### Run dbt Models:
```bash
cd dbt_transforms
dbt run --profiles-dir .
```

### Run Automated dbt Quality Tests:
```bash
dbt test --profiles-dir .
```
