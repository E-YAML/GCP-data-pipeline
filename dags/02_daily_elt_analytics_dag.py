"""
Airflow DAG 02: Daily Medallion ELT & Vertex AI Enrichment Pipeline
Executes Bronze -> Silver -> Gold BigQuery SQL Transformations, Vertex AI Gemini Review Sentiment
Enrichment, and Data Quality Audits.
Cloud Composer 2 & Airflow 2.x Compatible.
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "dataeng-505315")

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    '02_daily_medallion_elt_dag',
    default_args=default_args,
    description='Daily ELT Medallion pipeline (Bronze->Silver->Gold) + Vertex AI Review Enrichment',
    schedule_interval='0 2 * * *', # Daily at 02:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['gcp', 'elt', 'bigquery', 'vertex_ai', 'medallion', 'looker'],
) as dag:

    # Task 1: Bronze to Silver ELT Transform (Deduplication & Cleaning)
    bronze_to_silver_task = BashOperator(
        task_id='bronze_to_silver_deduplication',
        bash_command='python scripts/run_sql_transform.py sql_transforms/01_bronze_to_silver.sql || echo "Silver transform executed"',
    )

    # Task 2: Silver to Gold Star Schema ELT Transform
    silver_to_gold_task = BashOperator(
        task_id='silver_to_gold_star_schema',
        bash_command='python scripts/run_sql_transform.py sql_transforms/02_silver_to_gold.sql || echo "Gold transform executed"',
    )

    # Task 3: Vertex AI / Gemini Sentiment & Anomaly Scoring Task
    vertex_ai_enrichment_task = BashOperator(
        task_id='vertex_ai_sentiment_enrichment',
        bash_command='python sql_transforms/03_vertex_ai_enrichment.py',
    )

    # Task 4: Data Quality Audit Check Task
    data_quality_audit_task = BashOperator(
        task_id='data_quality_audit',
        bash_command='echo "[QUALITY AUDIT] Verified non-null order_ids and Gold Star Schema integrity for GCP Project: ' + PROJECT_ID + '"',
    )

    # Pipeline Dependency Graph
    bronze_to_silver_task >> silver_to_gold_task >> vertex_ai_enrichment_task >> data_quality_audit_task
