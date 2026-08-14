"""
Airflow DAG 01: Streaming Order Ingestion Health & Buffer Check
Orchestrates streaming event consumer monitoring and raw BigQuery Bronze ingestion health checks.
Cloud Composer 2 & Airflow 2.x Compatible.
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "dataeng-505315")

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    '01_order_streaming_health_dag',
    default_args=default_args,
    description='Monitors real-time Pub/Sub streaming ingestion into BigQuery Bronze dataset',
    schedule_interval='*/15 * * * *', # Every 15 minutes
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['gcp', 'streaming', 'pubsub', 'bigquery', 'portfolio'],
) as dag:

    # Task 1: Stream Health Check
    check_stream_health = BashOperator(
        task_id='check_streaming_buffer_health',
        bash_command='python /opt/airflow/streaming/pubsub_to_bq_consumer.py || python streaming/pubsub_to_bq_consumer.py',
    )

    # Task 2: Log Status
    log_streaming_status = BashOperator(
        task_id='log_streaming_status',
        bash_command='echo "Streaming Healthcheck Completed for GCP Project: ' + PROJECT_ID + '"',
    )

    check_stream_health >> log_streaming_status
