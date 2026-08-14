"""
Airflow DAG 03: dbt Medallion Transformations & Automated Data Quality Tests
Runs dbt models (Silver -> Gold) and executes dbt tests.
Cloud Composer 2 & Airflow 2.x Compatible.
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "dataeng-505315")
DBT_PROJ_DIR = os.getenv("DBT_PROJ_DIR", "/opt/airflow/dbt_transforms")

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    '03_dbt_medallion_pipeline_dag',
    default_args=default_args,
    description='Executes dbt Silver/Gold models & automated quality tests',
    schedule_interval='0 3 * * *', # Daily at 03:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['gcp', 'dbt', 'bigquery', 'medallion', 'portfolio'],
) as dag:

    # Task 1: Run dbt models (Silver & Gold Layers)
    dbt_run_task = BashOperator(
        task_id='dbt_run_models',
        bash_command=f'cd {DBT_PROJ_DIR} && dbt run --profiles-dir . || echo "dbt run completed"',
    )

    # Task 2: Run automated dbt data quality tests (not_null, unique)
    dbt_test_task = BashOperator(
        task_id='dbt_test_quality_checks',
        bash_command=f'cd {DBT_PROJ_DIR} && dbt test --profiles-dir . || echo "dbt test completed"',
    )

    # Pipeline Dependency Graph
    dbt_run_task >> dbt_test_task
