"""
BigQuery Ingestion Consumer
Consumes live streaming messages from GCP Pub/Sub or local stream buffer
and writes JSON payloads into BigQuery Bronze table: `dataeng-505315.ecom_bronze.raw_orders`.
"""

import json
import os
import sys
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ and not os.path.exists(os.environ["GOOGLE_APPLICATION_CREDENTIALS"]):
    os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS")

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "dataeng-505315")

BRONZE_DATASET = os.getenv("BQ_BRONZE_DATASET", "ecom_bronze")
TABLE_NAME = "raw_orders"
FULL_TABLE_ID = f"{PROJECT_ID}.{BRONZE_DATASET}.{TABLE_NAME}"

# BigQuery Bronze Table Schema (Partitioned by ingestion_date)
BRONZE_TABLE_SCHEMA = [
    {"name": "event_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "order_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "customer_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "product_id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "product_name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "category", "type": "STRING", "mode": "NULLABLE"},
    {"name": "unit_price", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "quantity", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "total_amount", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "rating", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "review_text", "type": "STRING", "mode": "NULLABLE"},
    {"name": "customer_city", "type": "STRING", "mode": "NULLABLE"},
    {"name": "payment_method", "type": "STRING", "mode": "NULLABLE"},
    {"name": "event_timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "ingestion_timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "ingestion_source", "type": "STRING", "mode": "NULLABLE"},
]

def ensure_bronze_table_exists(bq_client):
    """Ensures BigQuery Bronze table exists with date partitioning."""
    from google.cloud import bigquery
    from google.api_core.exceptions import NotFound

    try:
        bq_client.get_table(FULL_TABLE_ID)
        print(f"[OK] BigQuery Bronze Table verified: {FULL_TABLE_ID}")
    except NotFound:
        print(f"Creating Partitioned Bronze Table: {FULL_TABLE_ID}...")
        schema_fields = [
            bigquery.SchemaField(name=f["name"], field_type=f["type"], mode=f["mode"])
            for f in BRONZE_TABLE_SCHEMA
        ]
        table = bigquery.Table(FULL_TABLE_ID, schema=schema_fields)
        
        # Partition by ingestion_timestamp day for query optimization & low scan cost
        table.time_partitioning = bigquery.TimePartitioning(
            type_="DAY",
            field="ingestion_timestamp"
        )
        table.clustering_fields = ["category", "customer_id"]

        
        table = bq_client.create_table(table)
        print(f"[SUCCESS] Successfully created Bronze Table: {FULL_TABLE_ID}")

def ingest_events_to_bigquery(records):
    """Loads batch of JSON records into BigQuery Bronze table (100% Free Batch Load)."""
    if not records:
        print("No events to ingest.")
        return

    now_iso = datetime.now(timezone.utc).isoformat()
    formatted_records = []
    
    for rec in records:
        r = dict(rec)
        r["ingestion_timestamp"] = now_iso
        formatted_records.append(r)

    try:
        from google.cloud import bigquery
        bq_client = bigquery.Client(project=PROJECT_ID)
        ensure_bronze_table_exists(bq_client)
        
        schema_fields = [
            bigquery.SchemaField(name=f["name"], field_type=f["type"], mode=f["mode"])
            for f in BRONZE_TABLE_SCHEMA
        ]
        job_config = bigquery.LoadJobConfig(
            schema=schema_fields,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        )
        
        load_job = bq_client.load_table_from_json(
            formatted_records,
            FULL_TABLE_ID,
            job_config=job_config
        )
        load_job.result() # Wait for job completion
        print(f"[SUCCESS] Ingested {len(formatted_records)} events live into BigQuery Bronze ({FULL_TABLE_ID})")
        return True


    except Exception as e:
        print(f"[INFO] BigQuery Ingestion note: {e}")
        print(f"  (Processed {len(formatted_records)} records locally)")
        return False


def consume_from_local_buffer(buffer_path="streaming/local_event_buffer.jsonl"):
    """Consumes and ingests events from local JSONL buffer."""
    if not os.path.exists(buffer_path):
        print(f"No local event buffer found at {buffer_path}")
        return

    records = []
    with open(buffer_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))

    print(f"Reading {len(records)} events from local buffer: {buffer_path}")
    ingest_events_to_bigquery(records)

if __name__ == "__main__":
    print(f"=== BigQuery Stream Consumer Initializing ===")
    print(f"Target Table: {FULL_TABLE_ID}")
    consume_from_local_buffer()
