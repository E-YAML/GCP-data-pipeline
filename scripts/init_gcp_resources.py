"""
GCP BigQuery & Pub/Sub Resource Initializer
Creates the BigQuery Medallion Datasets (ecom_bronze, ecom_silver, ecom_gold)
and Pub/Sub Topics/Subscriptions in GCP Project: dataeng-505315.
"""

import os
import sys
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# If specified credentials file doesn't exist, remove var so ADC (gcloud auth) is used automatically
if "GOOGLE_APPLICATION_CREDENTIALS" in os.environ and not os.path.exists(os.environ["GOOGLE_APPLICATION_CREDENTIALS"]):
    os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS")

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "dataeng-505315")

REGION = os.getenv("GCP_REGION", "us-central1")
BRONZE_DATASET = os.getenv("BQ_BRONZE_DATASET", "ecom_bronze")
SILVER_DATASET = os.getenv("BQ_SILVER_DATASET", "ecom_silver")
GOLD_DATASET = os.getenv("BQ_GOLD_DATASET", "ecom_gold")

TOPIC_ID = "ecom-order-events-topic"
SUB_ID = "ecom-order-events-sub"

def create_bigquery_datasets():
    """Initializes BigQuery Bronze, Silver, and Gold datasets."""
    try:
        from google.cloud import bigquery
        from google.api_core.exceptions import Conflict
        
        client = bigquery.Client(project=PROJECT_ID)
        datasets = [
            (BRONZE_DATASET, "Raw Ingestion Layer (Partitioned JSON payloads)"),
            (SILVER_DATASET, "Cleaned & Deduplicated Layer"),
            (GOLD_DATASET, "Analytics Star Schema Facts & Dimensions")
        ]
        
        print(f"--- Initializing BigQuery Datasets in Project '{PROJECT_ID}' ---")
        for ds_id, description in datasets:
            full_dataset_id = f"{PROJECT_ID}.{ds_id}"
            dataset = bigquery.Dataset(full_dataset_id)
            dataset.location = REGION
            dataset.description = description
            
            try:
                dataset = client.create_dataset(dataset, timeout=30)
                print(f"[SUCCESS] Created BigQuery Dataset: {full_dataset_id}")
            except Conflict:
                print(f"[INFO] Dataset already exists: {full_dataset_id}")
            except Exception as e:
                print(f"[WARN] Could not create {ds_id}: {e}")
                
    except ImportError:
        print("[WARN] google-cloud-bigquery package not installed. Skipping direct BQ setup.")
    except Exception as e:
        print(f"[INFO] BigQuery Connection Note: {e}")
        print("  (If authenticating for the first time, ensure service account credentials or ADC are logged in)")

def create_pubsub_resources():
    """Initializes GCP Pub/Sub Topic and Subscription."""
    try:
        from google.cloud import pubsub_v1
        from google.api_core.exceptions import AlreadyExists
        
        publisher = pubsub_v1.PublisherClient()
        subscriber = pubsub_v1.SubscriberClient()
        
        topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
        sub_path = subscriber.subscription_path(PROJECT_ID, SUB_ID)
        
        print(f"\n--- Initializing Pub/Sub Resources in Project '{PROJECT_ID}' ---")
        try:
            publisher.create_topic(request={"name": topic_path})
            print(f"[SUCCESS] Created Pub/Sub Topic: {topic_path}")
        except AlreadyExists:
            print(f"[INFO] Pub/Sub Topic already exists: {topic_path}")
        except Exception as e:
            print(f"[WARN] Could not create topic: {e}")

        try:
            subscriber.create_subscription(request={"name": sub_path, "topic": topic_path})
            print(f"[SUCCESS] Created Pub/Sub Subscription: {sub_path}")
        except AlreadyExists:
            print(f"[INFO] Pub/Sub Subscription already exists: {sub_path}")
        except Exception as e:
            print(f"[WARN] Could not create subscription: {e}")

    except ImportError:
        print("[WARN] google-cloud-pubsub package not installed. Skipping direct Pub/Sub setup.")
    except Exception as e:
        print(f"[INFO] Pub/Sub Connection Note: {e}")


if __name__ == "__main__":
    print(f"=== GCP Data Engineering Resource Setup ===")
    print(f"Project ID: {PROJECT_ID}")
    print(f"Region:     {REGION}\n")
    
    create_bigquery_datasets()
    create_pubsub_resources()
    print("\n=== Setup Script Complete ===")
