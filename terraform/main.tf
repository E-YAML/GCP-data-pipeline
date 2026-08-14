terraform {
  required_version = ">= 1.5.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.20.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Pub/Sub Topic for E-Commerce Real-Time Order Stream
resource "google_pubsub_topic" "order_events_topic" {
  name = "ecom-order-events-topic"

  labels = {
    environment = var.environment
    managed_by  = "terraform"
  }
}

# Pub/Sub Subscription for BigQuery Ingestion Consumer
resource "google_pubsub_subscription" "order_events_sub" {
  name  = "ecom-order-events-sub"
  topic = google_pubsub_topic.order_events_topic.name

  ack_deadline_seconds = 20

  expiration_policy {
    ttl = "" # Never expire
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
}

# BigQuery Datasets (Bronze, Silver, Gold Medallion Layers)
resource "google_bigquery_dataset" "bronze_dataset" {
  dataset_id                 = var.bq_bronze_dataset
  friendly_name              = "E-Commerce Bronze Raw Layer"
  description                = "Raw ingested streaming and batch tables"
  location                   = var.region
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "silver_dataset" {
  dataset_id                 = var.bq_silver_dataset
  friendly_name              = "E-Commerce Silver Cleaned Layer"
  description                = "Cleaned, typed, and deduplicated tables"
  location                   = var.region
  delete_contents_on_destroy = true
}

resource "google_bigquery_dataset" "gold_dataset" {
  dataset_id                 = var.bq_gold_dataset
  friendly_name              = "E-Commerce Gold Analytics Layer"
  description                = "Star Schema Facts, Dimensions, and Aggregates"
  location                   = var.region
  delete_contents_on_destroy = true
}
