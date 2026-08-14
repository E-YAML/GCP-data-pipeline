output "pubsub_topic_id" {
  description = "GCP Pub/Sub Topic ID"
  value       = google_pubsub_topic.order_events_topic.id
}

output "pubsub_subscription_id" {
  description = "GCP Pub/Sub Subscription ID"
  value       = google_pubsub_subscription.order_events_sub.id
}

output "bronze_dataset_id" {
  description = "BigQuery Bronze Dataset ID"
  value       = google_bigquery_dataset.bronze_dataset.dataset_id
}

output "silver_dataset_id" {
  description = "BigQuery Silver Dataset ID"
  value       = google_bigquery_dataset.silver_dataset.dataset_id
}

output "gold_dataset_id" {
  description = "BigQuery Gold Dataset ID"
  value       = google_bigquery_dataset.gold_dataset.dataset_id
}
