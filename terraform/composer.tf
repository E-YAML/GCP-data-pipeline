# Service Account for Cloud Composer 2
resource "google_service_account" "composer_sa" {
  account_id   = "composer-executor-sa"
  display_name = "Cloud Composer Service Account"
}

# IAM Permissions for Composer Service Account
resource "google_project_iam_member" "composer_worker" {
  project = var.project_id
  role    = "roles/composer.worker"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

resource "google_project_iam_member" "composer_bq_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

resource "google_project_iam_member" "composer_pubsub_admin" {
  project = var.project_id
  role    = "roles/pubsub.admin"
  member  = "serviceAccount:${google_service_account.composer_sa.email}"
}

# Cloud Composer 2 Environment (Reference definition for production showcase)
# Note: Keep enable_cloud_composer = false to avoid incurring compute costs!
resource "google_composer_environment" "ecom_composer_env" {
  count  = var.enable_cloud_composer ? 1 : 0
  name   = "ecom-analytics-composer"
  region = var.region

  config {
    software_config {
      image_version = "composer-2.8.5-airflow-2.7.3"

      env_variables = {
        GCP_PROJECT_ID    = var.project_id
        BQ_BRONZE_DATASET = var.bq_bronze_dataset
        BQ_SILVER_DATASET = var.bq_silver_dataset
        BQ_GOLD_DATASET   = var.bq_gold_dataset
      }
    }

    node_config {
      service_account = google_service_account.composer_sa.name
    }
  }
}
