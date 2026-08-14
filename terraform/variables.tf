variable "project_id" {
  description = "GCP Project ID"
  type        = string
  default     = "dataeng-505315"
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
}

variable "bq_bronze_dataset" {
  description = "BigQuery Bronze raw dataset name"
  type        = string
  default     = "ecom_bronze"
}

variable "bq_silver_dataset" {
  description = "BigQuery Silver cleaned dataset name"
  type        = string
  default     = "ecom_silver"
}

variable "bq_gold_dataset" {
  description = "BigQuery Gold analytics dataset name"
  type        = string
  default     = "ecom_gold"
}

variable "enable_cloud_composer" {
  description = "Set to true only when deploying Cloud Composer compute cluster to GCP"
  type        = bool
  default     = false
}
