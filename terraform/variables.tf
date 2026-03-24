variable "credentials" {
  description = "My Credential"
  default     = "../keys/mnl-accident-pipeline-key.json"
}

variable "project" {
  description = "project name"
  default     = "mnl-accident-pipeline"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "mnl_accident_pipeline_dataset"
}

variable "gcs_bucket_name" {
  description = "My storage bucket name"
  default     = "mnl_accident_pipeline_bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}