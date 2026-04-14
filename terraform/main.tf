terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.0.0"
    }
  }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}

resource "google_storage_bucket" "mnl_accident_data_bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}
resource "google_storage_bucket" "temporary_bucket" {
  name          = var.gcs_temp_bucket_name
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "mnl_accident_dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}

resource "google_bigquery_table" "locations_staging_table" {
  dataset_id = google_bigquery_dataset.mnl_accident_dataset.dataset_id
  table_id   = "staging_locations"
  project = var.project

  deletion_protection = false

  schema     = jsonencode([
    {
      name = "city"
      type = "STRING",
      mode = "NULLABLE"
    },
    {
      name = "location"
      type = "STRING",
      mode = "NULLABLE"
    },
    {
      name = "latitude"
      type = "FLOAT",
      mode = "NULLABLE"
    },
    {
      name = "longitude"
      type = "FLOAT",
      mode = "NULLABLE"
    },
    {
      name = "accuracy"
      type = "FLOAT",
      mode = "NULLABLE"
    }
  ])
}

resource "google_bigquery_table" "locations_table" {
  dataset_id = google_bigquery_dataset.mnl_accident_dataset.dataset_id
  table_id   = "dim_locations"
  project = var.project

  deletion_protection = false

  table_constraints {
    primary_key {
      columns = ["location_id"]
    }
  }

  schema     = jsonencode([
    {
      name = "location_id"
      type = "STRING",
      mode = "NULLABLE"
    },
    {
      name = "city"
      type = "STRING",
      mode = "NULLABLE"
    },
    {
      name = "location"
      type = "STRING",
      mode = "NULLABLE"
    },
    {
      name = "latitude"
      type = "FLOAT",
      mode = "NULLABLE"
    },
    {
      name = "longitude"
      type = "FLOAT",
      mode = "NULLABLE"
    },
    {
      name = "accuracy"
      type = "FLOAT",
      mode = "NULLABLE"
    }
  ])
}
resource "google_bigquery_table" "enriched_table" {
  dataset_id = google_bigquery_dataset.mnl_accident_dataset.dataset_id
  table_id   = "staging_enriched"
  project = var.project

  deletion_protection = false

  schema     = jsonencode([
    {
      name = "date"
      type = "DATE",
      mode = "NULLABLE"
    },
    {
      name = "year"
      type = "INTEGER",
      mode = "NULLABLE"
    },
    {
      name = "month"
      type = "INTEGER",
      mode = "NULLABLE"
    },
    {
      name = "day"
      type = "INTEGER",
      mode = "NULLABLE"
    },
    {
      name = "week"
      type = "INTEGER",
      mode = "NULLABLE"
    },
    {
      name = "weekday"
      type = "INTEGER",
      mode = "NULLABLE"
    },
    {
      name = "time"
      type = "STRING",
      mode = "NULLABLE"
    },
    {
      name = "hour"
      type = "INTEGER",
      mode = "NULLABLE"
    },
    {
      name = "city"
      type = "STRING",
      mode = "NULLABLE"
    },
    {
      name = "location"
      type = "STRING",
      mode = "NULLABLE"
    },
    {
      name = "latitude"
      type = "FLOAT",
      mode = "NULLABLE"
    },
    {
      name = "longitude"
      type = "FLOAT",
      mode = "NULLABLE"
    },
    {
      name = "accuracy"
      type = "FLOAT",
      mode = "NULLABLE"
    },
    {
      name = "direction"
      type = "STRING",
      mode = "NULLABLE"
    },
    {
      name = "type"
      type = "STRING",
      mode = "NULLABLE"
    },
    {
      name = "lanes_blocked"
      type = "INTEGER",
      mode = "NULLABLE"
    },
    {
      name = "involved"
      type = "STRING",
      mode = "NULLABLE"
    },
    {
      name = "post"
      type = "STRING",
      mode = "NULLABLE"
    },
    {
      name = "link"
      type = "STRING",
      mode = "NULLABLE"
    }
  ])
}

resource "google_bigquery_table" "dates_table" {
  dataset_id = google_bigquery_dataset.mnl_accident_dataset.dataset_id
  table_id   = "dim_dates"
  project = var.project

  deletion_protection = false
  table_constraints {
    primary_key { 
      columns = ["date"] 
    }
  }

  schema     = jsonencode([
    {
      name = "date"
      type = "DATE",
      mode = "NULLABLE"
    },
    {
      name = "year"
      type = "INTEGER",
      mode = "NULLABLE"
    },
    {
      name = "month"
      type = "INTEGER",
      mode = "NULLABLE"
    },
    {
      name = "day"
      type = "INTEGER",
      mode = "NULLABLE"
    },
    {
      name = "week"
      type = "INTEGER",
      mode = "NULLABLE"
    },
    {
      name = "weekday"
      type = "INTEGER",
      mode = "NULLABLE"
    }
  ])
}

resource "google_bigquery_table" "direction_table" {
  dataset_id = google_bigquery_dataset.mnl_accident_dataset.dataset_id
  table_id   = "dim_direction"
  project = var.project
  
  deletion_protection = false
  table_constraints {
    primary_key { 
      columns = ["id"] 
    }
  }

  schema     = jsonencode([
    {
      name = "id"
      type = "STRING",
      mode = "NULLABLE"
    },
    {
      name = "full_name"
      type = "STRING",
      mode = "NULLABLE"
    },
    {
      name = "short_name"
      type = "STRING",
      mode = "NULLABLE"
    }
  ])
}

resource "google_bigquery_table" "event_type_table" {
  dataset_id = google_bigquery_dataset.mnl_accident_dataset.dataset_id
  table_id   = "dim_event_type"
  project = var.project
  
  deletion_protection = false
  table_constraints {
    primary_key { 
      columns = ["id"] 
    }
  }

  schema     = jsonencode([
    {
      name = "id"
      type = "STRING",
      mode = "NULLABLE"
    },
    {
      name = "type"
      type = "STRING",
      mode = "NULLABLE"
    }
  ])
}

resource "google_bigquery_table" "fact_events_table" {
  dataset_id = google_bigquery_dataset.mnl_accident_dataset.dataset_id
  table_id   = "fact_events"
  project = var.project

  deletion_protection = false

  table_constraints {
    primary_key {
      columns = ["id"]
    }

    foreign_keys {
      name = "fk_date_id"

      referenced_table {
        project_id = google_bigquery_dataset.mnl_accident_dataset.project
        dataset_id = google_bigquery_dataset.mnl_accident_dataset.dataset_id
        table_id   = google_bigquery_table.dates_table.table_id
      }

      column_references {
        referencing_column = "date_id"
        referenced_column = "date"
      }
    }
    foreign_keys {
      name = "fk_location_id"

      referenced_table {
        project_id = google_bigquery_dataset.mnl_accident_dataset.project
        dataset_id = google_bigquery_dataset.mnl_accident_dataset.dataset_id
        table_id   = google_bigquery_table.locations_table.table_id
      }

      column_references {
        referencing_column = "location_id"
        referenced_column = "location_id"
      }
    }
    foreign_keys {
      name = "fk_event_type_id"

      referenced_table {
        project_id = google_bigquery_dataset.mnl_accident_dataset.project
        dataset_id = google_bigquery_dataset.mnl_accident_dataset.dataset_id
        table_id   = google_bigquery_table.event_type_table.table_id
      }

      column_references {
        referencing_column = "event_type_id"
        referenced_column = "id"
      }
    }
    foreign_keys {
      name = "fk_direction_id"

      referenced_table {
        project_id = google_bigquery_dataset.mnl_accident_dataset.project
        dataset_id = google_bigquery_dataset.mnl_accident_dataset.dataset_id
        table_id   = google_bigquery_table.direction_table.table_id
      }

      column_references {
        referencing_column = "direction_id"
        referenced_column = "id"
      }
    }
  }

  schema     = jsonencode([
    {
      name = "id"
      type = "STRING",
      mode = "NULLABLE"
    },
    {
      name = "date_id"
      type = "DATE",
      mode = "NULLABLE"
    },
    {
      name = "location_id"
      type = "STRING",
      mode = "NULLABLE"
    },
    {
      name = "event_type_id"
      type = "STRING",
      mode = "NULLABLE"
    },
    {
      name = "direction_id"
      type = "STRING",
      mode = "NULLABLE"
    },
    {
      name = "lanes_blocked"
      type = "INTEGER",
      mode = "NULLABLE"
    },
    {
      name = "post"
      type = "STRING",
      mode = "NULLABLE"
    },
    {
      name = "link"
      type = "STRING",
      mode = "NULLABLE"
    }
  ])
}