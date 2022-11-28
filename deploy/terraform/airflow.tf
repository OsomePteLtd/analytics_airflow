

terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "4.44.1"
      }
    }
  backend "gcs" {
   bucket  = "osome-tfstate-analytics"
   prefix  = "terraform/cloud-composer/state"
   }
}


provider "google" {
  project = "healthy-clock-304411"
  region  = "us-central1"
}

resource "google_project_service" "composer_api" {
  provider = google
  project = "healthy-clock-304411"
  service = "composer.googleapis.com"
  // Disabling Cloud Composer API might irreversibly break all other
  // environments in your project.
  disable_on_destroy = false
}

resource "google_service_account" "custom_service_account" {
  provider = google
  account_id   = "airflow"
  display_name = "Airflow"
  description = "Airflow user for Analytics purposes"
}

resource "google_project_iam_member" "custom_service_account" {
  provider = google
  project  = "healthy-clock-304411"
  member   = format("serviceAccount:%s", google_service_account.custom_service_account.email)
  // Role for Public IP environments
  
  for_each = toset([
    "roles/composer.worker",
    "roles/bigquery.dataViewer",
    "roles/bigquery.user",
    ])
  role = each.key
}

resource "google_service_account_iam_member" "custom_service_account" {
  provider = google
  service_account_id = google_service_account.custom_service_account.name

  role = "roles/composer.ServiceAgentV2Ext"
  member = "serviceAccount:service-685919755896@cloudcomposer-accounts.iam.gserviceaccount.com"
}


resource "google_composer_environment" "analytics_airflow" {
  provider = google
  name = "analytics-airflow"

  labels = {
    owner = "analytics"
    env = "production"
  }

  config {
    software_config {
      image_version = "composer-2.0.31-airflow-2.2.5"

      pypi_packages = {
          apache-airflow-providers-sftp = "==4.1.0"
          apache-airflow-providers-slack = "==6.0.0"
          apache-airflow-providers-docker = "==3.1.0"
      }

      airflow_config_overrides = {
        dask-catchup_by_default = false
        core-max_active_runs_per_dag = 1
      }

    }

    node_config {
      service_account = google_service_account.custom_service_account.email
    }

    environment_size = "ENVIRONMENT_SIZE_SMALL"

    workloads_config {
      scheduler {
        cpu = 0.5
        memory_gb = 2
        storage_gb = 1
        count = 1
      }
      web_server {
        cpu = 0.5
        memory_gb = 2
        storage_gb = 2
      }
      worker {
        cpu = 1
        memory_gb = 2
        storage_gb = 2
        min_count = 1
        max_count = 3
      }
    }

    maintenance_window {
      start_time = "2022-01-01T17:00:00Z"
      end_time = "2022-01-01T23:00:00Z"
      recurrence = "FREQ=WEEKLY;BYDAY=SA,SU"
    }



  }
}