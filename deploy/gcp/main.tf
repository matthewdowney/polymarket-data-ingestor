terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 4.0"
    }
  }
}

provider "google" {
  project     = var.project_id
  region      = var.region
  credentials = file("${path.module}/terraform-deployer-key.json")
}

variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "northamerica-northeast1"
}

variable "zone" {
  description = "GCP zone"
  type        = string
  default     = "northamerica-northeast1-a"
}

variable "ssh_user" {
  description = "SSH username"
  type        = string
  default     = "polymarket"
}

# Generate random suffix for unique names
resource "random_string" "suffix" {
  length  = 8
  special = false
  upper   = false
}

# Cloud Storage bucket for data
resource "google_storage_bucket" "data" {
  name          = "${var.project_id}-polymarket-data-${random_string.suffix.result}"
  location      = "US"
  force_destroy = false

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type          = "SetStorageClass"
      storage_class = "ARCHIVE"
    }
  }
}

# Service account for the VM
data "google_service_account" "existing_logger" {
  count      = 1
  account_id = "polymarket-logger"
}

resource "google_service_account" "logger" {
  count        = length(data.google_service_account.existing_logger) == 0 ? 1 : 0
  account_id   = "polymarket-logger"
  display_name = "Polymarket Logger Service Account"
}

locals {
  logger_service_account = try(data.google_service_account.existing_logger[0].email, google_service_account.logger[0].email)
}

# IAM binding for storage access
resource "google_storage_bucket_iam_member" "logger" {
  bucket = google_storage_bucket.data.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${local.logger_service_account}"
}

# IAM binding for monitoring
resource "google_project_iam_member" "monitoring" {
  project = var.project_id
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${local.logger_service_account}"
}

resource "google_project_iam_member" "logging" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${local.logger_service_account}"
}

# Firewall rule for SSH
resource "google_compute_firewall" "ssh" {
  name    = "allow-ssh-polymarket"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = ["0.0.0.0/0"]  # Restrict to your IP for security
  target_tags   = ["polymarket-logger"]
}

# Startup script
locals {
  startup_script = templatefile("${path.module}/startup-script.sh", {
    bucket_name = google_storage_bucket.data.name
    project_id  = var.project_id
    region      = var.region
  })
}

# Compute instance
resource "google_compute_instance" "logger" {
  name         = "polymarket-logger"
  machine_type = "e2-small"  # 2 vCPU, 2GB memory
  zone         = var.zone

  tags = ["polymarket-logger"]

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2204-lts"
      size  = 100
      type  = "pd-ssd"
    }
  }

  network_interface {
    network = "default"
    access_config {
      # Ephemeral external IP
    }
  }

  service_account {
    email  = local.logger_service_account
    scopes = ["cloud-platform"]
  }

  metadata = {
    startup-script = local.startup_script
    ssh-keys       = "${var.ssh_user}:${file("~/.ssh/id_ed25519.pub")}"
  }

  # Allow stopping for maintenance
  allow_stopping_for_update = true
}

# Cloud Monitoring alerts - commented out until metrics are created
/*
resource "google_monitoring_alert_policy" "service_down" {
  display_name = "Polymarket Logger Service Down"
  combiner     = "OR"

  conditions {
    display_name = "Service not running"
    
    condition_threshold {
      filter          = "resource.type=\"gce_instance\" AND metric.type=\"custom.googleapis.com/polymarket/service_running\""
      duration        = "300s"
      comparison      = "COMPARISON_LT"
      threshold_value = 0.5

      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_MEAN"
      }
    }
  }

  notification_channels = []  # Add notification channels as needed
}

resource "google_monitoring_alert_policy" "no_data" {
  display_name = "Polymarket Logger No Data"
  combiner     = "OR"

  conditions {
    display_name = "No messages received"
    
    condition_threshold {
      filter          = "resource.type=\"gce_instance\" AND metric.type=\"custom.googleapis.com/polymarket/seconds_since_last_message\""
      duration        = "600s"
      comparison      = "COMPARISON_GT"
      threshold_value = 300

      aggregations {
        alignment_period   = "300s"
        per_series_aligner = "ALIGN_MEAN"
      }
    }
  }

  notification_channels = []
}
*/

# Outputs
output "instance_name" {
  value = google_compute_instance.logger.name
}

output "external_ip" {
  value = google_compute_instance.logger.network_interface[0].access_config[0].nat_ip
}

output "bucket_name" {
  value = google_storage_bucket.data.name
}

output "ssh_command" {
  value = "gcloud compute ssh ${google_compute_instance.logger.name} --zone=${var.zone}"
}

output "zone" {
  value = var.zone
}