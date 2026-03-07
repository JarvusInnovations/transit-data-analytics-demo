locals {
  project  = "transit-data-analytics-demo"
  region   = "us-east4"
  location = "US-EAST4"
}

provider "google" {
  project = local.project
  region  = local.region
}


# -----------------------------------------------------------------------------
# Enabled APIs
# -----------------------------------------------------------------------------

resource "google_project_service" "apis" {
  for_each = toset([
    "bigquery.googleapis.com",
    "bigquerystorage.googleapis.com",
    "compute.googleapis.com",
    "container.googleapis.com",
    "containerregistry.googleapis.com",
    "artifactregistry.googleapis.com",
    "iam.googleapis.com",
    "iamcredentials.googleapis.com",
    "logging.googleapis.com",
    "monitoring.googleapis.com",
    "secretmanager.googleapis.com",
    "storage.googleapis.com",
    "pubsub.googleapis.com",
    "run.googleapis.com",
  ])

  service            = each.value
  disable_on_destroy = false
}


# -----------------------------------------------------------------------------
# GCS Buckets
# -----------------------------------------------------------------------------

resource "google_storage_bucket" "buckets" {
  for_each = toset([
    "jarvus-transit-data-demo-raw",
    "jarvus-transit-data-demo-parsed",
    "test-jarvus-transit-data-demo-raw",
    "test-jarvus-transit-data-demo-parsed",
    "dev-jarvus-transit-data-demo-raw",
    "dev-jarvus-transit-data-demo-parsed",
    "transit-data-demo-metadata",
    "metabase-restic",
  ])

  name     = each.value
  location = local.location

  force_destroy = false

  dynamic "retention_policy" {
    for_each = each.value == "jarvus-transit-data-demo-raw" ? [1] : []
    content {
      retention_period = 31557600 # 1 year
    }
  }
}


# -----------------------------------------------------------------------------
# Service Accounts
# -----------------------------------------------------------------------------

resource "google_service_account" "fetcher" {
  account_id   = "fetcher"
  display_name = "fetcher"
}

resource "google_service_account" "test_fetcher" {
  account_id   = "test-fetcher"
  display_name = "test-fetcher"
}

resource "google_service_account" "dev_archiver" {
  account_id   = "dev-archiver"
  display_name = "dev-archiver"
}

resource "google_service_account" "dagster" {
  account_id = "dagster"
}

resource "google_service_account" "github_action" {
  account_id = "github-action"
}

resource "google_service_account" "metabase" {
  account_id   = "metabase"
  display_name = "metabase"
}

resource "google_service_account" "metabase_restic" {
  account_id   = "metabase-restic"
  display_name = "metabase-restic"
}

resource "google_service_account" "transitdash" {
  account_id   = "transitdash"
  display_name = "transitdash"
}


# -----------------------------------------------------------------------------
# Project-level IAM bindings
# -----------------------------------------------------------------------------

# dagster - storage admin (project-level)
resource "google_project_iam_member" "dagster_storage_admin" {
  project = local.project
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.dagster.email}"
}

# github-action - BigQuery
resource "google_project_iam_member" "github_action_bq_job_user" {
  project = local.project
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.github_action.email}"
}

resource "google_project_iam_member" "github_action_bq_user" {
  project = local.project
  role    = "roles/bigquery.user"
  member  = "serviceAccount:${google_service_account.github_action.email}"
}

# metabase - BigQuery + Storage
resource "google_project_iam_member" "metabase_bq_data_viewer" {
  project = local.project
  role    = "roles/bigquery.dataViewer"
  member  = "serviceAccount:${google_service_account.metabase.email}"
}

resource "google_project_iam_member" "metabase_bq_job_user" {
  project = local.project
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.metabase.email}"
}

resource "google_project_iam_member" "metabase_bq_metadata_viewer" {
  project = local.project
  role    = "roles/bigquery.metadataViewer"
  member  = "serviceAccount:${google_service_account.metabase.email}"
}

resource "google_project_iam_member" "metabase_bq_user" {
  project = local.project
  role    = "roles/bigquery.user"
  member  = "serviceAccount:${google_service_account.metabase.email}"
}

resource "google_project_iam_member" "metabase_storage_viewer" {
  project = local.project
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${google_service_account.metabase.email}"
}

# transitdash - BigQuery + Storage
resource "google_project_iam_member" "transitdash_bq_data_viewer" {
  project = local.project
  role    = "roles/bigquery.dataViewer"
  member  = "serviceAccount:${google_service_account.transitdash.email}"
}

resource "google_project_iam_member" "transitdash_bq_job_user" {
  project = local.project
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.transitdash.email}"
}

resource "google_project_iam_member" "transitdash_storage_viewer" {
  project = local.project
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${google_service_account.transitdash.email}"
}


# -----------------------------------------------------------------------------
# Bucket-level IAM bindings
# -----------------------------------------------------------------------------

# fetcher -> raw bucket (objectCreator)
resource "google_storage_bucket_iam_member" "fetcher_raw_creator" {
  bucket = google_storage_bucket.buckets["jarvus-transit-data-demo-raw"].name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.fetcher.email}"
}

# test-fetcher -> test raw bucket (objectCreator)
resource "google_storage_bucket_iam_member" "test_fetcher_raw_creator" {
  bucket = google_storage_bucket.buckets["test-jarvus-transit-data-demo-raw"].name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.test_fetcher.email}"
}

# dev-archiver -> dev raw bucket (objectCreator)
resource "google_storage_bucket_iam_member" "dev_archiver_raw_creator" {
  bucket = google_storage_bucket.buckets["dev-jarvus-transit-data-demo-raw"].name
  role   = "roles/storage.objectCreator"
  member = "serviceAccount:${google_service_account.dev_archiver.email}"
}

# github-action -> parsed bucket (objectViewer + legacyObjectReader)
resource "google_storage_bucket_iam_member" "github_action_parsed_viewer" {
  bucket = google_storage_bucket.buckets["jarvus-transit-data-demo-parsed"].name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.github_action.email}"
}

resource "google_storage_bucket_iam_member" "github_action_parsed_legacy_reader" {
  bucket = google_storage_bucket.buckets["jarvus-transit-data-demo-parsed"].name
  role   = "roles/storage.legacyObjectReader"
  member = "serviceAccount:${google_service_account.github_action.email}"
}

# metabase-restic -> metabase-restic bucket (objectAdmin)
resource "google_storage_bucket_iam_member" "metabase_restic_bucket_admin" {
  bucket = google_storage_bucket.buckets["metabase-restic"].name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.metabase_restic.email}"
}


# -----------------------------------------------------------------------------
# GKE Cluster
# -----------------------------------------------------------------------------

resource "google_container_cluster" "demo" {
  name     = "demo-cluster"
  location = local.region

  network    = "default"
  subnetwork = "default"

  release_channel {
    channel = "REGULAR"
  }

  ip_allocation_policy {
    cluster_secondary_range_name  = "gke-demo-cluster-pods-bf26a7d3"
    services_secondary_range_name = "gke-demo-cluster-services-bf26a7d3"
  }

  # Default node pool was removed after creation — we manage pools separately
  remove_default_node_pool = true
  initial_node_count       = 1

  deletion_protection = true

  network_policy {
    enabled  = true
    provider = "CALICO"
  }

  addons_config {
    gce_persistent_disk_csi_driver_config {
      enabled = true
    }
  }

  lifecycle {
    ignore_changes = [initial_node_count]
  }
}

resource "google_container_node_pool" "default" {
  name     = "default-pool"
  location = local.region
  cluster  = google_container_cluster.demo.name

  initial_node_count = 2

  management {
    auto_repair  = true
    auto_upgrade = true
  }

  node_config {
    machine_type = "e2-medium"
  }
}

resource "google_container_node_pool" "dagster_runs" {
  name     = "dagster-runs"
  location = local.region
  cluster  = google_container_cluster.demo.name

  initial_node_count = 1

  management {
    auto_repair  = true
    auto_upgrade = true
  }

  node_config {
    machine_type = "e2-highmem-2"
  }
}


# -----------------------------------------------------------------------------
# BigQuery Datasets (containers only — dbt manages tables/views)
# -----------------------------------------------------------------------------

resource "google_bigquery_dataset" "external_transit_data" {
  dataset_id = "external_transit_data"
  location   = local.location
}

resource "google_bigquery_dataset" "staging" {
  dataset_id = "staging"
  location   = local.location
}

resource "google_bigquery_dataset" "transitdash" {
  dataset_id = "transitdash"
  location   = local.location
}


# -----------------------------------------------------------------------------
# Firewall rules (custom only — GKE-managed rules are excluded)
# -----------------------------------------------------------------------------

resource "google_compute_firewall" "k8s_lb_http" {
  name    = "k8s-fw-a328ef34eb45242309b55b9a409edbea"
  network = "default"

  description = "{\"kubernetes.io/service-name\":\"ingress-nginx/ingress-nginx-controller\", \"kubernetes.io/service-ip\":\"35.245.81.75\"}"

  allow {
    protocol = "tcp"
    ports    = ["80", "443"]
  }

  source_ranges = ["0.0.0.0/0"]
  target_tags   = ["gke-demo-cluster-bf26a7d3-node"]
}
