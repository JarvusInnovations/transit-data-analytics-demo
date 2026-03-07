# transit-data-analytics-demo

A demonstration project showcasing modern transit data analytics practices.

## Setup

This project uses [uv](https://docs.astral.sh/uv/) for dependency management and [asdf](https://asdf-vm.com/) for tool version management.

### Prerequisites

- Install [asdf](https://asdf-vm.com/guide/getting-started.html)
- Run `asdf install` to install Python and uv versions from `.tool-versions`

### Install dependencies

```bash
uv sync  # Installs all dev dependencies by default
```

### Dependency groups

The project uses dependency groups to manage dependencies for different components:

- `uv sync --group fetcher-dev` - Fetcher development
- `uv sync --group dagster-dev` - Dagster/dags development
- `uv sync --group dbt-dev` - dbt/warehouse development
- `uv sync --group dev` - All development dependencies (default)

## pre-commit

Before contributing to the project, please run `pre-commit install` in the root directory of the repository to configure pre-commit linting/style checks.

## Inspecting saved files

For raw files, we save the contents as a base64-encoded string within a JSON
object that we control. You can inspect by decoding the field; a couple examples:

```bash
gsutil cat gs://test-jarvus-transit-data-demo-raw/septa__bus_detours/dt=2023-07-09/hour=2023-07-09T01:00:00Z/ts=2023-07-09T01:00:00Z/base64url=aHR0cHM6Ly93d3czLnNlcHRhLm9yZy9hcGkvQnVzRGV0b3Vycy9pbmRleC5waHA=/aHR0cHM6Ly93d3czLnNlcHRhLm9yZy9hcGkvQnVzRGV0b3Vycy9pbmRleC5waHA=.json | jq -r .contents | base64 -d | jq
```

```bash
gsutil cat gs://test-jarvus-transit-data-demo-raw/gtfs_schedule/dt=2023-07-14/hour=2023-07-14T14:00:00-04:00/ts=2023-07-14T14:12:19-04:00/base64url=aHR0cHM6Ly93d3cucmlkZXBydC5vcmcvZGV2ZWxvcGVycmVzb3VyY2VzL0dURlMuemlw/aHR0cHM6Ly93d3cucmlkZXBydC5vcmcvZGV2ZWxvcGVycmVzb3VyY2VzL0dURlMuemlw.json | jq -r .contents | base64 -d > gtfs.zip
```

## Infrastructure Teardown (March 2026)

This project's infrastructure was wound down in favor of a replacement project running under a different GCP project. The GCS data and BigQuery datasets were preserved; compute resources (GKE, etc.) were destroyed via OpenTofu.

### What was preserved

- **GCS buckets** — All 8 buckets in `transit-data-analytics-demo` remain intact with their data
- **BigQuery datasets** — `external_transit_data`, `staging`, `transitdash` remain (no cost when not queried). Tables/views are managed by dbt and can be recreated with `dbt run`
- **OpenTofu config** — `tf/` contains the full infrastructure definition, ready to re-apply

### Database backups

- **Metabase PostgreSQL** — `gs://transit-data-demo-metadata/backups/metabase-postgresql-20260307.sql.gz` (also backed up via restic to `gs://metabase-restic/`)
- **Dagster PostgreSQL** — `gs://transit-data-demo-metadata/backups/dagster-postgresql-20260307.sql.gz` (run history and partition materialization state)

### Re-deploying

To bring the infrastructure back up:

1. `cd tf && tofu init && tofu apply` — recreates GKE cluster, service accounts, IAM bindings
2. Re-deploy Kubernetes workloads (fetcher, dagster, ingress-nginx, cert-manager, etc.)
3. `cd warehouse && dbt run-operation stage_external_sources && dbt run` — recreates BigQuery tables/views
4. Optionally restore Dagster DB to recover partition materialization history
