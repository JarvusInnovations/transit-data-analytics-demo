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
