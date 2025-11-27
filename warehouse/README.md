This directory contains the [dbt](https://www.getdbt.com/) project for this demonstration project.

## Setup

### Prerequisites

Before you can set up the dbt project, you need to:
* Have [asdf](https://asdf-vm.com/) installed and run `asdf install` from the repo root
* [Initialize/authenticate to GCP](https://cloud.google.com/sdk/docs/initializing)

### Installation

From the repository root, run:

1. `uv sync --group dbt-dev` to install dbt and development dependencies
2. `uv run dbt deps` to [install dbt packages](https://docs.getdbt.com/reference/commands/deps)
3. `uv run dbt compile` to confirm that the project compiles

### Troubleshooting

If you get errors about a file called `~/.dbt/profiles.yml`, you may need to manually move this file to your home directory and/or combine it with an existing `profiles.yml` file if you already have one.

## Running the project

Once you have completed the setup, you can [run dbt commands](https://docs.getdbt.com/reference/dbt-commands) to build and test models.

This project uses the [dbt-external-tables](https://github.com/dbt-labs/dbt-external-tables) package to build external tables; to run these tables, you will need to run `dbt run-operation stage_external_sources --vars "ext_full_refresh: true"` (the caching is fairly aggressive, so if you have made any changes you will likely want to use the full refresh flag.) To make the external tables read from the test bucket, run `uv run dbt run-operation stage_external_sources --vars "{external_data_bucket: test-jarvus-transit-data-demo-parsed, ext_full_refresh: true}"`.
