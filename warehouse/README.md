This directory contains the [dbt](https://www.getdbt.com/) project for this demonstration project.

## Setup

### Happy path

Before you can set up the dbt project, you need to:
* [Install poetry](https://python-poetry.org/docs/#installation)
* [Initialize/authenticate to GCP](https://cloud.google.com/sdk/docs/initializing)

To set up this dbt project, run the following in this directory:

1. `poetry install` to [use Poetry to install](https://python-poetry.org/docs/cli/#install) project dependencies
2. `poetry run dbt deps` to [install dbt packages](https://docs.getdbt.com/reference/commands/deps)
3. `poetry run dbt compile` to confirm that the project compiles

### Troubleshooting

If any of the above commands do not work, you may need to try the following:

* [Update Poetry](https://python-poetry.org/docs/cli/#self-update) with `poetry self update`
* If you get errors about a file called `~/.dbt/profiles.yml`, you may need to manually move this file to your home directory and/or combine it with an existing `profiles.yml` file if you already have one.
