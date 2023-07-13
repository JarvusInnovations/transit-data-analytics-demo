This directory contains the [dbt](https://www.getdbt.com/) project for this demonstration project.

## Setup

### Happy path

To run this project for the first time, run the following in this directory:

1. `poetry install` to [use Poetry to install](https://python-poetry.org/docs/cli/#install) project dependencies
2. `poetry run dbt deps` to [install dbt packages](https://docs.getdbt.com/reference/commands/deps)
3. `poetry run dbt compile` to [confirm that the project compiles]

### Troubleshooting

If any of the above commands do not work, you may need to try the following:

* [Update Poetry](https://python-poetry.org/docs/cli/#self-update) with `poetry self update`
