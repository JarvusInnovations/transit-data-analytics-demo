# dags

This is a [Dagster](https://dagster.io/) project scaffolded with [`dagster project scaffold`](https://docs.dagster.io/getting-started/create-new-project).

## Getting started

First, ensure that poetry is installed and then install the dependencies.
```bash
curl -sSL https://install.python-poetry.org | python3 -
poetry install
```

Then, start the Dagster UI web server (optionally specifying a port):
```bash
poetry run dagster dev <--port 1234>
```

Open http://localhost:<port, 3000 default> with your browser to see the project.

### Unit testing

Tests are in the `dags_tests` directory and you can run tests using `pytest`:

```bash
poetry run pytest dags_tests
```

### Deployment
Dagster itself is deployed via hologit and Helm; the [values file](../kubernetes/values/prod-dagster.yml) contains any Kubernetes overrides. The dags/source code in this folder are deployed by pushing a Docker image (currently `ghcr.io/jarvusinnovations/transit-data-analytics-demo/dags:latest` built from [this folder](./Dockerfile)) that is then referenced by a user code deployment in the values.
