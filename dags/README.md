# dags

This is a [Dagster](https://dagster.io/) project scaffolded with [`dagster project scaffold`](https://docs.dagster.io/getting-started/create-new-project).

## Getting started

First, ensure that asdf is installed and install dependencies from the repository root:
```bash
asdf install
uv sync --group dagster-dev
```

Then, start the Dagster UI web server (optionally specifying a port):
```bash
uv run dagster dev <--port 1234>
```

Open http://localhost:<port, 3000 default> with your browser to see the project.

### Unit testing

Tests are in the `dags_tests` directory and you can run tests using `pytest`:

```bash
uv run pytest dags_tests
```

### Deployment

Dagster itself is deployed via hologit and Helm; the [values file](../kubernetes/values/prod-dagster.yml) contains any Kubernetes overrides. The dags/source code in this folder are deployed by pushing a Docker image (currently `ghcr.io/jarvusinnovations/transit-data-analytics-demo/dags:latest` built from the root [Containerfile](../Containerfile)) that is then referenced by a user code deployment in the values.
