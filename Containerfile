# Use a Python image with uv pre-installed
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS base

ENV UV_PROJECT_ENVIRONMENT=/usr/local

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy


## FETCHER CONTAINER
FROM base AS fetcher

LABEL org.opencontainers.image.source=https://github.com/JarvusInnovations/transit-data-analytics-demo

WORKDIR /app

# Install dependencies
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --only-group fetcher

# Copy application code
COPY fetcher/feeds.yaml .
COPY fetcher/fetcher ./fetcher

CMD ["python", "-m", "archiver"]


## DBT BUILD STAGE
FROM base AS dbt-build

WORKDIR /opt/app

# Install dbt dependencies
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --only-group dbt

# Copy and build dbt project
WORKDIR /opt/app/warehouse
COPY warehouse .
RUN uv run dbt deps


## WAREHOUSE (DBT) CONTAINER
FROM base AS warehouse

LABEL org.opencontainers.image.source=https://github.com/JarvusInnovations/transit-data-analytics-demo

WORKDIR /app

# Install dbt dependencies
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --only-group dbt

# Copy dbt project
COPY warehouse warehouse
WORKDIR /app/warehouse
RUN uv run dbt deps

CMD ["dbt"]


## DAGSTER PIPELINES CONTAINER
FROM base AS dagster-pipelines

LABEL org.opencontainers.image.source=https://github.com/JarvusInnovations/transit-data-analytics-demo

WORKDIR /opt/dagster

# Install dagster dependencies
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --only-group dagster-deploy-pipelines

# Copy pipeline code and dbt project
COPY dags dags
COPY --from=dbt-build /opt/app/warehouse warehouse

EXPOSE 3030
CMD ["python"]
