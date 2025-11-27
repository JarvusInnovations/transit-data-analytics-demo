# Claude Code Assistant Guide

This document provides context and references for the Claude Code assistant working with this repository.

## Repository Overview

Transit Data Analytics Demo - A system for fetching, processing, and analyzing public transit data feeds.

## Hologit

This repository uses Hologit for compositing and transforming Kubernetes manifests across different deployment environments.

**When to consult Hologit documentation:**

- Working with `.holo/` directory configurations
- Setting up or modifying holobranches (especially `k8s-manifests`)
- Configuring Kustomize or other lenses
- Understanding branch projections and GitOps workflows
- Debugging why files appear in unexpected locations in projected branches
- Understanding the underscore prefix "splat" operator in mapping files
- Understanding how mapping filenames serve as default holosource values
- Setting up new deployment environments or namespaces

See [docs/hologit.md](docs/hologit.md) for detailed documentation on Hologit concepts, patterns, and this repository's implementation.

**Current implementation highlights:**

- The `k8s-manifests` holobranch processes Kustomize overlays for `fetcher-test` and `fetcher-prod` environments
- Holospace is configured as `tdad` (short for transit-data-analytics-demo)
- Lenses are stored in `.holo/branches/k8s-manifests.lenses/`
- Each environment has separate mappings to enable isolated Kustomize builds

## Key Technologies

- **Python/uv**: Main application runtime and dependency management
- **asdf**: Tool version management (Python, uv)
- **Kubernetes/Kustomize**: Container orchestration and configuration management
- **Docker**: Container runtime for local development and production
- **GitHub Actions**: CI/CD pipeline for building and deploying
- **Hologit**: Git-based content composition and transformation

## Dependency Groups

The project uses uv dependency groups to manage dependencies for different components:

- `fetcher` / `fetcher-dev`: Feed fetching service
- `dagster` / `dagster-dev`: Dagster pipelines
- `dbt` / `dbt-dev`: dbt data warehouse
- `dev`: All development dependencies (default)

## Project Structure

- `fetcher/`: Python application for fetching transit data feeds
- `dags/`: Dagster pipelines for data processing
- `warehouse/`: dbt project for data modeling in BigQuery
- `kubernetes/`: Kubernetes manifests and Kustomize configurations
- `.holo/`: Hologit configuration for branch composition
- `.github/workflows/`: GitHub Actions CI/CD pipelines
- `Containerfile`: Multi-stage container build for all services
