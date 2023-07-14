from enum import StrEnum
from pathlib import Path
from typing import List, Optional

from invoke import task, Context, Result
from pydantic import BaseModel, parse_obj_as


# TODO: use GitPython to form absolute paths


class Driver(StrEnum):
    helm = "helm"
    kustomize = "kustomize"


class Deployment(BaseModel):
    name: str
    namespace: str
    driver: Driver

    # helm specific
    chart: Optional[Path] = None
    values: List[Path] = []

    # kustomize specific
    path: Optional[Path] = None

    @property
    def namespace_cli(self) -> str:
        return f"-n {self.namespace}" if self.namespace else ""

    @property
    def values_cli(self) -> str:
        return " ".join(f"-f ../{values}" for values in self.values)


class JarvusConfig(BaseModel):
    deployments: List[Deployment]


@task
def helm_plugins(c):
    c.run("helm plugin install https://github.com/databus23/helm-diff", warn=True)


@task
def parse_jarvus_config(c: Context):
    c.update({"jarvus_config": parse_obj_as(JarvusConfig, c.config["jarvus"]._config)})


@task(helm_plugins, parse_jarvus_config)
def hdiff(c, name=None):
    for deployment in c.config.jarvus_config.deployments:
        if not name or name == deployment.name:
            c.run(f"helm dependency build ../{deployment.chart}")
            c.run(
                f"helm diff upgrade {deployment.name} ../{deployment.chart} {deployment.namespace_cli} {deployment.values_cli}"
            )


@task(helm_plugins, parse_jarvus_config)
def happly(c, name=None):
    for deployment in c.config.jarvus_config.deployments:
        if not name or name == deployment.name:
            c.run(f"helm dependency build ../{deployment.chart}")

            res: Result = c.run(f"kubectl get ns {deployment.namespace}", warn=True)

            if res.exited:
                assert f'namespaces "{deployment.namespace}" not found' in res.stderr
                c.run(f"kubectl create ns {deployment.namespace}")

            c.run(
                f"helm upgrade --install {deployment.name} ../{deployment.chart} {deployment.namespace_cli} {deployment.values_cli}"
            )
