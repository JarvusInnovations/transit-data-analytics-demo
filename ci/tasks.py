from enum import StrEnum
from pathlib import Path
from typing import List, Optional

from invoke import task, Context
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
    dependency: bool = True
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
def diff(c, name=None):
    for deployment in c.config.jarvus_config.deployments:
        if not name or name == deployment.name:
            if deployment.driver == Driver.helm:
                if deployment.dependency:
                    c.run(f"helm dependency build ../{deployment.chart}")
                c.run(
                    f"helm diff upgrade {deployment.name} ../{deployment.chart} {deployment.namespace_cli} {deployment.values_cli} --allow-unreleased"
                )
            elif deployment.driver == Driver.kustomize:
                pass
            else:
                raise RuntimeError


@task(helm_plugins, parse_jarvus_config)
def apply(c, name=None):
    deployment: Deployment
    for deployment in c.config.jarvus_config.deployments:
        if not name or name == deployment.name:
            if deployment.driver == Driver.helm:
                if deployment.dependency:
                    c.run(f"helm dependency build ../{deployment.chart}")

                c.run(
                    f"helm upgrade --install --create-namespace  {deployment.name} ../{deployment.chart} {deployment.namespace_cli} {deployment.values_cli}"
                )
            elif deployment.driver == Driver.kustomize:
                c.run(f"kubectl apply -k ../{deployment.path}")
            else:
                raise RuntimeError
