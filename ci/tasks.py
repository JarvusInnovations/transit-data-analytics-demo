from enum import StrEnum
from pathlib import Path
from typing import List, Optional

from invoke import task, Context, UnexpectedExit
from pydantic import BaseModel, parse_obj_as


class Driver(StrEnum):
    helm = "helm"
    kustomize = "kustomize"


class Deployment(BaseModel):
    name: str
    namespace: Optional[str] = None
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
        return " ".join(f"-f {values}" for values in self.values)


class JarvusConfig(BaseModel):
    deployments: List[Deployment]


@task
def parse_jarvus_config(c: Context):
    c.update({"jarvus_config": parse_obj_as(JarvusConfig, c.config["jarvus"]._config)})


@task(parse_jarvus_config)
def hdiff(c):
    # c.run(f"helm diff upgrade {}")
    pass


@task(parse_jarvus_config)
def happly(c):
    for deployment in c.config.jarvus_config.deployments:
        c.run(f"helm dependency build ../{deployment.chart}")

        try:
            c.run(f"kubectl get ns {deployment.namespace}")
            # c.run(f"helm upgrade {deployment.name} ../{deployment.chart} {deployment.namespace_cli} {deployment.values_cli}")
        except UnexpectedExit as e:
            assert f'namespaces "{deployment.namespace}" not found' in e
            c.run(f"kubectl create ns {deployment.namespace}")
            # c.run(f"helm install {deployment.name} ../{deployment.chart} {deployment.namespace_cli} {deployment.values_cli}")
