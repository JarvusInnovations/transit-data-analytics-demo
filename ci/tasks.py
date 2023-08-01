from enum import StrEnum
from pathlib import Path
from typing import List, Optional, NoReturn

from invoke import task, Context
from pydantic import BaseModel, model_validator


# TODO: use GitPython to form absolute paths
# TODO: deploy secrets from Secret Manager


def _assert_never(x: NoReturn) -> NoReturn:
    assert False, "Unhandled type: {}".format(type(x).__name__)


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
    directory: Optional[Path] = None

    @property
    def namespace_cli(self) -> str:
        return f"-n {self.namespace}" if self.namespace else ""

    @property
    def values_cli(self) -> str:
        return " ".join(f"-f ../{values}" for values in self.values)

    @model_validator(mode="after")
    def check_fields_for_driver(self):
        if self.driver == Driver.helm:
            assert self.chart is not None
        elif self.driver == Driver.kustomize:
            assert self.directory is not None
        else:
            raise _assert_never(self.driver)
        return self


class JarvusConfig(BaseModel):
    deployments: List[Deployment]


@task
def helm_plugins(c):
    c.run("helm plugin install https://github.com/databus23/helm-diff", warn=True)


@task
def parse_jarvus_config(c: Context):
    c.update({"jarvus_config": JarvusConfig(**c.config["jarvus"]._config)})


@task(helm_plugins, parse_jarvus_config)
def diff(c, name=None):
    for deployment in c.config.jarvus_config.deployments:
        if not name or name == deployment.name:
            if deployment.driver == Driver.helm:
                if deployment.dependency:
                    c.run(f"helm dependency build ../{deployment.chart}")
                args = [
                    "helm",
                    "diff",
                    "upgrade",
                    deployment.name,
                    f"../{deployment.chart}",
                    deployment.namespace_cli,
                    deployment.values_cli,
                    "-allow-unreleased",
                ]
                c.run(" ".join(args))
            elif deployment.driver == Driver.kustomize:
                pass
            else:
                raise _assert_never(deployment.driver)


@task(helm_plugins, parse_jarvus_config)
def apply(c, name=None):
    deployment: Deployment
    for deployment in c.config.jarvus_config.deployments:
        if not name or name == deployment.name:
            if deployment.driver == Driver.helm:
                if deployment.dependency:
                    c.run(f"helm dependency build ../{deployment.chart}")
                args = [
                    "helm",
                    "upgrade",
                    "--install",
                    "--create-namespace",
                    deployment.name,
                    f"../{deployment.chart}",
                    deployment.namespace_cli,
                    deployment.values_cli,
                ]
                c.run(" ".join(args))
            elif deployment.driver == Driver.kustomize:
                c.run(f"kubectl apply -k ../{deployment.directory}")
            else:
                raise _assert_never(deployment.driver)
