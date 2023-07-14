from enum import StrEnum
from pathlib import Path
from typing import List, Optional

from invoke import task
from pydantic import BaseModel

class Driver(StrEnum):
    helm = "helm"
    kustomize = "kustomize"

class Deployment(BaseModel):
    name: str
    driver: Driver

    # helm stuff
    helm_chart: Optional[Path]
    helm_values: Optional[List[Path]]

    # kustomize stuff

    @property
    def values_cli(self) -> str:
        return " ".join(f"-f {values}" for values in self.helm_values)

class JarvusConfig(BaseModel):
    deployments: List[Deployment]

@task
def hdiff(c):
    c.run(f"helm diff upgrade {}")
