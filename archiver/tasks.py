import base64
from typing import Dict, Optional

import humanize
import requests
import typer
from huey import SqliteHuey
from pydantic import BaseModel, HttpUrl, validator
import pendulum

from archiver.metrics import HUEY_TASK_SIGNALS

huey = SqliteHuey(filename="/tmp/demo.db")


@huey.signal()
def all_signal_handler(signal, task, exc=None):
    HUEY_TASK_SIGNALS.labels(
        signal=signal,
        name=task.kwargs["config"].name,
        url=task.kwargs["config"].url,
        exc_type=type(exc).__name__,
    ).inc()


class FetchConfig(BaseModel):
    name: str
    description: Optional[str]
    url: HttpUrl


class FetchedFile(BaseModel):
    bucket: str
    table: str
    ts: pendulum.DateTime
    config: FetchConfig
    response_headers: Dict
    filename: str
    contents: str

    @property
    def gcs_key(self) -> str:
        hive_str = "/".join(
            [
                f"dt={self.ts.to_date_string()}",
                f"ts={self.ts.to_iso8601_string()}",
                f"base64url={base64.urlsafe_b64encode(self.config.url.encode('utf-8')).decode('utf-8')}",
            ]
        )
        return f"{self.bucket}/{self.table}/{hive_str}/{self.filename}"

    @validator("bucket")
    def bucket_prefix(cls, v):
        return v if v.startswith("gs://") else f"gs://{v}"

    @validator("contents", pre=True)
    def base64_contents(cls, v):
        return base64.b64encode(v) if isinstance(v, bytes) else v


@huey.task(
    expires=5,
)
def fetch_config(tick: pendulum.DateTime, config: FetchConfig, dry: bool = False):
    print(tick, config, flush=True)

    resp = requests.get(config.url)

    fetched_file = FetchedFile(
        bucket="gs://",
        table="",
        ts=tick,
        config=config,
        response_headers=resp.headers,
        filename="",
        contents=resp.content,
    )

    msg = f"Saving {humanize.naturalsize(len(fetched_file.contents))} to {fetched_file.gcs_key}"
    if dry:
        typer.secho(f"DRY RUN: {msg}")
    else:
        typer.secho(msg)
        # TODO: implement me
        pass
