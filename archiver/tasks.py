import abc
import base64
import os
from enum import Enum
from typing import Dict, Optional, List, ClassVar, Any

import humanize
import pendulum
import requests
import typer
from google.cloud import storage
from huey import RedisHuey  # type: ignore
from pydantic import BaseModel, HttpUrl, validator, root_validator

from archiver.metrics import HUEY_TASK_SIGNALS

RAW_BUCKET = os.environ["RAW_BUCKET"]
PARSED_BUCKET = os.environ["PARSED_BUCKET"]

huey = RedisHuey(
    host=os.environ["HUEY_REDIS_HOST"],
)

client = storage.Client()


@huey.on_startup()
def on_startup():
    pass


@huey.signal()
def all_signal_handler(signal, task, exc=None):
    HUEY_TASK_SIGNALS.labels(
        signal=signal,
        name=task.kwargs["config"].name,
        url=task.kwargs["config"].url,
        exc_type=type(exc).__name__,
    ).inc()


class FeedType(str, Enum):
    realtime = "realtime"
    schedule = "schedule"


class KeyValue(BaseModel):
    name: str
    value: Optional[str]
    valueSecret: Optional[str]

    @root_validator
    def some_value(cls, values):
        assert values["value"] or values["valueSecret"], str(values)
        return values


class FetchConfig(BaseModel):
    name: str
    agency: Optional[str]
    feed_type: FeedType
    is_gtfs: bool
    description: Optional[str]
    url: HttpUrl
    schedule_url: Optional[HttpUrl]  # TODO: referential integrity check?
    table: str
    query: List[KeyValue] = []
    headers: List[KeyValue] = []


SERIALIZERS = {
    str: str,
    pendulum.Date: lambda dt: dt.to_date_string(),
    pendulum.DateTime: lambda ts: ts.to_iso8601_string(),
}


class FetchedFile(BaseModel, abc.ABC):
    table: str  # TODO: should this come from config?
    ts: pendulum.DateTime
    config: FetchConfig
    response_code: int
    response_headers: Dict
    exception: Optional[Exception]

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {
            pendulum.DateTime: lambda ts: ts.to_iso8601_string(),
            Exception: str,
        }

    @property
    @abc.abstractmethod
    def bucket(self) -> str:
        """GCS bucket."""

    @property
    @abc.abstractmethod
    def partitions(self) -> List[str]:
        """List of partition keys."""

    @property
    @abc.abstractmethod
    def filename(self) -> str:
        """"""

    # TODO: can we make this work?
    # @property
    # @abc.abstractmethod
    # def contents(self) -> Union[bytes, Dict]:
    #     """The contents as bytes."""

    @property
    def dt(self) -> pendulum.Date:
        return self.ts.date()

    @property
    def base64url(self) -> str:
        # TODO: add non-auth query params
        return base64.urlsafe_b64encode(self.config.url.encode("utf-8")).decode("utf-8")

    @property
    def gcs_key(self) -> str:
        hive_str = "/".join(
            [f"{key}={SERIALIZERS[type(getattr(self, key))](getattr(self, key))}" for key in self.partitions]
        )
        return f"{self.table}/{hive_str}/{self.filename}"

    # @validator("bucket")
    # def bucket_prefix(cls, v):
    #     return v if v.startswith("gs://") else f"gs://{v}"

    @validator("exception")
    def exception_must_exist_if_no_contents(cls, values):
        assert values["contents"] or values["exception"]
        return values


class RawFetchedFile(FetchedFile):
    bucket: ClassVar[str] = RAW_BUCKET
    partitions: ClassVar[List[str]] = ["dt", "ts"]
    contents: bytes

    class Config:
        json_encoders = {
            bytes: lambda b: base64.b64encode(b).decode(),
        }

    @property
    def filename(self) -> str:
        return self.base64url

    @validator("contents", pre=True)
    def base64_contents(cls, v):
        return base64.b64decode(v) if isinstance(v, str) else v


class ParsedFetchedFile(FetchedFile):
    bucket: ClassVar[str] = PARSED_BUCKET
    partitions: ClassVar[List[str]] = ["dt", "base64url"]
    records: List[Dict[str, Any]]

    @property
    def filename(self) -> str:
        return self.ts.to_iso8601_string()

    @validator("ts")
    def every_ten_minutes(cls, v):
        assert v.minute
        return v


@huey.task(
    expires=int(os.getenv("HUEY_FETCH_CONFIG_EXPIRES", 5)),
)
def fetch_config(tick: pendulum.DateTime, config: FetchConfig, dry: bool = False):
    response = requests.get(config.url)
    response.raise_for_status()

    raw = RawFetchedFile(
        table=config.table,
        ts=tick,
        config=config,
        response_code=response.status_code,
        response_headers=response.headers,
        contents=response.content,
    )

    msg = f"Saved {humanize.naturalsize(len(raw.contents))} to {raw.bucket}/{raw.gcs_key}"
    if dry:
        typer.secho(f"DRY RUN: {msg}")
    else:
        client.bucket(raw.bucket.removeprefix("gs://")).blob(raw.gcs_key).upload_from_string(raw.json(), client=client)
        typer.secho(msg)
