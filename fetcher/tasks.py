import base64
import os
from enum import Enum
from typing import Dict, Optional, List, ClassVar, Any

import humanize
import pendulum
import requests
import typer
from google.cloud import storage  # type: ignore
from huey import RedisHuey  # type: ignore
from pydantic import BaseModel, HttpUrl, validator, root_validator, Extra

from fetcher.metrics import (
    HUEY_TASK_SIGNALS,
    FETCH_REQUEST_DELAY_SECONDS,
    COMMON_LABELNAMES,
    FETCH_REQUEST_DURATION_SECONDS,
    FETCH_SAVE_DURATION_SECONDS,
)

RAW_BUCKET = os.environ["RAW_BUCKET"]

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
        exc_type=type(exc).__name__,
        **task.kwargs["config"].labels,
    ).inc()


# TODO: in the future, we may have multiple file formats
#  for the same feed_type (e.g. we could get a JSON
#  response instead of proto)
class FeedType(str, Enum):
    # gtfs/other standards
    gtfs_schedule = "gtfs_schedule"
    gtfs_vehicle_positions = "gtfs_vehicle_positions"
    gtfs_trip_updates = "gtfs_trip_updates"
    gtfs_service_alerts = "gtfs_service_alerts"
    # agency/vendor-specific
    septa__arrivals = "septa__arrivals"
    septa__train_view = "septa__train_view"
    septa__transit_view_all = "septa__transit_view_all"


class KeyValues(BaseModel):
    key: str
    values: List[str]


class KeyValue(BaseModel):
    key: str
    value: Optional[str]
    valueSecret: Optional[str]

    @root_validator
    def some_value(cls, values):
        assert values["value"] or values["valueSecret"], str(values)
        return values


class FeedConfig(BaseModel):
    name: str
    url: HttpUrl
    feed_type: FeedType
    agency: Optional[str]
    description: Optional[str]
    schedule_url: Optional[HttpUrl]  # TODO: referential integrity check?
    query: List[KeyValue] = []
    headers: List[KeyValue] = []
    pages: List[KeyValues] = []

    class Config:
        extra = Extra.forbid

    @property
    def labels(self) -> Dict[str, Any]:
        return {k: v for k, v in self.dict().items() if k in COMMON_LABELNAMES}


SERIALIZERS = {
    str: str,
    pendulum.Date: lambda dt: dt.to_date_string(),
    pendulum.DateTime: lambda ts: ts.to_iso8601_string(),
}


class RawFetchedFile(BaseModel):
    bucket: ClassVar[str] = RAW_BUCKET
    partitions: ClassVar[List[str]] = ["dt", "hour", "ts", "base64url"]
    ts: pendulum.DateTime
    config: FeedConfig
    page: List[KeyValue] = []
    response_code: int
    response_headers: Dict
    contents: bytes
    exception: Optional[Exception]

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {
            bytes: lambda b: base64.b64encode(b).decode(),
            Exception: str,
            pendulum.DateTime: lambda ts: ts.to_iso8601_string(),
        }

    @property
    def dt(self) -> pendulum.Date:
        return self.ts.date()

    @property
    def hour(self) -> pendulum.DateTime:
        return self.ts.replace(minute=0, second=0)

    @property
    def base64url(self) -> str:
        # TODO: add non-auth query params
        url = requests.Request(url=self.config.url, params={kv.key: kv.value for kv in self.config.query}).url
        return base64.urlsafe_b64encode(url.encode("utf-8")).decode("utf-8")

    @property
    def filename(self) -> str:
        params_with_page = {
            **{kv.key: kv.value for kv in self.config.query if kv.value},  # exclude secrets
            **{kv.key: kv.value for kv in self.page},
        }
        url = requests.Request(url=self.config.url, params=params_with_page).prepare().url
        b64url = base64.urlsafe_b64encode(url.encode("utf-8")).decode("utf-8")
        return f"{b64url}.json"

    @property
    def gcs_key(self) -> str:
        hive_str = "/".join(
            [f"{key}={SERIALIZERS[type(getattr(self, key))](getattr(self, key))}" for key in self.partitions]
        )
        return f"{self.config.feed_type.value}/{hive_str}/{self.filename}"

    @validator("contents", pre=True)
    def base64_contents(cls, v):
        return base64.b64decode(v) if isinstance(v, str) else v

    @validator("exception")
    def exception_must_exist_if_no_contents(cls, values):
        assert values["contents"] or values["exception"]
        return values


@huey.task(
    expires=int(os.getenv("HUEY_FETCH_CONFIG_EXPIRES", 5)),
)
def fetch_feed(tick: pendulum.DateTime, config: FeedConfig, page: List[KeyValue] = [], dry: bool = False):
    FETCH_REQUEST_DELAY_SECONDS.labels(**config.labels).observe((pendulum.now() - tick).total_seconds())

    with FETCH_REQUEST_DURATION_SECONDS.labels(**config.labels).time():
        # TODO: the FeedConfig should manage the URL creation generically
        response = requests.get(
            config.url,
            params={
                **{kv.key: kv.value for kv in config.query},
                **{kv.key: kv.value for kv in page},
            },
        )
    response.raise_for_status()

    raw = RawFetchedFile(
        ts=tick,
        config=config,
        page=page,
        response_code=response.status_code,
        response_headers=response.headers,
        contents=response.content,
    )

    msg = f"Saved {humanize.naturalsize(len(raw.contents))} to {raw.bucket}/{raw.gcs_key}"
    if dry:
        typer.secho(f"DRY RUN: {msg}")
    else:
        with FETCH_SAVE_DURATION_SECONDS.labels(**config.labels).time():
            client.bucket(raw.bucket.removeprefix("gs://")).blob(raw.gcs_key).upload_from_string(
                raw.json(), client=client
            )
        typer.secho(msg)
