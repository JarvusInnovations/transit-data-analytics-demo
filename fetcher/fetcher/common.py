import base64
import datetime
import os
from enum import StrEnum
from typing import Dict, Optional, List, ClassVar, Any, Type, Callable

import pendulum
import requests
from pydantic import BaseModel, HttpUrl, validator, root_validator, Extra

from fetcher.metrics import (
    COMMON_LABELNAMES,
)

RAW_BUCKET = os.environ["RAW_BUCKET"]
PARSED_BUCKET = os.environ["PARSED_BUCKET"]

SERIALIZERS: Dict[Type, Callable] = {
    str: str,
    pendulum.Date: lambda dt: dt.to_date_string(),
    pendulum.DateTime: lambda ts: ts.to_iso8601_string(),
}


class FeedType(StrEnum):
    # gtfs/other standards
    gtfs_schedule = "gtfs_schedule"
    gtfs_rt__vehicle_positions = "gtfs_rt__vehicle_positions"
    gtfs_rt__trip_updates = "gtfs_rt__trip_updates"
    gtfs_rt__service_alerts = "gtfs_rt__service_alerts"
    # agency/vendor-specific
    septa__arrivals = "septa__arrivals"
    septa__train_view = "septa__train_view"
    septa__transit_view_all = "septa__transit_view_all"
    septa__bus_detours = "septa__bus_detours"
    septa__alerts_without_message = "septa__alerts_without_message"
    septa__alerts = "septa__alerts"
    septa__elevator_outages = "septa__elevator_outages"


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
        assert url is not None
        b64url = base64.urlsafe_b64encode(url.encode("utf-8")).decode("utf-8")
        return f"{b64url}.json"

    @property
    def table(self) -> str:
        return self.config.feed_type.value

    @property
    def gcs_key(self) -> str:
        hive_str = "/".join(
            [f"{key}={SERIALIZERS[type(getattr(self, key))](getattr(self, key))}" for key in self.partitions]
        )
        return f"{self.table}/{hive_str}/{self.filename}"

    @property
    # should we be wrapping or constructing a storage.Blob?!
    def uri(self) -> str:
        return f"{self.bucket}/{self.gcs_key}"

    @validator("ts")
    def parse_ts(cls, v):
        assert isinstance(v, datetime.datetime)
        return pendulum.instance(v).in_tz("UTC")

    @validator("contents", pre=True)
    def base64_contents(cls, v):
        return base64.b64decode(v) if isinstance(v, str) else v

    @validator("exception")
    def exception_must_exist_if_no_contents(cls, v, values):
        assert v or values["contents"]
        return v
