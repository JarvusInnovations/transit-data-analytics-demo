import base64
import datetime
import os
from typing import Dict, Optional, List, ClassVar, Any

import pendulum
import requests
from pydantic import BaseModel, HttpUrl, validator, root_validator, Extra

from fetcher.feed_types import FeedType
from fetcher.metrics import (
    COMMON_LABELNAMES,
)

RAW_BUCKET = os.environ["RAW_BUCKET"]
PARSED_BUCKET = os.environ["PARSED_BUCKET"]

SERIALIZERS = {
    str: str,
    pendulum.Date: lambda dt: dt.to_date_string(),
    pendulum.DateTime: lambda ts: ts.to_iso8601_string(),
}


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
        b64url = base64.urlsafe_b64encode(url.encode("utf-8")).decode("utf-8")
        return f"{b64url}.json"

    @property
    def gcs_key(self) -> str:
        hive_str = "/".join(
            [f"{key}={SERIALIZERS[type(getattr(self, key))](getattr(self, key))}" for key in self.partitions]
        )
        return f"{self.config.feed_type.value}/{hive_str}/{self.filename}"

    @validator("ts")
    def parse_ts(cls, v):
        return pendulum.instance(v, tz="UTC") if isinstance(v, datetime.datetime) else v

    @validator("contents", pre=True)
    def base64_contents(cls, v):
        return base64.b64decode(v) if isinstance(v, str) else v

    @validator("exception")
    def exception_must_exist_if_no_contents(cls, v, values):
        assert v or values["contents"]
        return v


# TODO: dedupe this with above
class HourAgg(BaseModel):
    bucket: ClassVar[str] = PARSED_BUCKET
    partitions: ClassVar[List[str]] = ["dt", "hour"]
    first_file: RawFetchedFile

    @property
    def dt(self):
        return self.first_file.dt

    @property
    def hour(self):
        return self.first_file.hour

    @property
    def filename(self):
        return f"{self.first_file.base64url}.json"

    @property
    def gcs_key(self) -> str:
        hive_str = "/".join(
            [f"{key}={SERIALIZERS[type(getattr(self, key))](getattr(self, key))}" for key in self.partitions]
        )
        return f"{self.first_file.config.feed_type.value}/{hive_str}/{self.filename}"


class FetchedRecord(BaseModel):
    file: RawFetchedFile
    record: Dict[str, Any]
