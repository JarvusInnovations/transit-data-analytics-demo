# This is copy-pasted from fetcher, but maybe fetcher should be a module in here?
import abc
import base64
import datetime
import os
from enum import StrEnum
from typing import Dict, Optional, List, ClassVar, Any, Type, Iterable, Callable, Union, Mapping

import pendulum
import requests
import typer.colors
import yaml
from google.cloud import storage  # type: ignore
from pydantic import BaseModel, HttpUrl, validator, root_validator, Extra, parse_obj_as, Field
from pydantic.dataclasses import dataclass
from slugify import slugify


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


class GtfsScheduleFileType(StrEnum):
    agency_txt = "agency.txt"
    stops_txt = "stops.txt"
    routes_txt = "routes.txt"
    trips_txt = "trips.txt"
    stop_times_txt = "stop_times.txt"
    calendar_txt = "calendar.txt"
    calendar_dates_txt = "calendar_dates.txt"
    fare_attributes_txt = "fare_attributes.txt"
    fare_rules_txt = "fare_rules.txt"
    fare_media_txt = "fare_media.txt"
    fare_products_txt = "fare_products.txt"
    fare_leg_rules_txt = "fare_leg_rules.txt"
    fare_transfer_rules_txt = "fare_transfer_rules.txt"
    areas_txt = "areas.txt"
    stop_areas_txt = "stop_areas.txt"
    shapes_txt = "shapes.txt"
    frequencies_txt = "frequencies.txt"
    transfers_txt = "transfers.txt"
    pathways_txt = "pathways.txt"
    levels_txt = "levels.txt"
    translations_txt = "translations.txt"
    feed_info_txt = "feed_info.txt"
    attributions_txt = "attributions.txt"


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
        from fetcher.metrics import COMMON_LABELNAMES  # type: ignore[import]

        return {k: v for k, v in self.dict().items() if k in COMMON_LABELNAMES}


class RawFetchedFile(BaseModel):
    bucket: ClassVar[str] = RAW_BUCKET
    partitions: ClassVar[List[str]] = ["dt", "hour", "ts", "base64url"]
    ts: pendulum.DateTime
    config: FeedConfig
    page: List[KeyValue] = []
    response_code: int
    response_headers: Mapping
    exception: Optional[Exception] = None

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


class RawFetchedFileWithContents(RawFetchedFile):
    contents: bytes

    @validator("contents", pre=True)
    def base64_contents(cls, v):
        return base64.b64decode(v) if isinstance(v, str) else v

    @validator("exception")
    def exception_must_exist_if_no_contents(cls, v, values):
        assert v or values["contents"]
        return v


class ParsedRecordMetadata(BaseModel):
    line_number: Optional[int]


class ParsedRecord(BaseModel):
    file: RawFetchedFile
    record: Dict[str, Any]
    metadata: ParsedRecordMetadata


# TODO: dedupe this with above, and maybe __root__ should be List[FetchedRecord]?
# this is a dataclass so we can use it as a dictionary key
@dataclass(eq=True, frozen=True)
class HourAgg(BaseModel):
    bucket: ClassVar[str] = PARSED_BUCKET
    table: Union[FeedType, GtfsScheduleFileType]
    partitions: ClassVar[List[str]] = ["dt", "hour"]
    base64url: str
    hour: pendulum.DateTime

    @validator("hour")
    def convert_hour(cls, v) -> pendulum.DateTime:
        assert isinstance(v, datetime.datetime)
        return pendulum.instance(v).in_tz("UTC")

    @property
    def dt(self):
        return self.hour.date()

    @property
    def filename(self):
        return f"{self.base64url}.jsonl.gz"

    @property
    def gcs_key(self) -> str:
        hive_str = "/".join(
            [f"{key}={SERIALIZERS[type(getattr(self, key))](getattr(self, key))}" for key in self.partitions]
        )
        hive_table = (
            f"gtfs_schedule__{slugify(self.table, separator='_')}"
            if isinstance(self.table, GtfsScheduleFileType)
            else self.table
        )
        return f"{hive_table}/{hive_str}/{self.filename}"


class ParseOutcomeMetadata(BaseModel):
    hash: str


class ParseOutcome(BaseModel):
    file: RawFetchedFile = Field(exclude={"contents"})
    metadata: ParseOutcomeMetadata
    success: bool
    exception: Optional[Exception] = None

    class Config:
        arbitrary_types_allowed = True
        json_encoders = {
            Exception: str,
            pendulum.DateTime: lambda ts: ts.to_iso8601_string(),
        }


class FeedTypeHourParseOutcomes(BaseModel):
    bucket: ClassVar[str] = PARSED_BUCKET
    partitions: ClassVar[List[str]] = ["dt"]
    feed_type: FeedType
    hour: pendulum.DateTime

    @property
    def table(self) -> str:
        return f"{self.feed_type.value}__parse_outcomes"

    @validator("hour")
    def convert_hour(cls, v) -> pendulum.DateTime:
        assert isinstance(v, datetime.datetime)
        return pendulum.instance(v).in_tz("UTC")

    @property
    def dt(self) -> pendulum.Date:
        return self.hour.date()

    @property
    def filename(self):
        return f"{self.hour.to_iso8601_string()}.jsonl"

    @property
    def gcs_key(self) -> str:
        hive_str = "/".join(
            [f"{key}={SERIALIZERS[type(getattr(self, key))](getattr(self, key))}" for key in self.partitions]
        )
        return f"{self.table}/{hive_str}/{self.filename}"


# https://github.com/pydantic/pydantic/discussions/2410
class FeedContents(BaseModel, abc.ABC):
    @property
    @abc.abstractmethod
    def feed_types(self) -> List[FeedType]:
        ...

    @property
    @abc.abstractmethod
    def records(self) -> Iterable[Dict]:
        raise NotImplementedError


class GtfsRealtime(FeedContents):
    feed_types: ClassVar[List[FeedType]] = [
        FeedType.gtfs_rt__vehicle_positions,
        FeedType.gtfs_rt__trip_updates,
        FeedType.gtfs_rt__service_alerts,
    ]
    header: Dict
    entity: List[Dict] = []

    @property
    def records(self) -> Iterable[Dict]:
        for entity in self.entity:
            yield dict(
                header=self.header,
                entity=entity,
            )


class ListOfDicts(FeedContents):
    feed_types: ClassVar[List[FeedType]] = [
        FeedType.gtfs_schedule,  # requires special handling
        FeedType.septa__train_view,
        FeedType.septa__alerts_without_message,
        FeedType.septa__alerts,
    ]
    __root__: List[Dict]

    @property
    def records(self) -> Iterable[Dict]:
        return self.__root__


class SeptaArrivals(FeedContents):
    feed_types: ClassVar[List[FeedType]] = [FeedType.septa__arrivals]
    __root__: Dict[str, List[Dict[str, List[Dict]]]]

    @validator("__root__")
    def direction_has_one_entry(cls, v):
        for key, directions in v.items():
            for direction_dict in directions:
                assert len(direction_dict) <= 1
        return v

    @property
    def records(self) -> Iterable[Dict]:
        for key, directions in self.__root__.items():
            for direction_dict in directions:
                for direction, updates in direction_dict.items():
                    for update in updates:
                        yield dict(
                            key=key,
                            direction_key=direction,
                            **update,
                        )


class SeptaTransitViewAll(FeedContents):
    feed_types: ClassVar[List[FeedType]] = [FeedType.septa__transit_view_all]
    routes: List[Dict[str, List[Dict]]]

    @property
    def records(self) -> Iterable[Dict]:
        records = []
        assert len(self.routes) == 1
        for route, vehicles in self.routes[0].items():
            records.extend(vehicles)
        return records


class SeptaBusDetours(FeedContents):
    feed_types: ClassVar[List[FeedType]] = [FeedType.septa__bus_detours]
    __root__: List[Dict[str, Union[str, List[Dict[str, Any]]]]]

    @property
    def records(self) -> Iterable[Dict]:
        for route in self.__root__:
            # could create a type for this
            for detour in route["route_info"]:
                assert isinstance(detour, dict)
                yield dict(
                    route_id=route["route_id"],
                    **detour,
                )


class SeptaElevatorOutages(FeedContents):
    feed_types: ClassVar[List[FeedType]] = [FeedType.septa__elevator_outages]
    meta: Dict[str, Any]
    results: List[Dict[str, str]]

    @property
    def records(self) -> Iterable[Dict]:
        for result in self.results:
            yield dict(
                meta=self.meta,
                **result,
            )


# this is type ignored because mypy does not understand that feed_types
# will be a ClassVar[List]
FEED_TYPES: Dict[FeedType, Type[FeedContents]] = {
    feed_type: kls for kls in FeedContents.__subclasses__() for feed_type in kls.feed_types  # type: ignore
}

missing_feed_types = [feed_type.value for feed_type in FeedType if feed_type not in FEED_TYPES]
assert not missing_feed_types, f"Missing parse configurations for {missing_feed_types}"

if __name__ == "__main__":
    with open("./feeds.yaml") as f:
        configs = parse_obj_as(List[FeedConfig], yaml.safe_load(f))
    for config in configs:
        if config.feed_type == FeedType.gtfs_schedule:
            response = requests.get(config.url)
            response.raise_for_status()
            raw = RawFetchedFileWithContents(
                ts=pendulum.now().replace(microsecond=0),
                config=config,
                page=[],
                response_code=response.status_code,
                response_headers=response.headers,
                contents=response.content,
            )
            client = storage.Client()
            typer.secho(f"Saving to {raw.bucket}/{raw.gcs_key}", fg=typer.colors.MAGENTA)
            client.bucket(raw.bucket.removeprefix("gs://")).blob(raw.gcs_key).upload_from_string(
                raw.json(), client=client
            )
