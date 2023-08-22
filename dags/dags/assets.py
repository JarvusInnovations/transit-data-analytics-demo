import csv
import gzip
import hashlib
import io
import json
import os
import zipfile
from collections import defaultdict, namedtuple
from io import BytesIO
from typing import Optional, List, DefaultDict, Iterable, Union, Dict

import humanize
import pendulum
from dagster import (
    asset,
    get_dagster_logger,
    AssetExecutionContext,
    HourlyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    AssetIn,
)
from google.cloud import storage  # type: ignore
from google.protobuf.json_format import MessageToDict
from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2  # type: ignore
from pydantic import parse_obj_as, ValidationError, BaseModel
from tqdm import tqdm

from .common import (
    SERIALIZERS,
    HourAgg,
    ParsedRecord,
    RawFetchedFile,
    FeedType,
    FEED_TYPES,
    GtfsRealtime,
    GtfsScheduleFileType,
    ListOfDicts,
    ParsedRecordMetadata,
    ParseOutcome,
    ParseOutcomeMetadata,
)

HourKey = namedtuple("HourKey", ["feed_type", "hour", "base64url"])


def hour_key(blob: storage.Blob) -> HourKey:
    (
        feed_type,
        dtequals,
        hourequals,
        tsequals,
        base64urlequals,
        filename,
    ) = blob.name.split("/")
    _, hour = hourequals.split("=")
    _, base64url = base64urlequals.split("=", maxsplit=1)
    return HourKey(feed_type, hour, base64url)


class ParsedFile(BaseModel):
    feed_type: Union[FeedType, GtfsScheduleFileType]
    hash: bytes
    records: Iterable[Dict]


def file_to_records(
    file: RawFetchedFile,
) -> Iterable[ParsedFile]:
    logger = get_dagster_logger()
    pydantic_type = FEED_TYPES[file.config.feed_type]
    try:
        if file.config.feed_type == FeedType.gtfs_schedule:
            with zipfile.ZipFile(BytesIO(file.contents)) as zipf:
                for zipf_file in zipf.namelist():
                    with zipf.open(zipf_file) as f:
                        contents = f.read()
                    reader = csv.DictReader(io.TextIOWrapper(io.BytesIO(contents), encoding="utf-8"))
                    # TODO: this will throw an error if attempting to parse a file we don't enumerate
                    #  we probably want to just throw a warning/generate an outcome rather than stop
                    #  further processing
                    # looking up the enum by value not name
                    yield ParsedFile(
                        feed_type=GtfsScheduleFileType(zipf_file),
                        hash=hashlib.md5(contents).digest(),
                        records=parse_obj_as(ListOfDicts, list(reader)).records,
                    )
        elif pydantic_type == GtfsRealtime:
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(file.contents)
            yield ParsedFile(
                feed_type=file.config.feed_type,
                hash=hashlib.md5(file.contents).digest(),
                records=GtfsRealtime(**MessageToDict(feed)).records,
            )
        else:
            yield ParsedFile(
                feed_type=file.config.feed_type,
                hash=hashlib.md5(file.contents).digest(),
                records=parse_obj_as(pydantic_type, json.loads(file.contents)).records,
            )
    except (ValidationError, DecodeError) as e:
        logger.error(f"{type(e)} occurred on {file.bucket}/{file.gcs_key}")
        raise


def save_hour_agg(
    agg: HourAgg,
    records: List[ParsedRecord],
    pbar=None,
    client: Optional[storage.Client] = None,
    timeout: int = 60,
) -> int:
    logger = get_dagster_logger()
    # TODO: add asserts to check all same hour/url/etc.
    client = client or storage.Client()
    contents = gzip.compress(
        "\n".join([record.json(exclude={"file": {"contents"}}) for record in records]).encode("utf-8")
    )
    content_size = humanize.naturalsize(len(contents))
    agg_path = f"{agg.bucket}/{agg.gcs_key}"

    if contents:
        logger.info(f"Saving {len(records)} records ({content_size}) to {agg_path}")
        start = pendulum.now()
        blob = client.bucket(agg.bucket.removeprefix("gs://")).blob(agg.gcs_key)
        blob.upload_from_string(contents, timeout=timeout, client=client)
        logger.info(f"Took {humanize.naturaldelta(start.diff().total_seconds())} to save {content_size} to {agg_path}")
    else:
        logger.warning(f"WARNING: no records found for aggregation {agg}")
    return len(contents)


# mostly exists so we can call directly to debug
def parse_blob(blob: storage.Blob, client: storage.Client) -> RawFetchedFile:
    bio = BytesIO()
    client.download_blob_to_file(blob, file_obj=bio)
    bio.seek(0)
    return RawFetchedFile(**json.load(bio))


def handle_hour(
    key: HourKey,
    blobs: List[storage.Blob],
    pbar: Optional[tqdm] = None,
    timeout: int = 60,
) -> List[ParseOutcome]:
    logger = get_dagster_logger()
    logger.info(f"Handling {len(blobs)=} for {key}")
    client = storage.Client()
    outcomes = []
    aggs: DefaultDict[Union[FeedType, GtfsScheduleFileType], List[ParsedRecord]] = defaultdict(list)

    # we could do this streaming, but data should be small enough
    for blob in blobs:
        blob_hash = hashlib.md5()
        file = parse_blob(blob=blob, client=client)
        for parsed_file in file_to_records(file):
            if parsed_file.records:
                blob_hash.update(parsed_file.hash)
                aggs[parsed_file.feed_type].extend(
                    [
                        ParsedRecord(
                            file=file,
                            record=record,
                            metadata=ParsedRecordMetadata(
                                line_number=idx,
                            ),
                        )
                        for idx, record in enumerate(parsed_file.records)
                    ]
                )
            else:
                logger.warning(f"WARNING: no records found for {parsed_file.feed_type} {blob.self_link}")
        outcomes.append(
            ParseOutcome(
                file=file,
                metadata=ParseOutcomeMetadata(
                    hash=blob_hash.hexdigest(),
                ),
                success=True,
            )
        )

    for feed_type, records in aggs.items():
        save_hour_agg(
            agg=HourAgg(
                table=feed_type,
                **key._asdict(),
            ),
            records=records,
        )

    return outcomes


@asset(
    partitions_def=MultiPartitionsDefinition(
        {
            "feed_type": StaticPartitionsDefinition(list(FeedType.__members__.keys())),
            "hour": HourlyPartitionsDefinition(start_date="2023-07-05-00:00"),
        }
    ),
    io_manager_key="gcs_io_manager",
)
def raw_files_list(
    context: AssetExecutionContext,
) -> Dict[str, List[storage.Blob]]:
    logger = get_dagster_logger()
    keys: Dict = context.partition_key.keys_by_dimension  # type: ignore[attr-defined]
    logger.info(f"handling {keys}")
    feed_type: str = keys["feed_type"]
    hour = pendulum.from_format(keys["hour"], "YYYY-MM-DD-HH:mm")

    prefix = "/".join(
        [
            feed_type,
            f"dt={SERIALIZERS[pendulum.Date](hour.date())}",
            f"hour={SERIALIZERS[pendulum.DateTime](hour)}",
        ]
    )

    bucket = os.environ["RAW_BUCKET"]
    client = storage.Client()
    logger.info(f"Listing items in {bucket}/{prefix}...")
    blobs: List[storage.Blob] = list(client.list_blobs(bucket.removeprefix("gs://"), prefix=prefix))

    # remove client from blob
    for blob in blobs:
        blob.bucket._client = None

    aggs: Dict[str, List[storage.Blob]] = defaultdict(list)

    for blob in blobs:
        aggs[hour_key(blob).base64url].append(blob)

    logger.info(f"Found {len(blobs)=} grouped into {len(aggs)=}.")
    context.add_output_metadata(
        metadata={
            "num_aggs": len(aggs),
            "num_blobs": len(blobs),
        }
    )
    return aggs


@asset(
    partitions_def=MultiPartitionsDefinition(
        {
            "feed_type": StaticPartitionsDefinition(list(FeedType.__members__.keys())),
            "hour": HourlyPartitionsDefinition(start_date="2023-07-05-00:00"),
        }
    ),
    ins={
        "raw_files_list": AssetIn(input_manager_key="gcs_io_manager"),
    },
    io_manager_key="pydantic_gcs_io_manager",
)
def parsed_and_grouped_files(
    context: AssetExecutionContext,
    raw_files_list: Dict[str, List[storage.Blob]],
) -> List[ParseOutcome]:
    logger = get_dagster_logger()
    keys: Dict = context.partition_key.keys_by_dimension  # type: ignore[attr-defined]
    logger.info(f"handling {keys}")
    feed_type: str = keys["feed_type"]
    hour = pendulum.from_format(keys["hour"], "YYYY-MM-DD-HH:mm")

    url_to_outcomes: Dict[str, List[ParseOutcome]] = defaultdict(list)
    for base64url, blobs in raw_files_list.items():
        outcomes = handle_hour(
            key=HourKey(
                feed_type=feed_type,
                hour=hour,
                base64url=base64url,
            ),
            blobs=blobs,
        )
        url_to_outcomes[base64url].extend(outcomes)

    return [outcome for outcomes in url_to_outcomes.values() for outcome in outcomes]
