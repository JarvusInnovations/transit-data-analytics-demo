"""
Parses fetched data and groups hourly to reduce the number of files for external tables.
"""
import csv
import datetime
import gzip
import io
import json
import traceback
import zipfile
from collections import defaultdict, namedtuple
from concurrent.futures import ProcessPoolExecutor, as_completed
from io import BytesIO
from pprint import pformat
from typing import Optional, List, Annotated, DefaultDict, Iterable, Tuple, Union, Dict

import humanize
import pendulum
import typer
import yaml
from google.cloud import storage  # type: ignore
from google.protobuf.json_format import MessageToDict
from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2  # type: ignore
from pydantic import parse_obj_as, ValidationError
from tqdm import tqdm

from fetcher.common import (
    SERIALIZERS,
    HourAgg,
    ParsedRecord,
    RawFetchedFile,
    FeedType,
    FeedConfig,
    FEED_TYPES,
    GtfsRealtime,
    GtfsScheduleFileType,
    ListOfDicts,
)

HourKey = namedtuple("HourKey", ["hour", "base64url"])
app = typer.Typer()


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
    return HourKey(hour, base64url)


def file_to_records(
    file: RawFetchedFile,
) -> Iterable[Tuple[Union[FeedType, GtfsScheduleFileType], Iterable[Dict]]]:
    pydantic_type = FEED_TYPES[file.config.feed_type]
    try:
        if file.config.feed_type == FeedType.gtfs_schedule:
            with zipfile.ZipFile(BytesIO(file.contents)) as zipf:
                for zipf_file in zipf.namelist():
                    # https://stackoverflow.com/a/60918920
                    with io.TextIOWrapper(zipf.open(zipf_file), encoding="utf-8") as f:
                        reader = csv.DictReader(f)
                        # TODO: this will throw an error if attempting to parse a file we don't enumerate
                        #  we probably want to just throw a warning/generate an outcome rather than stop
                        #  further processing
                        # looking up the enum by value not name
                        yield GtfsScheduleFileType(zipf_file), parse_obj_as(ListOfDicts, list(reader)).records
        elif pydantic_type == GtfsRealtime:
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(file.contents)
            yield file.config.feed_type, GtfsRealtime(**MessageToDict(feed)).records
        else:
            yield file.config.feed_type, parse_obj_as(pydantic_type, json.loads(file.contents)).records
    except (ValidationError, DecodeError) as e:
        typer.secho(f"{type(e)} occurred on {file.bucket}/{file.gcs_key}", fg=typer.colors.RED)
        raise


def save_hour_agg(
    agg: HourAgg,
    records: List[ParsedRecord],
    pbar=None,
    client: Optional[storage.Client] = None,
    timeout: int = 60,
) -> int:
    # TODO: add asserts to check all same hour/url/etc.
    client = client or storage.Client()
    contents = gzip.compress(
        "\n".join([record.json(exclude={"file": {"contents"}}) for record in records]).encode("utf-8")
    )
    content_size = humanize.naturalsize(len(contents))
    agg_path = f"{agg.bucket}/{agg.gcs_key}"

    if contents:
        msg = f"Saving {len(records)} records ({content_size}) to {agg_path}"
        if pbar:
            pbar.write(msg)
        else:
            typer.secho(msg)
        start = pendulum.now()
        blob = client.bucket(agg.bucket.removeprefix("gs://")).blob(agg.gcs_key)
        blob.upload_from_string(contents, timeout=timeout, client=client)
        start.diff()
        msg = f"Took {humanize.naturaldelta(start.diff().total_seconds())} to save {content_size} to {agg_path}"
        if pbar:
            pbar.write(msg)
        else:
            typer.secho(msg)
    else:
        msg = f"WARNING: no records found for aggregation {agg}"
        if pbar:
            pbar.write(msg)
        else:
            typer.secho(msg, fg=typer.colors.YELLOW)
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
) -> int:
    def write(*args, fg=None, **kwargs):
        if pbar:
            pbar.write(*args, **kwargs)
        else:
            typer.secho(*args, **kwargs, fg=fg)

    write(f"Handling {len(blobs)=} for {key}")
    client = storage.Client()
    aggs: DefaultDict[Union[FeedType, GtfsScheduleFileType], List[ParsedRecord]] = defaultdict(list)

    # we could do this streaming, but data should be small enough
    for blob in blobs:
        file = parse_blob(blob=blob, client=client)
        for feed_type, parsed_records in file_to_records(file):
            if parsed_records:
                aggs[feed_type].extend(
                    [
                        ParsedRecord(file=file, line_number=idx, record=record)
                        for idx, record in enumerate(parsed_records)
                    ]
                )
            else:
                write(
                    f"WARNING: no records found for {feed_type} {blob.self_link}",
                    fg=typer.colors.YELLOW,
                )

    written = 0
    for feed_type, records in aggs.items():
        written += save_hour_agg(
            agg=HourAgg(
                table=feed_type,
                **key._asdict(),
            ),
            records=records,
        )
    return written


@app.command()
def file(uri: str):
    client = storage.Client()
    file = parse_blob(blob=storage.Blob.from_string(uri, client=client), client=client)
    typer.secho(f"Found {sum(len(list(records)) for _, records in file_to_records(file))} records in {file.gcs_key}")


@app.command()
def day(
    dt: Annotated[
        datetime.datetime,
        typer.Argument(
            formats=["%Y-%m-%d"],
        ),
    ] = datetime.date.today(),  # type: ignore[assignment]
    feed_type: Annotated[Optional[List[FeedType]], typer.Option()] = None,
    exclude: Annotated[Optional[List[FeedType]], typer.Option()] = None,
    bucket: str = RawFetchedFile.bucket,
    base64url: Optional[str] = None,
    workers: int = 8,
    timeout: int = 60,
):
    """
    Parse a collect on of raw data files and save in hourly-partitioned JSONL files named by base64-encoded URL.

    E.g. gs://test-jarvus-transit-data-demo-parsed/gtfs_rt__vehicle_positions/dt=2023-07-07/hour=2023-07-07T01:00:00+00:00/aHR0cHM6Ly90cnVldGltZS5wb3J0YXV0aG9yaXR5Lm9yZy9ndGZzcnQtdHJhaW4vdmVoaWNsZXM=.jsonl
    """
    if feed_type and exclude:
        raise ValueError("cannot specify both table and exclude")

    client = storage.Client()

    if feed_type:
        feed_types = feed_type
    else:
        with open("./feeds.yaml") as f:
            configs = parse_obj_as(List[FeedConfig], yaml.safe_load(f))
        feed_types_set = set(config.feed_type for config in configs)
        if exclude:
            feed_types_set = feed_types_set - set(exclude)
        feed_types = list(feed_types_set)

    errors = []
    for ft in feed_types:
        prefix = f"{ft.value}/dt={SERIALIZERS[pendulum.Date](pendulum.instance(dt).date())}/"
        typer.secho(f"Listing items in {bucket}/{prefix}...", fg=typer.colors.MAGENTA)
        blobs: List[storage.Blob] = list(client.list_blobs(bucket.removeprefix("gs://"), prefix=prefix))

        # remove client from blob
        for blob in blobs:
            blob.bucket._client = None

        aggs = defaultdict(list)

        for blob in blobs:
            blob_key = hour_key(blob)
            if not base64url or base64url == blob_key.base64url:
                aggs[blob_key].append(blob)

        typer.secho(f"Found {len(blobs)=} grouped into {len(aggs)=}.", fg=typer.colors.MAGENTA)

        pbar = tqdm(total=len(aggs), leave=False, desc=ft)
        with ProcessPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(
                    handle_hour,
                    key=key,
                    blobs=blobs,
                    timeout=timeout,
                ): key
                for key, blobs in aggs.items()
            }

            for future in as_completed(futures):
                key = futures[future]
                try:
                    pbar.update(1)
                    future.result()
                except Exception as e:
                    typer.secho(
                        f"Exception returned for {key}: {traceback.format_exc()}",
                        fg=typer.colors.RED,
                    )
                    errors.append(e)

    if errors:
        raise RuntimeError(pformat(errors))


if __name__ == "__main__":
    app()
