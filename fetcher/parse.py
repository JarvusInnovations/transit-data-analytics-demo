"""
Parses fetched data and groups hourly to reduce the number of files for external tables.
"""
import datetime
import gzip
import json
import traceback
from collections import defaultdict, namedtuple
from concurrent.futures import ProcessPoolExecutor, as_completed
from io import BytesIO
from typing import Optional, List, Annotated

import humanize
import pendulum
import typer
import yaml
from google.cloud import storage  # type: ignore
from google.protobuf.json_format import MessageToDict
from google.transit import gtfs_realtime_pb2  # type: ignore
from pydantic import parse_obj_as, ValidationError
from tqdm import tqdm

from fetcher.common import (
    SERIALIZERS,
    HourAgg,
    FetchedRecord,
    RawFetchedFile,
    FeedType,
    FeedConfig,
    FEED_TYPES,
    GtfsRealtime,
    FeedContents,
)

HourKey = namedtuple("HourKey", ["hour", "base64url"])


def hour_key(blob: storage.Blob) -> HourKey:
    feed_type, dtequals, hourequals, tsequals, base64urlequals, filename = blob.name.split("/")
    _, hour = hourequals.split("=")
    _, base64url = base64urlequals.split("=", maxsplit=1)
    return HourKey(hour, base64url)


# TODO: handle protos
def handle_hour(
    key: HourKey,
    blobs: List[storage.Blob],
    pbar: Optional[tqdm] = None,
    timeout: int = 60,
) -> int:
    if pbar:
        pbar.write(f"Handling {len(blobs)=} for {key}")
    client = storage.Client()
    first_file = None
    records: List[FetchedRecord] = []

    # we could do this streaming, but data should be small enough
    for blob in blobs:
        bio = BytesIO()
        client.download_blob_to_file(blob, file_obj=bio)
        bio.seek(0)
        file = RawFetchedFile(**json.load(bio))

        if not first_file:
            first_file = file

        pydantic_type = FEED_TYPES[file.config.feed_type]
        parsed_response: FeedContents
        try:
            if pydantic_type == GtfsRealtime:
                feed = gtfs_realtime_pb2.FeedMessage()
                feed.ParseFromString(file.contents)
                parsed_response = GtfsRealtime(**MessageToDict(feed))
            else:
                parsed_response = parse_obj_as(pydantic_type, json.loads(file.contents))
        except ValidationError:
            msg = f"Validation error occurred on {blob.path}"
            if pbar:
                pbar.write(msg)
            else:
                typer.secho(msg, fg=typer.colors.RED)
            raise
        if parsed_response.records:
            records.extend([FetchedRecord(file=file, record=record) for record in parsed_response.records])
        else:
            msg = f"WARNING: no records found for {blob.path}"
            if pbar:
                pbar.write(msg)
            else:
                typer.secho(msg, fg=typer.colors.YELLOW)

    agg = HourAgg(first_file=first_file)
    # TODO: add asserts to check all same hour/url/etc.

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
        msg = f"WARNING: no records found for {key}"
        if pbar:
            pbar.write(msg)
        else:
            typer.secho(msg, fg=typer.colors.YELLOW)
    return len(contents)


def main(
    dt: Annotated[
        datetime.datetime,
        typer.Argument(
            formats=["%Y-%m-%d"],
        ),
    ] = datetime.date.today(),
    table: Annotated[Optional[List[FeedType]], typer.Option()] = None,
    exclude: Annotated[Optional[List[FeedType]], typer.Option()] = None,
    bucket: str = RawFetchedFile.bucket,
    base64url: Optional[str] = None,
    workers: int = 8,
    timeout: int = 60,
):
    """
    Parse a collect on of raw data files and save in hourly-partitioned JSONL files named by base64-encoded URL.

    E.g. gs://test-jarvus-transit-data-demo-parsed/gtfs_vehicle_positions/dt=2023-07-07/hour=2023-07-07T01:00:00+00:00/aHR0cHM6Ly90cnVldGltZS5wb3J0YXV0aG9yaXR5Lm9yZy9ndGZzcnQtdHJhaW4vdmVoaWNsZXM=.jsonl
    """
    if table and exclude:
        raise ValueError("cannot specify both table and exclude")

    client = storage.Client()

    if table:
        tables = table
    else:
        with open("./feeds.yaml") as f:
            configs = parse_obj_as(List[FeedConfig], yaml.safe_load(f))
        tables = list(set(config.feed_type for config in configs) - set(exclude))

    for tbl in tables:
        prefix = f"{tbl.value}/dt={SERIALIZERS[pendulum.Date](pendulum.instance(dt).date())}/"
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

        pbar = tqdm(total=len(aggs), leave=False, desc=tbl)
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
                except Exception:
                    typer.secho(f"Exception returned for {key}: {traceback.format_exc()}", fg=typer.colors.RED)
                    raise


if __name__ == "__main__":
    typer.run(main)
