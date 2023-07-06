"""
Parses fetched data and groups hourly to reduce the number of files for external tables.
"""
import datetime
import json
from collections import defaultdict, namedtuple
from io import BytesIO
from typing import Optional, List, Annotated

import humanize
import pendulum
import typer
import yaml
from google.cloud import storage
from pydantic import parse_obj_as
from tqdm import tqdm

from fetcher.common import (
    SERIALIZERS,
    RAW_BUCKET,
    HourAgg,
    FetchedRecord,
    RawFetchedFile,
    FeedType,
    FeedConfig,
    FEED_TYPES,
)

HourKey = namedtuple("HourKey", ["hour", "base64url"])


def hour_key(blob: storage.Blob) -> HourKey:
    feed_type, dtequals, hourequals, tsequals, base64urlequals, filename = blob.name.split("/")
    _, hour = hourequals.split("=")
    _, base64url = base64urlequals.split("=", maxsplit=1)
    return HourKey(hour, base64url)


# TODO: handle protos
def handle_hour(key: HourKey, blobs: List[storage.Blob], pbar: Optional[tqdm] = None):
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
        parsed_response = parse_obj_as(pydantic_type, json.loads(file.contents))
        records.extend([FetchedRecord(file=file, record=record) for record in parsed_response.records])

    agg = HourAgg(first_file=first_file)
    # TODO: add asserts to check all same hour/url/etc.

    contents = "\n".join([record.json(exclude={"file": {"contents"}}) for record in records])

    msg = f"Saving {len(records)} records ({humanize.naturalsize(len(contents))}) to {agg.bucket}/{agg.gcs_key}"
    if pbar:
        pbar.write(msg)
    else:
        typer.secho(msg)
    client.bucket(agg.bucket.removeprefix("gs://")).blob(agg.gcs_key).upload_from_string(contents, client=client)


def main(
    dt: datetime.datetime,
    table: Annotated[Optional[List[FeedType]], typer.Option()] = None,
    bucket: Optional[str] = RAW_BUCKET,
):
    client = storage.Client()

    if table:
        tables = table
    else:
        with open("./feeds.yaml") as f:
            configs = parse_obj_as(List[FeedConfig], yaml.safe_load(f))
        tables = set(config.feed_type for config in configs)

    itr = tqdm(tables)
    for tbl in itr:
        itr.set_description(tbl.value)
        prefix = f"{tbl.value}/dt={SERIALIZERS[pendulum.Date](pendulum.instance(dt).date())}/"

        itr.write(f"Listing items in {bucket}/{prefix}...")
        blobs: List[storage.Blob] = list(client.list_blobs(bucket.removeprefix("gs://"), prefix=prefix))
        itr.write(f"Found {len(blobs)=}.")

        aggs = defaultdict(list)

        for blob in blobs:
            aggs[hour_key(blob)].append(blob)

        subitr = tqdm(aggs.items())
        for key, blobs in subitr:
            subitr.set_description(str(key))
            subitr.write(f"Handling {len(blobs)=} for {key}")
            handle_hour(key, blobs, pbar=subitr)


if __name__ == "__main__":
    typer.run(main)
