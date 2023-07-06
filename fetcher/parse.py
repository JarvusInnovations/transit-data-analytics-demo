"""
Parses fetched data and groups hourly to reduce the number of files for external tables.
"""
import datetime
import json
from collections import defaultdict, namedtuple
from io import BytesIO
from typing import Optional, List

import humanize
import pendulum
import typer
from google.cloud import storage
from tqdm import tqdm

from fetcher.common import SERIALIZERS, RAW_BUCKET, HourAgg, FetchedRecord, RawFetchedFile, FeedType
from fetcher.feed_types import SeptaTransitViewAll

HourKey = namedtuple("HourKey", ["hour", "base64url"])


def hour_key(blob: storage.Blob) -> HourKey:
    _, dtequals, hourequals, tsequals, base64urlequals, filename = blob.name.split("/")
    _, hour = hourequals.split("=")
    _, base64url = base64urlequals.split("=")
    return HourKey(hour, base64url)


# TODO: handle protos
def handle_hour(key: HourKey, blobs: List[storage.Blob]):
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

        match file.config.feed_type.value:
            case FeedType.septa__transit_view_all:
                resp = SeptaTransitViewAll(**json.loads(file.contents))
                assert len(resp.routes) == 1
                for route, vehicles in resp.routes[0].items():
                    records.extend([FetchedRecord(file=file, record=vehicle) for vehicle in vehicles])
            case _:
                raise NotImplementedError

    agg = HourAgg(first_file=first_file)
    # TODO: add asserts to check all same hour/url/etc.

    contents = "\n".join([record.json(exclude={"file": {"contents"}}) for record in records])

    typer.secho(f"Saving {humanize.naturalsize(len(contents))} to {agg.bucket}/{agg.gcs_key}")
    # client.bucket(agg.bucket.removeprefix("gs://")).blob(agg.gcs_key).upload_from_string(contents, client=client)


def main(
    table: str,
    dt: datetime.datetime,
    bucket: Optional[str] = RAW_BUCKET,
):
    client = storage.Client()

    prefix = f"{table}/dt={SERIALIZERS[pendulum.Date](pendulum.instance(dt).date())}/"

    typer.secho(f"Listing items in {bucket}/{prefix}...", fg=typer.colors.MAGENTA)
    blobs: List[storage.Blob] = list(client.list_blobs(bucket.removeprefix("gs://"), prefix=prefix))
    typer.secho(f"Found {len(blobs)=}.")

    aggs = defaultdict(list)

    for blob in blobs:
        aggs[hour_key(blob)].append(blob)

    for key, blobs in tqdm(aggs.items()):
        typer.secho(f"Handling {len(blobs)=} for {key}")
        handle_hour(key, blobs)


if __name__ == "__main__":
    typer.run(main)
