import os
from collections import namedtuple, defaultdict
from typing import Dict, List

import pendulum
from dagster import (
    asset,
    get_dagster_logger,
    AssetExecutionContext,
    HourlyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
)
from google.cloud import storage

from .common import FeedType, SERIALIZERS

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


@asset(
    partitions_def=MultiPartitionsDefinition(
        {
            "feed_type": StaticPartitionsDefinition(list(FeedType.__members__.keys())),
            "hour": HourlyPartitionsDefinition(start_date="2023-07-05-00:00"),
        }
    )
)
def raw_files_list(
    context: AssetExecutionContext,
) -> Dict[HourKey, List[storage.Blob]]:
    logger = get_dagster_logger()
    keys = context.partition_key.keys_by_dimension
    logger.info(f"handling {keys}")
    feed_type: str = keys["feed_type"]
    hour = keys["hour"]
    prefix = "/".join(
        [
            feed_type,
            f"dt={SERIALIZERS[pendulum.Date](pendulum.parse(hour).date())}",
            f"hour={SERIALIZERS[pendulum.Date](pendulum.parse(hour))}",
        ]
    )

    bucket = os.getenv("RAW_BUCKET")
    client = storage.Client()
    logger.info(f"Listing items in {bucket}/{prefix}...")
    blobs: List[storage.Blob] = list(client.list_blobs(bucket.removeprefix("gs://"), prefix=prefix))

    # remove client from blob
    for blob in blobs:
        blob.bucket._client = None

    aggs: Dict[HourKey, List[storage.Blob]] = defaultdict(list)

    for blob in blobs:
        aggs[hour_key(blob)].append(blob)

    logger.info(f"Found {len(blobs)=} grouped into {len(aggs)=}.")
    return aggs
    #
    # results = []
    # for item_id in topstory_ids:
    #     item = requests.get(
    #         f"https://hacker-news.firebaseio.com/v0/item/{item_id}.json"
    #     ).json()
    #     results.append(item)
    #
    #     if len(results) % 20 == 0:
    #         logger.info(f"Got {len(results)} items so far.")
    #
    # df = pd.DataFrame(results)
    #
    # context.add_output_metadata(
    #     metadata={
    #         "num_records": len(df),  # Metadata can be any key-value pair
    #         "preview": MetadataValue.md(df.head().to_markdown()),
    #         # The `MetadataValue` class has useful static methods to build Metadata
    #     }
    # )
