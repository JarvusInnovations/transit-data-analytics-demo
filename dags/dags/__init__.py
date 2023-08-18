import os
import pickle
from typing import Union, Any

import pendulum
from dagster import (
    AssetSelection,
    Definitions,
    define_asset_job,
    load_assets_from_modules,
    OutputContext,
    InputContext,
)
from dagster._utils import PICKLE_PROTOCOL
from dagster._utils.backoff import backoff
from dagster_gcp import GCSResource  # type: ignore[import]
from dagster_gcp.gcs import PickledObjectGCSIOManager  # type: ignore[import]
from google.api_core.exceptions import Forbidden, ServiceUnavailable, TooManyRequests
from upath import UPath

from . import assets
from .common import SERIALIZERS


# Addition: a ScheduleDefinition the job it should run and a cron schedule of how frequently to run it
# hackernews_schedule = ScheduleDefinition(
#     job=parse_job,
#     cron_schedule="0 * * * *",
# )


class HivePartitionedGCSIOManager(PickledObjectGCSIOManager):
    def get_path_for_partition(
        self, context: Union[InputContext, OutputContext], path: UPath, partition: str
    ) -> "UPath":
        """Override this method if you want to use a different partitioning scheme
        (for example, if the saving function handles partitioning instead).
        The extension will be added later.

        Args:
            context (Union[InputContext, OutputContext]): The context for the I/O operation.
            path (UPath): The path to the file or object.
            partition (str): Formatted partition/multipartition key

        Returns:
            UPath: The path to the file with the partition key appended.
        """
        feed_type, hour = partition.split("/")
        parsed_hour = pendulum.from_format(hour, "YYYY-MM-DD-HH:mm")
        return path / "/".join(
            [
                f"feed_type={feed_type}",
                f"dt={SERIALIZERS[pendulum.Date](parsed_hour.date())}",
                f"{SERIALIZERS[pendulum.DateTime](parsed_hour)}",
            ]
        )

    def load_from_path(self, context: InputContext, path: UPath) -> Any:
        bytes_obj = self.bucket_obj.blob(str(path)).download_as_bytes()
        return pickle.loads(bytes_obj)

    def dump_to_path(self, context: OutputContext, obj: Any, path: UPath) -> None:
        if self.path_exists(path):
            context.log.warning(f"Removing existing GCS key: {path}")
            self.unlink(path)

        pickled_obj = pickle.dumps(obj, PICKLE_PROTOCOL)

        backoff(
            self.bucket_obj.blob(str(path)).upload_from_string,
            args=[pickled_obj],
            retry_on=(TooManyRequests, Forbidden, ServiceUnavailable),
        )


defs = Definitions(
    assets=load_assets_from_modules([assets]),
    # schedules=[hackernews_schedule],
    jobs=[
        define_asset_job("parse_job", selection=AssetSelection.all()),
    ],
    resources={
        "gcs_io_manager": HivePartitionedGCSIOManager(
            client=GCSResource(project="transit-data-analytics-demo").get_client(),
            bucket=os.environ["PARSED_BUCKET"].removeprefix("gs://"),
            prefix="",  # no prefix; tables are the first partition right now
        ),
    },
)
