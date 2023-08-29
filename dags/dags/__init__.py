import os
from typing import Union, Any

import pendulum
from dagster import (
    AssetSelection,
    Definitions,
    define_asset_job,
    load_assets_from_modules,
    OutputContext,
    InputContext,
    build_schedule_from_partitioned_job,
)
from dagster._utils.backoff import backoff
from dagster_gcp import GCSResource, ConfigurablePickledObjectGCSIOManager  # type: ignore[import]
from dagster_gcp.gcs import PickledObjectGCSIOManager  # type: ignore[import]
from google.api_core.exceptions import Forbidden, ServiceUnavailable, TooManyRequests
from pydantic import BaseModel
from upath import UPath

from . import assets
from .common import SERIALIZERS


class HivePartitionedPydanticGCSIOManager(PickledObjectGCSIOManager):
    def get_path_for_partition(
        self, context: Union[InputContext, OutputContext], path: UPath, partition: str
    ) -> "UPath":
        """
        (Docs taken from parent class)

        Override this method if you want to use a different partitioning scheme
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
                f"{SERIALIZERS[pendulum.DateTime](parsed_hour)}.jsonl",
            ]
        )

    def load_from_path(self, context: InputContext, path: UPath) -> Any:
        raise NotImplementedError("HivePartitionedGCSIOManager cannot be used to load data")

    def dump_to_path(self, context: OutputContext, obj: Any, path: UPath) -> None:
        assert isinstance(obj, list)

        if self.path_exists(path):
            context.log.warning(f"Removing existing GCS key: {path}")
            self.unlink(path)

        if obj:
            assert isinstance(obj[0], BaseModel)

            jsonl_str = "\n".join(item.json() for item in obj)

            backoff(
                self.bucket_obj.blob(str(path)).upload_from_string,
                args=[jsonl_str],
                retry_on=(TooManyRequests, Forbidden, ServiceUnavailable),
            )


defs = Definitions(
    assets=load_assets_from_modules([assets]),
    schedules=[
        build_schedule_from_partitioned_job(
            define_asset_job("parse_job", selection=AssetSelection.all()),
        ),
    ],
    resources={
        "gcs_io_manager": ConfigurablePickledObjectGCSIOManager(
            gcs=GCSResource(project="transit-data-analytics-demo"),
            gcs_bucket=os.environ["PARSED_BUCKET"].removeprefix("gs://"),
            gcs_prefix="",
        ),
        "pydantic_gcs_io_manager": HivePartitionedPydanticGCSIOManager(
            client=GCSResource(project="transit-data-analytics-demo").get_client(),
            bucket=os.environ["PARSED_BUCKET"].removeprefix("gs://"),
            prefix="",  # no prefix; tables are the first partition right now
        ),
    },
)
