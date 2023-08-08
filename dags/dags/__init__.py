import os

from dagster import (
    AssetSelection,
    Definitions,
    define_asset_job,
    load_assets_from_modules,
)

from dagster_gcp import ConfigurablePickledObjectGCSIOManager, GCSResource

from . import assets


# Addition: a ScheduleDefinition the job it should run and a cron schedule of how frequently to run it
# hackernews_schedule = ScheduleDefinition(
#     job=parse_job,
#     cron_schedule="0 * * * *",
# )


class HivePartitionedGCSIOManager(ConfigurablePickledObjectGCSIOManager):
    ...


defs = Definitions(
    assets=load_assets_from_modules([assets]),
    # schedules=[hackernews_schedule],
    jobs=[
        define_asset_job("parse_job", selection=AssetSelection.all()),
    ],
    resources={
        "gcs_io_manager": HivePartitionedGCSIOManager(
            gcs=GCSResource(project="transit-data-analytics-demo"),
            gcs_bucket=os.getenv("PARSED_BUCKET").removeprefix("gs://"),
            gcs_prefix="",  # no prefix; tables are the first partition right now
        ),
    },
)
