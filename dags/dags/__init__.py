from dagster import (
    AssetSelection,
    Definitions,
    define_asset_job,
    load_assets_from_modules,
    FilesystemIOManager,
)

from dagster_gcp import ConfigurablePickledObjectGCSIOManager, GCSResource

from . import assets

all_assets = load_assets_from_modules([assets])

# Define a job that will materialize the assets
parse_job = define_asset_job("parse_job", selection=AssetSelection.all())

# Addition: a ScheduleDefinition the job it should run and a cron schedule of how frequently to run it
# hackernews_schedule = ScheduleDefinition(
#     job=parse_job,
#     cron_schedule="0 * * * *",
# )

file_io_manager = FilesystemIOManager(base_dir="data")

gcs_io_manager = ConfigurablePickledObjectGCSIOManager(
    gcs=GCSResource(project="transit-data-analytics-demo"),
    gcs_bucket="",
)

defs = Definitions(
    assets=all_assets,
    # schedules=[hackernews_schedule],
    jobs=[parse_job],
    resources={
        "file_io_manager": file_io_manager,
        "gcs_io_manager": gcs_io_manager,
    },
)
