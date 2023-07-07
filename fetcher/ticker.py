import time
from functools import cache
from typing import List, Tuple

import humanize
import pendulum
import schedule
import typer
import yaml
from prometheus_client import start_http_server
from pydantic import parse_obj_as

from fetcher.common import KeyValue, FeedConfig, FeedType
from fetcher.tasks import fetch_feed


def configs_to_urls(configs: List[FeedConfig]) -> List[Tuple[FeedConfig, List[KeyValue]]]:
    fetches = []
    for config in configs:
        if config.pages:
            # TODO: support more of these; cross-product?
            assert len(config.pages) == 1
            page = config.pages[0]
            for value in page.values:
                fetches.append((config, [KeyValue(key=page.key, value=value)]))
        else:
            fetches.append((config, []))
    return fetches


@cache
def get_configs() -> List[FeedConfig]:
    with open("./feeds.yaml") as f:
        configs = parse_obj_as(List[FeedConfig], yaml.safe_load(f))
    # TODO: can we check that keys will be unique?
    return configs


def tick(seconds: int, dry: bool, feed_types: List[FeedType]):
    ts: pendulum.DateTime = pendulum.now(tz=pendulum.UTC).replace(second=seconds, microsecond=0)
    typer.secho(f"Ticking {ts.to_iso8601_string()} for {feed_types=}")
    configs = [config for config in get_configs() if config.feed_type in feed_types]
    fetches = configs_to_urls(configs)

    for config, page in fetches:
        fetch_feed(tick=ts, config=config, page=page, dry=dry)
    print(f"Took {humanize.naturaltime(pendulum.now() - ts)} to enqueue {len(fetches)} fetches.")


def main(dry: bool = False):
    start_http_server(8000)

    typer.secho(f"Found {len(get_configs())=} feed configs.", fg=typer.colors.MAGENTA)

    schedule.every().minute.at(":00").do(
        tick,
        seconds=0,
        dry=dry,
        feed_types=[
            FeedType.gtfs_vehicle_positions,
            FeedType.gtfs_trip_updates,
            FeedType.gtfs_service_alerts,
            FeedType.septa__arrivals,
            FeedType.septa__train_view,
            FeedType.septa__transit_view_all,
            FeedType.septa__bus_detours,
            FeedType.septa__alerts_without_message,
            FeedType.septa__alerts,
            FeedType.septa__elevator_outages,
        ],
    )
    schedule.every().day.at("00:00").do(
        tick,
        seconds=0,
        dry=dry,
        feed_types=[
            FeedType.gtfs_schedule,
        ],
    )

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    typer.run(main)
