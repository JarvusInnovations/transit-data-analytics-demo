import time
from functools import cache
from typing import List

import humanize
import pendulum
import schedule
import typer
import yaml
from prometheus_client import start_http_server
from pydantic import parse_obj_as

from archiver.tasks import FetchConfig, fetch_config


@cache
def get_configs() -> List[FetchConfig]:
    with open("./feeds.yaml") as f:
        return parse_obj_as(List[FetchConfig], yaml.safe_load(f))


def tick(seconds, dry):
    ts: pendulum.DateTime = pendulum.now(tz=pendulum.UTC).replace(second=seconds, microsecond=0)
    print(f"Ticking {ts.to_iso8601_string()}!", flush=True)
    configs = get_configs()

    start = pendulum.now()
    for config in configs:
        fetch_config(tick=ts, config=config, dry=dry)
    print(f"Took {humanize.naturaltime(pendulum.now() - start)} to enqueue {len(configs)} fetches.")


def main(dry: bool = False):
    start_http_server(8000)

    schedule.every().minute.at(":00").do(tick, seconds=0, dry=dry)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    typer.run(main)
