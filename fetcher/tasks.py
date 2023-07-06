import os
from typing import List

import humanize
import pendulum
import requests
import typer
from google.cloud import storage  # type: ignore
from huey import RedisHuey  # type: ignore

from fetcher.common import FeedConfig, KeyValue, RawFetchedFile
from fetcher.metrics import (
    HUEY_TASK_SIGNALS,
    FETCH_REQUEST_DELAY_SECONDS,
    FETCH_REQUEST_DURATION_SECONDS,
    FETCH_SAVE_DURATION_SECONDS,
)

huey = RedisHuey(
    host=os.environ["HUEY_REDIS_HOST"],
)

client = storage.Client()


@huey.on_startup()
def on_startup():
    pass


@huey.signal()
def all_signal_handler(signal, task, exc=None):
    HUEY_TASK_SIGNALS.labels(
        signal=signal,
        exc_type=type(exc).__name__,
        **task.kwargs["config"].labels,
    ).inc()


@huey.task(
    expires=int(os.getenv("HUEY_FETCH_CONFIG_EXPIRES", 5)),
)
def fetch_feed(tick: pendulum.DateTime, config: FeedConfig, page: List[KeyValue] = [], dry: bool = False):
    FETCH_REQUEST_DELAY_SECONDS.labels(**config.labels).observe((pendulum.now() - tick).total_seconds())

    with FETCH_REQUEST_DURATION_SECONDS.labels(**config.labels).time():
        # TODO: the FeedConfig should manage the URL creation generically
        response = requests.get(
            config.url,
            params={
                **{kv.key: kv.value for kv in config.query},
                **{kv.key: kv.value for kv in page},
            },
        )
    response.raise_for_status()

    raw = RawFetchedFile(
        ts=tick,
        config=config,
        page=page,
        response_code=response.status_code,
        response_headers=response.headers,
        contents=response.content,
    )

    msg = f"Saved {humanize.naturalsize(len(raw.contents))} to {raw.bucket}/{raw.gcs_key}"
    if dry:
        typer.secho(f"DRY RUN: {msg}")
    else:
        with FETCH_SAVE_DURATION_SECONDS.labels(**config.labels).time():
            client.bucket(raw.bucket.removeprefix("gs://")).blob(raw.gcs_key).upload_from_string(
                raw.json(), client=client
            )
        typer.secho(msg)
