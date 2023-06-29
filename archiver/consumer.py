"""
Mostly taken from https://github.com/coleifer/huey/blob/master/huey/bin/huey_consumer.py
but we want to be able to run a prometheus server.
"""
import logging
import os
import sys

import typer
from huey.constants import WORKER_THREAD  # type: ignore
from huey.consumer_options import ConsumerConfig  # type: ignore
from prometheus_client import start_http_server

from archiver.tasks import huey


def main():
    start_http_server(8000)

    # use the huey-defined defaults if not provided in env
    config = ConsumerConfig(
        workers=int(os.getenv("HUEY_WORKERS", 1)),
        worker_type=os.getenv("HUEY_WORKER_TYPE", WORKER_THREAD),
        backoff=float(os.getenv("HUEY_BACKOFF", 1.15)),
        max_delay=float(os.getenv("HUEY_MAX_DELAY", 10)),
        periodic=False,
    )

    config.validate()
    config.setup_logger(logging.getLogger("huey"))

    huey.create_consumer(**config.values).run()


if __name__ == "__main__":
    # straight from huey_consumer.py
    if sys.version_info >= (3, 8) and sys.platform == "darwin":
        import multiprocessing

        try:
            multiprocessing.set_start_method("fork")
        except RuntimeError:
            pass

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    typer.run(main)
