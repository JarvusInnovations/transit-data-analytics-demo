"""
Mostly taken from https://github.com/coleifer/huey/blob/master/huey/bin/huey_consumer.py
but we want to be able to run a prometheus server.
"""
import logging
import os

import typer
from huey.consumer_options import ConsumerConfig
from prometheus_client import start_http_server

from archiver.tasks import huey


def main():
    start_http_server(8000)

    config = ConsumerConfig(
        workers=os.getenv("HUEY_WORKERS", 1),
    )
    config.validate()

    # Set up logging for the "huey" namespace.
    logger = logging.getLogger('huey')
    config.setup_logger(logger)

    consumer = huey.create_consumer(**config.values)
    consumer.run()


if __name__ == '__main__':
    typer.run(main)
