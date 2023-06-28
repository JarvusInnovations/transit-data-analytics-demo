from typing import Dict, Type, Any

import pendulum
from polyfactory.factories.pydantic_factory import ModelFactory

from archiver.tasks import RawFetchedFile, FetchConfig, FeedType


# TODO: get this working
class RawFetchedFileFactory(ModelFactory[RawFetchedFile]):
    __model__ = RawFetchedFile

    @classmethod
    def get_provider_map(cls) -> Dict[Type, Any]:
        providers_map = super().get_provider_map()

        return {
            pendulum.DateTime: lambda: pendulum.now(),
            **providers_map,
        }


def test_fetched_raw_file_serializes():
    RawFetchedFile(
        table="whatever",
        ts=pendulum.now(),
        config=FetchConfig(
            name="whatever",
            feed_type=FeedType.realtime,
            is_gtfs=False,
            url="https://whatever.com",
            table="something",
        ),
        response_code=200,
        response_headers={},
        contents=b"test test 123",
    ).json()
