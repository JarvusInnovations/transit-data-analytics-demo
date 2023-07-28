from typing import Dict, Type, Any

import pendulum
from polyfactory.factories.pydantic_factory import ModelFactory

from fetcher.common import RawFetchedFile, FeedConfig, FeedType


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
        ts=pendulum.now(),
        config=FeedConfig(
            name="whatever",
            feed_type=FeedType.gtfs_rt__vehicle_positions,
            url="https://whatever.com",
        ),
        response_code=200,
        response_headers={},
        contents=b"test test 123",
    ).json()
