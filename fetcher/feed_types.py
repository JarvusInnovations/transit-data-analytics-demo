from enum import Enum

# TODO: in the future, we may have multiple file formats
#  for the same feed_type (e.g. we could get a JSON
#  response instead of proto)
from typing import Dict, List

from pydantic import BaseModel


class FeedType(str, Enum):
    # gtfs/other standards
    gtfs_schedule = "gtfs_schedule"
    gtfs_vehicle_positions = "gtfs_vehicle_positions"
    gtfs_trip_updates = "gtfs_trip_updates"
    gtfs_service_alerts = "gtfs_service_alerts"
    # agency/vendor-specific
    septa__arrivals = "septa__arrivals"
    septa__train_view = "septa__train_view"
    septa__transit_view_all = "septa__transit_view_all"


class SeptaTransitViewAll(BaseModel):
    routes: List[Dict[str, List[Dict]]]
