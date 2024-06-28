from dagster import Definitions

from repositories.enforcement.schedules import parking_citations_to_featureclass
from repositories.enforcement.unregistered_vehicles import process_politess_exports

DEFINITIONS = Definitions(
    jobs=[process_politess_exports], schedules=[parking_citations_to_featureclass]
)
