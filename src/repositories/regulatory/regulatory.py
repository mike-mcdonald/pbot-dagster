from dagster import Definitions

from repositories.regulatory.analyze_driver_records.job import analyze_driver_records
from repositories.regulatory.analyze_driver_records.schedule import (
    analyze_driver_records as analyze_driver_records_schedule,
)

DEFINITIONS = Definitions(
    jobs={analyze_driver_records}, schedules={analyze_driver_records_schedule}
)
