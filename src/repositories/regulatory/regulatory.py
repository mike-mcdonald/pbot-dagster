from dagster import Definitions

from repositories.regulatory.analyze_driver_records.job import analyze_driver_records

DEFINITIONS = Definitions(
    jobs={analyze_driver_records},
)
