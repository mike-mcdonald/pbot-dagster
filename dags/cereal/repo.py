from dagster import repository
from cereal import cereal_pipeline, cereal_schedule


@repository
def dags_repo():
    return [cereal_pipeline, cereal_schedule]
